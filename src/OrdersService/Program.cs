using System.Linq;
using System.Text;
using System.Text.Json;
using Dapper;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var connectionString = builder.Configuration.GetValue<string>("ORDERS_CONNECTION_STRING")
    ?? "Host=orders-db;Port=5432;Database=ordersdb;Username=postgres;Password=postgres;";
var rabbitHost = builder.Configuration.GetValue<string>("RABBITMQ_HOST") ?? "rabbitmq";
var paymentRequestQueue = builder.Configuration.GetValue<string>("PAYMENT_REQUEST_QUEUE") ?? "order.payment.requested";
var paymentStatusQueue = builder.Configuration.GetValue<string>("PAYMENT_STATUS_QUEUE") ?? "payment.status.changed";

builder.Services.AddSingleton<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(connectionString));
builder.Services.AddSingleton(sp => new RabbitConnectionProvider(rabbitHost, paymentRequestQueue, paymentStatusQueue));
builder.Services.AddSingleton<DbInitializer>();
builder.Services.AddHostedService<MigrationHostedService>();
builder.Services.AddSingleton<OrderOutboxDispatcher>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<OrderOutboxDispatcher>());
builder.Services.AddSingleton<PaymentStatusConsumer>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PaymentStatusConsumer>());

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/orders", async (CreateOrderRequest request, HttpContext context, IDbConnectionFactory db) =>
{
    var userId = context.Request.Headers["X-User-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(userId))
    {
        return Results.BadRequest(new { error = "X-User-Id header is required" });
    }

    if (request.Amount <= 0)
    {
        return Results.BadRequest(new { error = "Amount must be positive" });
    }

    await using var conn = db.CreateConnection();
    await conn.OpenAsync();
    await using var tx = await conn.BeginTransactionAsync();

    var orderId = Guid.NewGuid();
    await conn.ExecuteAsync(
        "INSERT INTO orders(id, user_id, amount, description, status) VALUES (@id, @user_id, @amount, @description, @status)",
        new { id = orderId, user_id = userId, amount = request.Amount, description = request.Description, status = OrderStatus.New }, tx);

    var paymentRequest = new PaymentRequest(
        MessageId: Guid.NewGuid(),
        OrderId: orderId,
        UserId: userId,
        Amount: request.Amount,
        Description: request.Description ?? string.Empty);

    await conn.ExecuteAsync(
        "INSERT INTO order_outbox(id, type, payload, occurred_at, processed_at) VALUES (@id, @type, @payload::jsonb, @occurred_at, NULL)",
        new
        {
            id = Guid.NewGuid(),
            type = "PaymentRequested",
            payload = JsonSerializer.Serialize(paymentRequest),
            occurred_at = DateTime.UtcNow
        }, tx);

    await tx.CommitAsync();

    return Results.Created($"/orders/{orderId}", new { id = orderId, status = OrderStatus.New });
});

app.MapGet("/orders", async (HttpContext context, IDbConnectionFactory db) =>
{
    var userId = context.Request.Headers["X-User-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(userId))
    {
        return Results.BadRequest(new { error = "X-User-Id header is required" });
    }

    await using var conn = db.CreateConnection();
    await conn.OpenAsync();
    var orders = await conn.QueryAsync<Order>("SELECT id, user_id AS UserId, amount, description, status FROM orders WHERE user_id = @userId ORDER BY created_at DESC", new { userId });
    return Results.Ok(orders);
});

app.MapGet("/orders/{id:guid}", async (Guid id, HttpContext context, IDbConnectionFactory db) =>
{
    var userId = context.Request.Headers["X-User-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(userId))
    {
        return Results.BadRequest(new { error = "X-User-Id header is required" });
    }

    await using var conn = db.CreateConnection();
    await conn.OpenAsync();
    var order = await conn.QuerySingleOrDefaultAsync<Order>("SELECT id, user_id AS UserId, amount, description, status FROM orders WHERE id = @id AND user_id = @userId", new { id, userId });
    if (order is null)
    {
        return Results.NotFound();
    }

    return Results.Ok(order);
});

app.MapGet("/health", () => Results.Ok(new { status = "Orders Service is running" }));

app.Run();

public record CreateOrderRequest(decimal Amount, string? Description);
public record PaymentRequest(Guid MessageId, Guid OrderId, string UserId, decimal Amount, string Description);
public record PaymentStatus(Guid MessageId, Guid CorrelationId, Guid OrderId, string UserId, string Status, string Reason);
public record Order(Guid Id, string UserId, decimal Amount, string? Description, string Status);

public static class OrderStatus
{
    public const string New = "NEW";
    public const string Finished = "FINISHED";
    public const string Cancelled = "CANCELLED";
}

public interface IDbConnectionFactory
{
    NpgsqlConnection CreateConnection();
}

public sealed class NpgsqlConnectionFactory : IDbConnectionFactory
{
    private readonly string _connectionString;

    public NpgsqlConnectionFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public NpgsqlConnection CreateConnection() => new NpgsqlConnection(_connectionString);
}

public sealed class DbInitializer
{
    private readonly IDbConnectionFactory _factory;

    public DbInitializer(IDbConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task InitializeAsync()
    {
        await using var conn = _factory.CreateConnection();
        await conn.OpenAsync();

        const string sql = @"
        CREATE TABLE IF NOT EXISTS orders(
            id UUID PRIMARY KEY,
            user_id TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            description TEXT,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS order_outbox(
            id UUID PRIMARY KEY,
            type TEXT NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS payment_event_inbox(
            message_id UUID PRIMARY KEY,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        );";

        await conn.ExecuteAsync(sql);
    }
}

public sealed class MigrationHostedService : IHostedService
{
    private readonly DbInitializer _initializer;

    public MigrationHostedService(DbInitializer initializer)
    {
        _initializer = initializer;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _initializer.InitializeAsync();
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class RabbitConnectionProvider
{
    private readonly ConnectionFactory _factory;
    public string PaymentRequestQueue { get; }
    public string PaymentStatusQueue { get; }

    public RabbitConnectionProvider(string host, string requestQueue, string statusQueue)
    {
        _factory = new ConnectionFactory { HostName = host, DispatchConsumersAsync = true };
        PaymentRequestQueue = requestQueue;
        PaymentStatusQueue = statusQueue;
    }

    public IConnection CreateConnection() => _factory.CreateConnection();
}

public sealed class OrderOutboxDispatcher : BackgroundService
{
    private readonly IDbConnectionFactory _db;
    private readonly RabbitConnectionProvider _provider;

    public OrderOutboxDispatcher(IDbConnectionFactory db, RabbitConnectionProvider provider)
    {
        _db = db;
        _provider = provider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var connection = _provider.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue: _provider.PaymentRequestQueue, durable: true, exclusive: false, autoDelete: false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var conn = _db.CreateConnection();
                await conn.OpenAsync(stoppingToken);
                await using var tx = await conn.BeginTransactionAsync(stoppingToken);

                var pending = (await conn.QueryAsync<OutboxRow>(
                    "SELECT id, type, payload FROM order_outbox WHERE processed_at IS NULL ORDER BY occurred_at FOR UPDATE SKIP LOCKED LIMIT 20",
                    transaction: tx)).ToList();

                foreach (var row in pending)
                {
                    var props = channel.CreateBasicProperties();
                    props.Persistent = true;
                    var body = Encoding.UTF8.GetBytes(row.Payload);
                    channel.BasicPublish(exchange: string.Empty, routingKey: _provider.PaymentRequestQueue, basicProperties: props, body: body);

                    await conn.ExecuteAsync("UPDATE order_outbox SET processed_at = NOW() WHERE id = @id", new { row.Id }, tx);
                }

                await tx.CommitAsync(stoppingToken);
            }
            catch
            {
                // retry later
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private record OutboxRow(Guid Id, string Type, string Payload);
}

public sealed class PaymentStatusConsumer : BackgroundService
{
    private readonly IDbConnectionFactory _db;
    private readonly RabbitConnectionProvider _provider;

    public PaymentStatusConsumer(IDbConnectionFactory db, RabbitConnectionProvider provider)
    {
        _db = db;
        _provider = provider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var connection = _provider.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue: _provider.PaymentStatusQueue, durable: true, exclusive: false, autoDelete: false);
        channel.BasicQos(0, 1, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                var message = JsonSerializer.Deserialize<PaymentStatus>(json);
                if (message is null)
                {
                    channel.BasicAck(ea.DeliveryTag, false);
                    return;
                }

                await HandleStatusAsync(message);
                channel.BasicAck(ea.DeliveryTag, false);
            }
            catch
            {
                channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        channel.BasicConsume(queue: _provider.PaymentStatusQueue, autoAck: false, consumer: consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private async Task HandleStatusAsync(PaymentStatus status)
    {
        await using var conn = _db.CreateConnection();
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();

        var inboxInserted = await conn.ExecuteAsync(
            "INSERT INTO payment_event_inbox(message_id, processed_at) VALUES (@messageId, NULL) ON CONFLICT(message_id) DO NOTHING",
            new { messageId = status.MessageId }, tx);

        if (inboxInserted == 0)
        {
            var alreadyProcessed = await conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(1) FROM payment_event_inbox WHERE message_id = @messageId AND processed_at IS NOT NULL",
                new { messageId = status.MessageId }, tx);
            if (alreadyProcessed > 0)
            {
                await tx.CommitAsync();
                return;
            }
        }

        var newStatus = status.Status == "SUCCEEDED" ? OrderStatus.Finished : OrderStatus.Cancelled;
        await conn.ExecuteAsync(
            "UPDATE orders SET status = @status WHERE id = @orderId",
            new { status = newStatus, orderId = status.OrderId }, tx);

        await conn.ExecuteAsync("UPDATE payment_event_inbox SET processed_at = NOW() WHERE message_id = @messageId", new { messageId = status.MessageId }, tx);
        await tx.CommitAsync();
    }
}
