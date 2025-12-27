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

var connectionString = builder.Configuration.GetValue<string>("PAYMENTS_CONNECTION_STRING")
    ?? "Host=payments-db;Port=5432;Database=paymentsdb;Username=postgres;Password=postgres;";
var rabbitHost = builder.Configuration.GetValue<string>("RABBITMQ_HOST") ?? "rabbitmq";
var requestQueue = builder.Configuration.GetValue<string>("PAYMENT_REQUEST_QUEUE") ?? "order.payment.requested";
var statusQueue = builder.Configuration.GetValue<string>("PAYMENT_STATUS_QUEUE") ?? "payment.status.changed";

builder.Services.AddSingleton<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(connectionString));
builder.Services.AddSingleton(sp => new RabbitConnectionProvider(rabbitHost, requestQueue, statusQueue));
builder.Services.AddSingleton<DbInitializer>();
builder.Services.AddHostedService<MigrationHostedService>();
builder.Services.AddSingleton<PaymentRequestConsumer>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PaymentRequestConsumer>());
builder.Services.AddSingleton<PaymentOutboxDispatcher>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PaymentOutboxDispatcher>());

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/accounts", async (HttpContext context, IDbConnectionFactory db) =>
{
    var userId = context.Request.Headers["X-User-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(userId))
    {
        return Results.BadRequest(new { error = "X-User-Id header is required" });
    }

    var initialBalance = 0m;
    using var reader = new StreamReader(context.Request.Body);
    var body = await reader.ReadToEndAsync();
    if (!string.IsNullOrWhiteSpace(body))
    {
        try
        {
            var payload = JsonSerializer.Deserialize<CreateAccountRequest>(body);
            initialBalance = payload?.InitialBalance ?? 0m;
        }
        catch (JsonException)
        {
            return Results.BadRequest(new { error = "Invalid request body" });
        }
    }

    if (initialBalance < 0)
    {
        return Results.BadRequest(new { error = "Initial balance cannot be negative" });
    }

    await using var conn = db.CreateConnection();
    await conn.OpenAsync();
    await using var tx = await conn.BeginTransactionAsync();

    var inserted = await conn.ExecuteAsync(
        "INSERT INTO accounts(user_id, balance) VALUES (@userId, @balance) ON CONFLICT(user_id) DO NOTHING",
        new { userId, balance = initialBalance }, tx);

    if (inserted == 0)
    {
        await tx.RollbackAsync();
        return Results.Conflict(new { error = "Account already exists" });
    }

    await tx.CommitAsync();
    return Results.Created($"/accounts/{userId}", new { userId, balance = initialBalance });
});

app.MapPost("/accounts/deposit", async (HttpContext context, IDbConnectionFactory db, DepositRequest request) =>
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

    var updated = await conn.ExecuteAsync(
        "UPDATE accounts SET balance = balance + @amount WHERE user_id = @userId",
        new { amount = request.Amount, userId }, tx);

    if (updated == 0)
    {
        await tx.RollbackAsync();
        return Results.NotFound(new { error = "Account not found" });
    }

    await tx.CommitAsync();
    var balance = await conn.ExecuteScalarAsync<decimal>("SELECT balance FROM accounts WHERE user_id = @userId", new { userId });
    return Results.Ok(new { userId, balance });
});

app.MapGet("/accounts/balance", async (HttpContext context, IDbConnectionFactory db) =>
{
    var userId = context.Request.Headers["X-User-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(userId))
    {
        return Results.BadRequest(new { error = "X-User-Id header is required" });
    }

    await using var conn = db.CreateConnection();
    await conn.OpenAsync();
    var balance = await conn.ExecuteScalarAsync<decimal?>("SELECT balance FROM accounts WHERE user_id = @userId", new { userId });

    if (balance is null)
    {
        return Results.NotFound(new { error = "Account not found" });
    }

    return Results.Ok(new { userId, balance });
});

app.MapGet("/health", () => Results.Ok(new { status = "Payments Service is running" }));

app.Run();

public record CreateAccountRequest(decimal? InitialBalance);
public record DepositRequest(decimal Amount);
public record PaymentRequest(Guid MessageId, Guid OrderId, string UserId, decimal Amount, string Description);
public record PaymentStatus(Guid MessageId, Guid CorrelationId, Guid OrderId, string UserId, string Status, string Reason);

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
        CREATE TABLE IF NOT EXISTS accounts(
            user_id TEXT PRIMARY KEY,
            balance NUMERIC NOT NULL
        );
        CREATE TABLE IF NOT EXISTS payment_inbox(
            message_id UUID PRIMARY KEY,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS payment_outbox(
            id UUID PRIMARY KEY,
            type TEXT NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
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

public sealed class RabbitConnectionProvider : IDisposable
{
    private readonly ConnectionFactory _factory;
    public string RequestQueue { get; }
    public string StatusQueue { get; }

    public RabbitConnectionProvider(string host, string requestQueue, string statusQueue)
    {
        _factory = new ConnectionFactory { HostName = host, DispatchConsumersAsync = true };
        RequestQueue = requestQueue;
        StatusQueue = statusQueue;
    }

    public IConnection CreateConnection() => _factory.CreateConnection();

    public void Dispose()
    {
        // Factory does not implement IDisposable, nothing to dispose.
    }
}

public sealed class PaymentRequestConsumer : BackgroundService
{
    private readonly IDbConnectionFactory _db;
    private readonly RabbitConnectionProvider _provider;

    public PaymentRequestConsumer(IDbConnectionFactory db, RabbitConnectionProvider provider)
    {
        _db = db;
        _provider = provider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var connection = _provider.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: _provider.RequestQueue, durable: true, exclusive: false, autoDelete: false);
        channel.BasicQos(0, 1, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                var request = JsonSerializer.Deserialize<PaymentRequest>(json);
                if (request is null)
                {
                    channel.BasicAck(ea.DeliveryTag, false);
                    return;
                }

                await HandleRequestAsync(request);
                channel.BasicAck(ea.DeliveryTag, false);
            }
            catch
            {
                channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        channel.BasicConsume(queue: _provider.RequestQueue, autoAck: false, consumer: consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private async Task HandleRequestAsync(PaymentRequest request)
    {
        await using var conn = _db.CreateConnection();
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();

        var inboxInserted = await conn.ExecuteAsync(
            "INSERT INTO payment_inbox(message_id, processed_at) VALUES (@messageId, NULL) ON CONFLICT(message_id) DO NOTHING",
            new { messageId = request.MessageId }, tx);

        if (inboxInserted == 0)
        {
            var alreadyProcessed = await conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(1) FROM payment_inbox WHERE message_id = @messageId AND processed_at IS NOT NULL",
                new { messageId = request.MessageId }, tx);
            if (alreadyProcessed > 0)
            {
                await tx.CommitAsync();
                return;
            }
        }

        var accountExists = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM accounts WHERE user_id = @userId", new { request.UserId }, tx);
        if (accountExists == 0)
        {
            await EnqueueStatusAsync(conn, tx, request, "FAILED", "Account not found");
            await conn.ExecuteAsync("UPDATE payment_inbox SET processed_at = NOW() WHERE message_id = @messageId", new { messageId = request.MessageId }, tx);
            await tx.CommitAsync();
            return;
        }

        var updated = await conn.ExecuteAsync(
            "UPDATE accounts SET balance = balance - @amount WHERE user_id = @userId AND balance >= @amount",
            new { amount = request.Amount, userId = request.UserId }, tx);

        if (updated == 0)
        {
            await EnqueueStatusAsync(conn, tx, request, "FAILED", "Insufficient funds");
        }
        else
        {
            await EnqueueStatusAsync(conn, tx, request, "SUCCEEDED", "Payment captured");
        }

        await conn.ExecuteAsync("UPDATE payment_inbox SET processed_at = NOW() WHERE message_id = @messageId", new { messageId = request.MessageId }, tx);
        await tx.CommitAsync();
    }

    private async Task EnqueueStatusAsync(NpgsqlConnection conn, NpgsqlTransaction tx, PaymentRequest request, string status, string reason)
    {
        var payload = new PaymentStatus(
            MessageId: Guid.NewGuid(),
            CorrelationId: request.MessageId,
            OrderId: request.OrderId,
            UserId: request.UserId,
            Status: status,
            Reason: reason);

        await conn.ExecuteAsync(
            "INSERT INTO payment_outbox(id, type, payload, occurred_at, processed_at) VALUES (@id, @type, @payload::jsonb, @occurred_at, NULL)",
            new
            {
                id = Guid.NewGuid(),
                type = "PaymentStatus",
                payload = JsonSerializer.Serialize(payload),
                occurred_at = DateTime.UtcNow
            }, tx);
    }
}

public sealed class PaymentOutboxDispatcher : BackgroundService
{
    private readonly IDbConnectionFactory _db;
    private readonly RabbitConnectionProvider _provider;

    public PaymentOutboxDispatcher(IDbConnectionFactory db, RabbitConnectionProvider provider)
    {
        _db = db;
        _provider = provider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var connection = _provider.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue: _provider.StatusQueue, durable: true, exclusive: false, autoDelete: false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var conn = _db.CreateConnection();
                await conn.OpenAsync(stoppingToken);

                await using var tx = await conn.BeginTransactionAsync(stoppingToken);

                var pending = (await conn.QueryAsync<OutboxRow>(
                    "SELECT id, type, payload FROM payment_outbox WHERE processed_at IS NULL ORDER BY occurred_at FOR UPDATE SKIP LOCKED LIMIT 20",
                    transaction: tx)).ToList();

                foreach (var row in pending)
                {
                    var body = Encoding.UTF8.GetBytes(row.Payload);
                    var props = channel.CreateBasicProperties();
                    props.Persistent = true;
                    channel.BasicPublish(exchange: string.Empty, routingKey: _provider.StatusQueue, basicProperties: props, body: body);

                    await conn.ExecuteAsync("UPDATE payment_outbox SET processed_at = NOW() WHERE id = @id", new { row.Id }, tx);
                }

                await tx.CommitAsync(stoppingToken);
            }
            catch
            {
                // swallow and retry
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private record OutboxRow(Guid Id, string Type, string Payload);
}
