using System.Net.Http.Headers;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var ordersBase = builder.Configuration.GetValue<string>("ORDERS_BASE_URL") ?? "http://orders-service:8081";
var paymentsBase = builder.Configuration.GetValue<string>("PAYMENTS_BASE_URL") ?? "http://payments-service:8082";

builder.Services.AddHttpClient("orders", client => client.BaseAddress = new Uri(ordersBase));
builder.Services.AddHttpClient("payments", client => client.BaseAddress = new Uri(paymentsBase));

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => Results.Ok(new { status = "API Gateway ready" }));

app.Map("/{service}/{**path}", async (string service, string? path, HttpContext context, IHttpClientFactory factory) =>
{
    var clientName = service.ToLowerInvariant() switch
    {
        "orders" => "orders",
        "payments" => "payments",
        _ => null
    };

    if (clientName is null)
    {
        return Results.NotFound(new { error = "Unknown downstream service" });
    }

    var client = factory.CreateClient(clientName);
    var target = $"/{path}".TrimEnd('/');
    if (string.IsNullOrEmpty(path))
    {
        target = string.Empty;
    }
    var uriBuilder = new UriBuilder(new Uri(client.BaseAddress!, target))
    {
        Query = context.Request.QueryString.HasValue ? context.Request.QueryString.Value![1..] : string.Empty
    };

    var forwardRequest = new HttpRequestMessage(new HttpMethod(context.Request.Method), uriBuilder.Uri);

    foreach (var header in context.Request.Headers)
    {
        if (header.Key.Equals("Host", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!forwardRequest.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()) && forwardRequest.Content is not null)
        {
            forwardRequest.Content?.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
        }
    }

    if (context.Request.ContentLength > 0)
    {
        var content = new StreamContent(context.Request.Body);
        if (!string.IsNullOrWhiteSpace(context.Request.ContentType))
        {
            content.Headers.ContentType = MediaTypeHeaderValue.Parse(context.Request.ContentType);
        }
        forwardRequest.Content = content;
    }

    var response = await client.SendAsync(forwardRequest, HttpCompletionOption.ResponseHeadersRead, context.RequestAborted);

    context.Response.StatusCode = (int)response.StatusCode;
    foreach (var header in response.Headers)
    {
        context.Response.Headers[header.Key] = header.Value.ToArray();
    }

    if (response.Content is not null)
    {
        foreach (var header in response.Content.Headers)
        {
            context.Response.Headers[header.Key] = header.Value.ToArray();
        }

        await response.Content.CopyToAsync(context.Response.Body);
    }

    return Results.Empty;
});

app.Run();
