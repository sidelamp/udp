using System.Net;
using UdpServer;

// Config
const int ListenPort = 5000;
const int BufferCapacity = 1000;
const int NackIntervalMs = 100;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var service = new ReceiverService(ListenPort, BufferCapacity, NackIntervalMs);

try
{
    await service.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // Expected on shutdown
}
