using System.Net;
using System.Net.Sockets;
using NetUdpClient = System.Net.Sockets.UdpClient;
using UdpClient;

// Config
const string ServerHost = "127.0.0.1";
const int ServerPort = 5000;
const int PayloadSize = 1024;
const double SkipProbability = 0.02; // 2%

var serverEndPoint = new IPEndPoint(IPAddress.Parse(ServerHost), ServerPort);
using var udpClient = new NetUdpClient(5001);
Console.WriteLine($"[CLIENT] Local endpoint: {udpClient.Client.LocalEndPoint}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var sendService = new SendService(udpClient, serverEndPoint, PayloadSize, SkipProbability);
var retransmitService = new RetransmitService(udpClient, serverEndPoint, sendService.PendingPackets);

try
{
    await Task.WhenAll(
        sendService.RunAsync(cts.Token),
        retransmitService.RunAsync(cts.Token));
}
catch (OperationCanceledException)
{
    // Expected on shutdown
}

Console.WriteLine("\n[CLIENT] Shutdown complete.");
