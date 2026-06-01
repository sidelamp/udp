using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("UDP Server starting on port 5000...");

using var udpServer = new UdpClient(5000);

while (true)
{
    var remoteEndPoint = new IPEndPoint(IPAddress.Any, 0);
    byte[] receivedBytes = udpServer.Receive(ref remoteEndPoint);
    string receivedText = Encoding.UTF8.GetString(receivedBytes);

    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] From {remoteEndPoint}: {receivedText}");

    string response = $"re: {receivedText}";
    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
    udpServer.Send(responseBytes, responseBytes.Length, remoteEndPoint);
}
