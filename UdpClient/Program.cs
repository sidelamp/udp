using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("UDP Client starting...");

using var udpClient = new UdpClient();
var serverEndPoint = new IPEndPoint(IPAddress.Loopback, 5000);

string message = "TEST UDP";
byte[] messageBytes = Encoding.UTF8.GetBytes(message);

Console.WriteLine($"Sending: {message}");
udpClient.Send(messageBytes, messageBytes.Length, serverEndPoint);

udpClient.Client.ReceiveTimeout = 5000;
try
{
    var remoteEndPoint = new IPEndPoint(IPAddress.Any, 0);
    byte[] receivedBytes = udpClient.Receive(ref remoteEndPoint);
    var receivedText = Encoding.UTF8.GetString(receivedBytes);

    Console.WriteLine($"Received from server: {receivedText}");
}
catch (SocketException)
{
    Console.WriteLine("No response from server (timeout).");
}

Console.WriteLine("Press any key to exit...");
Console.ReadKey();
