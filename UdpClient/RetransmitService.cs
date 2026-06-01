using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using NetUdpClient = System.Net.Sockets.UdpClient;

namespace UdpClient;

public class RetransmitService
{
    private readonly NetUdpClient _udpClient;
    private readonly IPEndPoint _serverEndPoint;
    private readonly ConcurrentDictionary<uint, byte[]> _pendingPackets;

    public static int RetransmitCount { get; private set; }

    public RetransmitService(NetUdpClient udpClient, IPEndPoint serverEndPoint, ConcurrentDictionary<uint, byte[]> pendingPackets)
    {
        _udpClient = udpClient;
        _serverEndPoint = serverEndPoint;
        _pendingPackets = pendingPackets;
    }

    public Task RunAsync(CancellationToken ct) => Task.Run(() =>
    {
        int expiredCount = 0;
        int lastLogCount = 0;

        _udpClient.Client.ReceiveTimeout = 1000;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var remoteEp = new IPEndPoint(IPAddress.Any, 0);
                byte[] data = _udpClient.Receive(ref remoteEp);

                if (PacketHelper.TryParseNack(data, out List<uint> missingSeqs))
                {
                    foreach (uint seq in missingSeqs)
                    {
                        if (_pendingPackets.TryGetValue(seq, out byte[]? packet))
                        {
                            _udpClient.Send(packet, packet.Length, _serverEndPoint);
                            RetransmitCount++;
                        }
                        else
                        {
                            expiredCount++;
                        }
                    }
                    if (RetransmitCount / 100 > lastLogCount / 100 && RetransmitCount > 0)
                    {
                        lastLogCount = RetransmitCount;
                        Console.WriteLine($"[RETX] Retransmitted: {RetransmitCount}, expired: {expiredCount}");
                    }
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                // Timeout — loop back to check cancellation
            }
            catch (SocketException ex) when (ct.IsCancellationRequested || ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }
    });
}
