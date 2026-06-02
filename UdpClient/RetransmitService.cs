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
        _udpClient.Client.ReceiveTimeout = 1000;

        var stats = new RetransmitStats();

        while (!ct.IsCancellationRequested)
        {
            if (!TryReceive(out byte[]? data))
                continue;

            if (PacketHelper.TryParseNack(data!, out List<uint> missingSeqs))
                ProcessNack(missingSeqs, ref stats);
        }
    }, ct);

    private bool TryReceive(out byte[]? data)
    {
        data = null;
        try
        {
            var remoteEp = new IPEndPoint(IPAddress.Any, 0);
            data = _udpClient.Receive(ref remoteEp);
            return true;
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
        {
            return false;
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
        {
            return false;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    private void ProcessNack(List<uint> missingSeqs, ref RetransmitStats stats)
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
                stats.ExpiredCount++;
            }
        }

        stats.TryLog(RetransmitCount);
    }

    private struct RetransmitStats
    {
        public int ExpiredCount;
        private int _lastLogCount;

        public void TryLog(int retransmitCount)
        {
            if (retransmitCount / 100 <= _lastLogCount / 100 || retransmitCount == 0)
                return;

            _lastLogCount = retransmitCount;
            Console.WriteLine($"[RETX] Retransmitted: {retransmitCount}, expired: {ExpiredCount}");
        }
    }
}
