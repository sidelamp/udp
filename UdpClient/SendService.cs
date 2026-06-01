using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using NetUdpClient = System.Net.Sockets.UdpClient;

namespace UdpClient;

public class SendService
{
    private const int TargetPacketsPerSec = 1000;

    private readonly NetUdpClient _udpClient;
    private readonly IPEndPoint _serverEndPoint;
    private readonly int _payloadSize;
    private readonly double _skipProbability;

    public ConcurrentDictionary<uint, byte[]> PendingPackets { get; } = new();
    public int SentCount { get; private set; }
    public int SkippedCount { get; private set; }

    public SendService(NetUdpClient udpClient, IPEndPoint serverEndPoint, int payloadSize, double skipProbability)
    {
        _udpClient = udpClient;
        _serverEndPoint = serverEndPoint;
        _payloadSize = payloadSize;
        _skipProbability = skipProbability;
    }

    public Task RunAsync(CancellationToken ct) => Task.Run(() =>
    {
        Console.WriteLine($"[CLIENT] Sending to {_serverEndPoint}, payload={_payloadSize}B, target={TargetPacketsPerSec} pkt/s, skip={_skipProbability:P0}");
        Console.WriteLine("[CLIENT] Press Ctrl+C to stop\n");

        uint seq = 0;
        byte[] payload = new byte[_payloadSize];
        var stats = new StatsState();
        var rateLimiter = new RateLimiter(TargetPacketsPerSec);

        while (!ct.IsCancellationRequested)
        {
            if (TrySkip(seq, ref stats.SkipLogThrottle))
            {
                seq++;
                rateLimiter.OnPacketAttempted();
                continue;
            }

            if (!TrySendPacket(seq, payload, ref stats.BytesSinceLastStats))
                break;

            CleanupOldPending(seq);
            seq++;
            rateLimiter.OnPacketAttempted();

            stats.TryPrint(SentCount, SkippedCount);
            rateLimiter.Apply();
        }
    });

    private bool TrySkip(uint seq, ref int skipLogThrottle)
    {
        if (Random.Shared.NextDouble() >= _skipProbability)
            return false;

        SkippedCount++;
        if (++skipLogThrottle % 20 == 0)
            Console.WriteLine($"[SKIP] Seq {seq} skipped ({SkippedCount} total)");
        return true;
    }

    private bool TrySendPacket(uint seq, byte[] payload, ref long bytesSinceLastStats)
    {
        Random.Shared.NextBytes(payload);
        byte[] packet = PacketHelper.BuildDataPacket(seq, payload);
        PendingPackets[seq] = packet;

        try
        {
            _udpClient.Send(packet, packet.Length, _serverEndPoint);
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
        {
            Console.WriteLine("[CLIENT] Server disconnected");
            return false;
        }

        SentCount++;
        bytesSinceLastStats += packet.Length;
        return true;
    }

    private void CleanupOldPending(uint seq)
    {
        if (seq <= 5000 || seq % 1000 != 0)
            return;

        uint cutoff = seq - 5000;
        foreach (var key in PendingPackets.Keys)
        {
            if (key < cutoff)
                PendingPackets.TryRemove(key, out _);
        }
    }

    private struct StatsState
    {
        public long BytesSinceLastStats;
        public int SkipLogThrottle;
        private DateTime _lastStatsTime = DateTime.UtcNow;

        public StatsState() { }

        public void TryPrint(int sentCount, int skippedCount)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastStatsTime).TotalMilliseconds < 1000)
                return;

            double elapsed = (now - _lastStatsTime).TotalSeconds;
            double rateMb = BytesSinceLastStats / elapsed / 1_000_000;

            Console.WriteLine(
                $"[STAT] Sent: {sentCount} | " +
                $"Skipped: {skippedCount} | " +
                $"Retransmitted: {RetransmitService.RetransmitCount} | " +
                $"Rate: {rateMb:F2} MB/s");

            _lastStatsTime = now;
            BytesSinceLastStats = 0;
        }
    }

    private struct RateLimiter
    {
        private readonly int _targetPacketsPerSec;
        private readonly Stopwatch _sw;
        private long _totalPacketsAttempted;

        public RateLimiter(int targetPacketsPerSec)
        {
            _targetPacketsPerSec = targetPacketsPerSec;
            _sw = Stopwatch.StartNew();
            _totalPacketsAttempted = 0;
        }

        public void OnPacketAttempted() => _totalPacketsAttempted++;

        public void Apply()
        {
            long expectedMs = _totalPacketsAttempted * 1000 / _targetPacketsPerSec;
            long actualMs = _sw.ElapsedMilliseconds;
            if (expectedMs > actualMs)
                Thread.Sleep((int)(expectedMs - actualMs));
        }
    }
}
