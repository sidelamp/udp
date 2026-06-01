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
        var lastStatsTime = DateTime.UtcNow;
        long bytesSinceLastStats = 0;
        int skipLogThrottle = 0;

        // Rate limiter
        var sw = Stopwatch.StartNew();
        long totalPacketsAttempted = 0;

        while (!ct.IsCancellationRequested)
        {
            // Random skip
            if (Random.Shared.NextDouble() < _skipProbability)
            {
                SkippedCount++;
                totalPacketsAttempted++;
                if (++skipLogThrottle % 20 == 0)
                    Console.WriteLine($"[SKIP] Seq {seq} skipped ({SkippedCount} total)");
                seq++;
                continue;
            }

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
                break;
            }
            SentCount++;
            bytesSinceLastStats += packet.Length;
            totalPacketsAttempted++;

            // Clean old pending packets (older than 5000 seqs)
            if (seq > 5000 && seq % 1000 == 0)
            {
                uint cutoff = seq - 5000;
                foreach (var key in PendingPackets.Keys)
                {
                    if (key < cutoff)
                        PendingPackets.TryRemove(key, out _);
                }
            }

            seq++;

            // Stats every 1 second
            var now = DateTime.UtcNow;
            if ((now - lastStatsTime).TotalMilliseconds >= 1000)
            {
                double elapsed = (now - lastStatsTime).TotalSeconds;
                double rateMb = bytesSinceLastStats / elapsed / 1_000_000;

                Console.WriteLine(
                    $"[STAT] Sent: {SentCount} | " +
                    $"Skipped: {SkippedCount} | " +
                    $"Retransmitted: {RetransmitService.RetransmitCount} | " +
                    $"Rate: {rateMb:F2} MB/s");

                lastStatsTime = now;
                bytesSinceLastStats = 0;
            }

            // Rate limit: if we're ahead of schedule, sleep
            long expectedMs = totalPacketsAttempted * 1000 / TargetPacketsPerSec;
            long actualMs = sw.ElapsedMilliseconds;
            if (expectedMs > actualMs)
            {
                Thread.Sleep((int)(expectedMs - actualMs));
            }
        }
    });
}
