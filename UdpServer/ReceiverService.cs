using System.Net;
using System.Net.Sockets;

namespace UdpServer;

public class ReceiverService
{
    private readonly UdpClient _udpServer;
    private readonly RingBuffer _buffer;
    private readonly int _nackIntervalMs;
    private IPEndPoint? _clientEndPoint;
    private readonly Dictionary<uint, DateTime> _nackCooldown = new();
    private static readonly TimeSpan CooldownTime = TimeSpan.FromSeconds(2);

    public int NackSentCount { get; private set; }

    public ReceiverService(int port, int bufferCapacity, int nackIntervalMs)
    {
        _udpServer = new UdpClient(port);
        _udpServer.Client.ReceiveTimeout = nackIntervalMs;
        _buffer = new RingBuffer(bufferCapacity);
        _nackIntervalMs = nackIntervalMs;
    }

    public Task RunAsync(CancellationToken ct) => Task.Run(() =>
    {
        Console.WriteLine($"[SERVER] Listening on port {_udpServer.Client.LocalEndPoint}...");
        Console.WriteLine("[SERVER] Press Ctrl+C to stop\n");

        var lastStatsTime = DateTime.UtcNow;
        var lastNackTime = DateTime.UtcNow;
        long bytesSinceLastStats = 0;

        while (!ct.IsCancellationRequested)
        {
            // Try to receive a packet (blocks up to timeout)
            try
            {
                var remoteEp = new IPEndPoint(IPAddress.Any, 0);
                byte[] data = _udpServer.Receive(ref remoteEp);

                _clientEndPoint ??= remoteEp;

                if (PacketHelper.TryParseDataPacket(data, out uint seq, out byte[] payload))
                {
                    _buffer.Insert(seq, payload);
                    _buffer.AdvanceContiguous();
                    bytesSinceLastStats += data.Length;
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                // Timeout — fall through to periodic checks
            }
            catch (SocketException ex) when (ct.IsCancellationRequested || ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                break;
            }

            var now = DateTime.UtcNow;

            // NACK scan on interval (not every packet)
            if ((now - lastNackTime).TotalMilliseconds >= _nackIntervalMs)
            {
                lastNackTime = now;

                var gaps = _buffer.ScanForGaps();
                var toNack = gaps.Where(seq =>
                    !_nackCooldown.TryGetValue(seq, out var lastNack) ||
                    (now - lastNack) > CooldownTime).ToList();

                if (toNack.Count > 0 && _clientEndPoint != null)
                {
                    foreach (uint seq in toNack)
                        _nackCooldown[seq] = now;
                    SendNacks(toNack);
                }

                // Clean cooldown entries for filled gaps
                if (_nackCooldown.Count > 100)
                {
                    var filled = _nackCooldown.Keys.Where(k => !_buffer.IsGap(k)).ToList();
                    foreach (uint seq in filled)
                        _nackCooldown.Remove(seq);
                }
            }

            // Stats every 1 second
            if ((now - lastStatsTime).TotalMilliseconds >= 1000)
            {
                double elapsed = (now - lastStatsTime).TotalSeconds;
                double rateMb = bytesSinceLastStats / elapsed / 1_000_000;

                Console.WriteLine(
                    $"[STAT] Received: {_buffer.TotalReceived} | " +
                    $"Gaps: {_buffer.TotalGapsDetected} | " +
                    $"NACKed: {NackSentCount} | " +
                    $"Rate: {rateMb:F2} MB/s | " +
                    $"Buffered: {_buffer.BufferedCount}/{_buffer.Capacity}");

                lastStatsTime = now;
                bytesSinceLastStats = 0;
            }
        }

        Console.WriteLine($"\n[SERVER] Shutdown. Final: Received={_buffer.TotalReceived}, Gaps={_buffer.TotalGapsDetected}");
    });

    private void SendNacks(List<uint> missingSeqs)
    {
        if (_clientEndPoint == null) return;

        const int maxPerNack = 300;
        for (int i = 0; i < missingSeqs.Count; i += maxPerNack)
        {
            var chunk = missingSeqs.Skip(i).Take(maxPerNack).ToList();
            byte[] nack = PacketHelper.BuildNackPacket(chunk);
            try
            {
                _udpServer.Send(nack, nack.Length, _clientEndPoint);
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                return;
            }
            NackSentCount += chunk.Count;
        }
    }
}
