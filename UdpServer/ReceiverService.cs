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

    ~ReceiverService()
    {
        _udpServer.Dispose();
    }

    public Task RunAsync(CancellationToken ct) => Task.Run(() =>
    {
        Console.WriteLine($"[SERVER] Listening on port {_udpServer.Client.LocalEndPoint}...");
        Console.WriteLine("[SERVER] Press Ctrl+C to stop\n");

        var stats = new StatsState();
        var nackScheduler = new NackScheduler(_nackIntervalMs);
        int nackSentCount = 0;

        while (!ct.IsCancellationRequested)
        {
            if (TryReceive(out byte[]? data, out IPEndPoint? remoteEp))
            {
                _clientEndPoint ??= remoteEp;
                ProcessDataPacket(data!, ref stats.BytesSinceLastStats);
            }

            var now = DateTime.UtcNow;
            nackScheduler.TryRun(now, _buffer, _nackCooldown, _clientEndPoint, SendNacks, ref nackSentCount);
            NackSentCount = nackSentCount;
            stats.TryPrint(_buffer, nackSentCount);
        }

        Console.WriteLine($"\n[SERVER] Shutdown. Final: Received={_buffer.TotalReceived}, Gaps={_buffer.TotalGapsDetected}");
    });

    private bool TryReceive(out byte[]? data, out IPEndPoint? remoteEp)
    {
        data = null;
        remoteEp = null;
        try
        {
            remoteEp = new IPEndPoint(IPAddress.Any, 0);
            data = _udpServer.Receive(ref remoteEp);
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
    }

    private void ProcessDataPacket(byte[] data, ref long bytesSinceLastStats)
    {
        if (PacketHelper.TryParseDataPacket(data, out uint seq, out byte[] payload))
        {
            _buffer.Insert(seq, payload);
            _buffer.AdvanceContiguous();
            bytesSinceLastStats += data.Length;
        }
    }

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

    #region Structs
    private struct StatsState
    {
        public long BytesSinceLastStats;
        private DateTime _lastStatsTime = DateTime.UtcNow;

        public StatsState() { }

        public void TryPrint(RingBuffer buffer, int nackSentCount)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastStatsTime).TotalMilliseconds < 1000)
                return;

            double elapsed = (now - _lastStatsTime).TotalSeconds;
            double rateMb = BytesSinceLastStats / elapsed / 1_000_000;

            Console.WriteLine(
                $"[STAT] Received: {buffer.TotalReceived} | " +
                $"Gaps: {buffer.TotalGapsDetected} | " +
                $"NACKed: {nackSentCount} | " +
                $"Rate: {rateMb:F2} MB/s | " +
                $"Buffered: {buffer.BufferedCount}/{buffer.Capacity}");

            _lastStatsTime = now;
            BytesSinceLastStats = 0;
        }
    }

    private struct NackScheduler(int intervalMs)
    {
        private DateTime _lastNackTime = DateTime.UtcNow;

        public void TryRun(
            DateTime now,
            RingBuffer buffer,
            Dictionary<uint, DateTime> cooldown,
            IPEndPoint? clientEndPoint,
            Action<List<uint>> sendNacks,
            ref int nackSentCount)
        {
            if ((now - _lastNackTime).TotalMilliseconds < intervalMs)
                return;

            _lastNackTime = now;

            var gaps = buffer.ScanForGaps();
            var toNack = gaps.Where(seq =>
                !cooldown.TryGetValue(seq, out var lastNack) ||
                (now - lastNack) > CooldownTime).ToList();

            if (toNack.Count > 0 && clientEndPoint != null)
            {
                foreach (uint seq in toNack)
                    cooldown[seq] = now;
                sendNacks(toNack);
                nackSentCount += toNack.Count;
            }

            if (cooldown.Count > 100)
            {
                var filled = cooldown.Keys.Where(k => !buffer.IsGap(k)).ToList();
                foreach (uint seq in filled)
                    cooldown.Remove(seq);
            }
        }
    }
    #endregion
}
