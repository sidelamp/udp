namespace UdpServer;

public class RingBuffer
{
    private readonly Slot[] _buffer;

    public int Capacity { get; }
    public uint BaseSeq { get; private set; }
    public uint HighestSeqSeen { get; private set; }
    public uint NextExpected { get; private set; }
    public int TotalReceived { get; private set; }
    public int TotalGapsDetected { get; private set; }

    public RingBuffer(int capacity)
    {
        Capacity = capacity;
        _buffer = new Slot[capacity];
    }

    public void Insert(uint seq, byte[] data)
    {
        if (seq < BaseSeq) return; // too old, evicted

        // Detect new gaps: packets between HighestSeqSeen and seq are missing
        if (TotalReceived > 0 && seq > HighestSeqSeen)
        {
            uint newGaps = seq - HighestSeqSeen - 1;
            TotalGapsDetected += (int)newGaps;
        }

        // Advance window if needed
        if (seq >= BaseSeq + (uint)Capacity)
        {
            uint advance = seq - BaseSeq - (uint)Capacity + 1;
            for (uint i = 0; i < advance; i++)
            {
                int idx = (int)((BaseSeq + i) % (uint)Capacity);
                _buffer[idx] = default;
            }
            BaseSeq += advance;
            if (NextExpected < BaseSeq)
                NextExpected = BaseSeq;
        }

        int index = (int)((seq - BaseSeq) % (uint)Capacity);
        _buffer[index] = new Slot(data, true);
        TotalReceived++;

        if (seq > HighestSeqSeen || TotalReceived == 1)
            HighestSeqSeen = seq;
    }

    public void AdvanceContiguous()
    {
        while (NextExpected <= HighestSeqSeen)
        {
            int idx = (int)((NextExpected - BaseSeq) % (uint)Capacity);
            if (!_buffer[idx].Occupied) break;
            NextExpected++;
        }
    }

    public List<uint> ScanForGaps()
    {
        var gaps = new List<uint>();
        if (TotalReceived == 0) return gaps;

        for (uint seq = NextExpected; seq <= HighestSeqSeen; seq++)
        {
            int idx = (int)((seq - BaseSeq) % (uint)Capacity);
            if (!_buffer[idx].Occupied)
                gaps.Add(seq);
        }
        return gaps;
    }

    public bool IsGap(uint seq)
    {
        if (seq < BaseSeq || seq > HighestSeqSeen) return false;
        int idx = (int)((seq - BaseSeq) % (uint)Capacity);
        return !_buffer[idx].Occupied;
    }

    public int BufferedCount => (int)Math.Max(0, HighestSeqSeen - NextExpected + 1);

    private readonly record struct Slot(byte[]? Data, bool Occupied);
}
