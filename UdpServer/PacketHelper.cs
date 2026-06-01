using System.Buffers.Binary;

namespace UdpServer;

public static class PacketHelper
{
    public const byte NackMarker = 0xFF;

    public static bool TryParseDataPacket(byte[] data, out uint seq, out byte[] payload)
    {
        seq = 0;
        payload = [];

        if (data.Length < 6) return false;
        if (data[0] == NackMarker) return false;

        seq = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0, 4));
        ushort payloadLen = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(4, 2));

        if (data.Length < 6 + payloadLen) return false;

        payload = data.AsSpan(6, payloadLen).ToArray();
        return true;
    }

    public static byte[] BuildNackPacket(IReadOnlyList<uint> missingSeqs)
    {
        // [0xFF: 1B][Count: 2B BE][Seq1: 4B][Seq2: 4B]...
        byte[] packet = new byte[1 + 2 + missingSeqs.Count * 4];
        packet[0] = NackMarker;
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(1, 2), (ushort)missingSeqs.Count);

        for (int i = 0; i < missingSeqs.Count; i++)
        {
            BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(3 + i * 4, 4), missingSeqs[i]);
        }

        return packet;
    }
}
