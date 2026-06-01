using System.Buffers.Binary;

namespace UdpClient;

public static class PacketHelper
{
    public const byte NackMarker = 0xFF;

    public static byte[] BuildDataPacket(uint seq, byte[] payload)
    {
        // [SeqNum: 4B BE][PayloadLen: 2B BE][Payload: N bytes]
        byte[] packet = new byte[4 + 2 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0, 4), seq);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(4, 2), (ushort)payload.Length);
        payload.CopyTo(packet.AsSpan(6));
        return packet;
    }

    public static bool TryParseNack(byte[] data, out List<uint> missingSeqs)
    {
        missingSeqs = [];

        if (data.Length < 3) return false;
        if (data[0] != NackMarker) return false;

        ushort count = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(1, 2));
        if (data.Length < 3 + count * 4) return false;

        for (int i = 0; i < count; i++)
        {
            missingSeqs.Add(BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(3 + i * 4, 4)));
        }

        return true;
    }
}
