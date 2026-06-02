# UDP Reliable Transfer Demo

A C# / .NET 8.0 demonstration of reliable data transfer over UDP using sequence numbers, NACK-based retransmission, and a ring buffer for ordered delivery.

## Architecture

The solution contains two console applications:

```
udp.sln
├── UdpClient   — sends data packets, handles retransmissions on NACK
└── UdpServer   — receives packets, detects gaps, sends NACKs
```

### Flow

```
Client                          Server
  |                                |
  |── DATA(seq=N, payload) ──────▶|
  |                                | insert into RingBuffer
  |                                | detect gaps
  |◀── NACK([seq=M, ...]) ────────|
  |                                |
  |── DATA(seq=M, payload) ──────▶| retransmit
```

## Packet Format

### Data Packet (Client → Server)

| Field       | Size    | Description            |
|-------------|---------|------------------------|
| SeqNum      | 4 bytes | Sequence number (big-endian `uint32`) |
| PayloadLen  | 2 bytes | Payload length (big-endian `uint16`)  |
| Payload     | N bytes | Random data            |

### NACK Packet (Server → Client)

| Field       | Size    | Description            |
|-------------|---------|------------------------|
| Marker      | 1 byte  | `0xFF` — distinguishes NACK from data |
| Count       | 2 bytes | Number of missing sequences (big-endian `uint16`) |
| SeqNumbers  | 4 × N   | Missing sequence numbers (big-endian `uint32` each) |

## Components

### UdpClient

- **SendService** — generates random payloads and sends data packets at a target rate of 1000 pkt/s. Randomly skips packets (configurable probability) to simulate loss. Stores sent packets in a `ConcurrentDictionary` for potential retransmission.
- **RetransmitService** — listens for NACK packets from the server and retransmits the requested sequences from the pending-packets store.
- **PacketHelper** — builds data packets and parses NACK packets using big-endian binary serialization (`System.Buffers.Binary`).

### UdpServer

- **ReceiverService** — listens on a UDP port, parses incoming data packets, inserts payloads into the ring buffer, periodically scans for gaps, and sends NACKs for missing sequences. Uses a cooldown to avoid duplicate NACKs.
- **RingBuffer** — fixed-capacity circular buffer indexed by sequence number. Supports out-of-order insertion, contiguous advancement, and gap scanning.
- **PacketHelper** — parses data packets and builds NACK packets.

## Configuration

### Client (`UdpClient/Program.cs`)

| Constant          | Default    | Description                     |
|-------------------|------------|---------------------------------|
| `ServerHost`      | `127.0.0.1`| Server IP address               |
| `ServerPort`      | `5000`     | Server UDP port                 |
| `PayloadSize`     | `1024`     | Payload size in bytes           |
| `SkipProbability` | `0.02`     | Probability of skipping a packet (simulates loss) |

### Server (`UdpServer/Program.cs`)

| Constant          | Default | Description                     |
|-------------------|---------|---------------------------------|
| `ListenPort`      | `5000`  | UDP port to listen on           |
| `BufferCapacity`  | `1000`  | Ring buffer slot count          |
| `NackIntervalMs`  | `100`   | Interval between NACK scans (ms)|

## Running

```bash
# Start the server first
dotnet run --project UdpServer

# Then start the client
dotnet run --project UdpClient
```

## Running on Linux

### Prerequisites

Install the .NET 8.0 SDK (or runtime for published apps):

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install -y dotnet-sdk-8.0

# Or use the official install script:
# https://dot.net/v1/dotnet-install.sh
```

### Run from source

```bash
# Terminal 1 — start the server
dotnet run --project UdpServer

# Terminal 2 — start the client
dotnet run --project UdpClient
```

### Publish

Build self-contained, single-file executables for Linux x64:

```bash
# Server
dotnet publish UdpServer -c Release -r linux-x64 --self-contained true \
  -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true \
  -o publish/server

# Client
dotnet publish UdpClient -c Release -r linux-x64 --self-contained true \
  -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true \
  -o publish/client
```

> For a smaller output that still requires the .NET runtime on the target machine,
> replace `--self-contained true` with `--self-contained false`.

### Run published binaries

```bash
# Copy the publish/ folder to the target Linux machine, then:

# Terminal 1 — server
chmod +x publish/server/UdpServer
./publish/server/UdpServer

# Terminal 2 — client
chmod +x publish/client/UdpClient
./publish/client/UdpClient
```

No .NET runtime is required on the target machine when published with `--self-contained true`.

Press **Ctrl+C** to stop either application gracefully.

## Output

Both applications print periodic statistics:

- **Client**: sent count, skipped count, retransmitted count, throughput (MB/s)
- **Server**: received count, gaps detected, NACKed count, throughput (MB/s), buffer utilization
