# file-service

A portable, stack-agnostic gRPC microservice that owns S3-compatible object
storage I/O and ClamAV malware scanning. Drop it into any project that needs
MinIO-backed file handling without re-implementing storage, streaming, or
antivirus logic.

**Module path:** `github.com/ctf-platform/file-service`
To use your own path, run a single find-and-replace across the repository:

```bash
grep -rl 'github.com/ctf-platform/file-service' . | xargs sed -i \
  's|github.com/ctf-platform/file-service|github.com/your-org/file-service|g'
```

---

## Quick start

```bash
cp .env.example .env          # fill in MINIO_ACCESS_KEY and MINIO_SECRET_KEY
docker compose up -d
```

Verify the gRPC health check:

```bash
docker compose exec file-service /grpc_health_probe -addr=:50051
# status: SERVING
```

---

## What it does

| Responsibility | Implemented |
|---|---|
| Streaming download (64 KiB chunks, never buffers the full file) | GetFile RPC |
| Streaming upload (multipart, 16 MiB parts) | PutFile RPC |
| Delete / stat / list objects | DeleteFile, StatFile, ListFiles |
| Time-limited presigned download URLs | GetPresignedUrl RPC |
| ClamAV INSTREAM malware scan | ScanFile RPC |
| Redis-backed presigned URL cache (80 % of expiry TTL) | built into GetPresignedUrl |
| Redis-backed stat cache (60 s TTL, auto-invalidated on write) | built into StatFile |
| gRPC Health Checking Protocol | `google.golang.org/grpc/health` |

What it does **not** do: authentication, authorisation, database persistence,
or any application-level business logic. All of that belongs in the consuming
service.

---

## gRPC API

Defined in `proto/fileservice/v1/fileservice.proto`.

### RPCs

| RPC | Type | Description |
|-----|------|-------------|
| `GetFile` | server-streaming | Stream object in 64 KiB chunks. `content_type` on first chunk only. |
| `PutFile` | client-streaming | First message = `PutFileMeta`, remaining = raw 64 KiB chunks. |
| `DeleteFile` | unary | Remove object; evicts stat and presigned-URL caches. |
| `StatFile` | unary | Object metadata. Redis-cached for 60 s. |
| `ListFiles` | unary | All objects under an optional prefix. |
| `GetPresignedUrl` | unary | Time-limited download URL. Redis-cached. |
| `ScanFile` | client-streaming | Stream file to ClamAV INSTREAM; returns `ScanResult`. |

### ScanResult enum

| Value | Meaning |
|-------|---------|
| `SCAN_RESULT_CLEAN` | No malware. |
| `SCAN_RESULT_INFECTED` | Threat detected; `virus_name` is populated. |
| `SCAN_RESULT_UNAVAILABLE` | ClamAV not configured or unreachable (fail-open). |

---

## Configuration

All configuration is read from environment variables at startup.

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `MINIO_ENDPOINT` | `minio` | — | MinIO hostname |
| `MINIO_PORT` | `9000` | — | MinIO port |
| `MINIO_ACCESS_KEY` | — | **yes** | Access key |
| `MINIO_SECRET_KEY` | — | **yes** | Secret key |
| `MINIO_USE_SSL` | `false` | — | Enable TLS to MinIO |
| `REDIS_URL` | — | — | Redis URL; caching disabled when empty |
| `CLAMAV_HOST` | `clamav` | — | clamd hostname |
| `CLAMAV_PORT` | — | — | clamd port; scanning disabled when empty |
| `GRPC_PORT` | `50051` | — | gRPC listen port |

---

## Streaming design

Every file transfer uses an `io.Pipe` to bridge the gRPC stream and
MinIO/ClamAV without buffering the full file in memory.

```
GetFile:   MinIO reader ──64 KiB──► stream.Send()

PutFile:   stream.Recv() ──64 KiB──► io.Pipe ──► minio PutObject()
                                                  (multipart, 16 MiB parts)

ScanFile:  stream.Recv() ──64 KiB──► io.Pipe ──► clamd INSTREAM
```

The 64 KiB chunk size is exported as `minioclient.ChunkSize` so client
implementations can use the same value.

---

## ClamAV notes

The INSTREAM protocol is implemented directly over TCP without any third-party
library. The `z`-prefix (`zINSTREAM\x00`) is used, which means clamd sends
null-terminated responses; the scanner trims `\x00` as well as whitespace
before comparing suffixes (`" OK"`, `" FOUND"`).

All network errors and unexpected responses return `ResultUnavailable`
(fail-open). The consuming service decides how to handle unavailability
(e.g. mark the file as pending and retry later).

---

## Building

### Prerequisites

```bash
# Go toolchain
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# protoc - https://grpc.io/docs/protoc-installation/
```

### Commands

```bash
make tidy       # go mod tidy
make generate   # regenerate gRPC stubs from proto/
make build      # compile the server binary
make test       # run all tests
```

The `gen/` directory is committed so the service builds without running
`make generate` in CI. Only regenerate when the proto contract changes.

---

## Integrating from another project

### Adding to an existing Docker Compose stack

```yaml
# your project's compose.yml
services:
  file-service:
    image: ghcr.io/ctf-platform/file-service:latest   # or build: ./file-service
    restart: unless-stopped
    environment:
      MINIO_ENDPOINT:   minio
      MINIO_PORT:       "9000"
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}
      REDIS_URL:        redis://redis:6379/0
      CLAMAV_HOST:      clamav
      CLAMAV_PORT:      "3310"
    networks: [appdata, scan]
    healthcheck:
      test: ["/grpc_health_probe", "-addr=:50051"]
      interval: 10s
      retries: 5
```

Set `FILE_SERVICE_ADDR=file-service:50051` in your application container.

### Generating a client

Run `protoc` (or `buf generate`) against
`proto/fileservice/v1/fileservice.proto` to produce stubs for your stack.

#### Node.js / TypeScript (NestJS, Next.js, Express)

```bash
npm install @grpc/grpc-js @grpc/proto-loader
# or generate typed stubs with ts-proto:
npm install -D ts-proto
protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto \
       --ts_proto_out=./src/generated \
       --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true \
       -I ./proto proto/fileservice/v1/fileservice.proto
```

Runtime loading without codegen (fastest to get started):

```typescript
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'node:path';

const def = protoLoader.loadSync(
  path.join(__dirname, 'proto/fileservice/v1/fileservice.proto'),
  { keepCase: true, longs: Number, enums: String, defaults: true, oneofs: true },
);
const proto = grpc.loadPackageDefinition(def) as any;
const client = new proto.fileservice.v1.FileService(
  process.env.FILE_SERVICE_ADDR ?? 'localhost:50051',
  grpc.credentials.createInsecure(),
);
```

#### Python (Django, FastAPI, Flask)

```bash
pip install grpcio grpcio-tools
python -m grpc_tools.protoc \
  -I ./proto --python_out=./generated --grpc_python_out=./generated \
  proto/fileservice/v1/fileservice.proto
```

```python
import grpc
from generated.fileservice.v1 import fileservice_pb2, fileservice_pb2_grpc

channel = grpc.insecure_channel(os.environ['FILE_SERVICE_ADDR'])
stub    = fileservice_pb2_grpc.FileServiceStub(channel)

for chunk in stub.GetFile(fileservice_pb2.GetFileRequest(bucket='b', key='k')):
    process(chunk.data)
```

#### Ruby on Rails

```bash
gem install grpc grpc-tools
grpc_tools_ruby_protoc -I ./proto \
  --ruby_out=./lib/generated --grpc_out=./lib/generated \
  proto/fileservice/v1/fileservice.proto
```

#### Java / Spring Boot

Use `protoc-gen-java` + `protoc-gen-grpc-java` or the
`com.google.protobuf:protobuf-maven-plugin`.

#### Go (another module)

```go
import (
    "google.golang.org/grpc"
    pb "github.com/ctf-platform/file-service/gen/fileservice/v1"
)

conn, _  := grpc.NewClient("file-service:50051", grpc.WithInsecure())
client   := pb.NewFileServiceClient(conn)
stream, _ := client.GetFile(ctx, &pb.GetFileRequest{Bucket: "b", Key: "k"})
```

---

## Versioning the contract

The proto package is `fileservice.v1`. Breaking changes (removed fields,
changed types) must move to `fileservice.v2` with a corresponding new
Go module path suffix. Additive changes (new optional fields, new RPCs)
are backward-compatible and do not require a version bump.

---

## Health check

The Dockerfile ships `grpc-health-probe` at `/grpc_health_probe`. The service
registers `google.golang.org/grpc/health` so the probe can verify `SERVING`.

```bash
/grpc_health_probe -addr=:50051
# status: SERVING
```

For Kubernetes:

```yaml
livenessProbe:
  exec:
    command: ["/grpc_health_probe", "-addr=:50051"]
  initialDelaySeconds: 10
  periodSeconds: 10
```
