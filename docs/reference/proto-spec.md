# Proto Specification

The canonical API contract is defined in `proto/pelicanq/v1/`. Three proto files define the entire surface:

- `message.proto` — Message and ConsumedMessage types
- `queue.proto` — QueueService RPC definitions
- `admin.proto` — AdminService RPC definitions

## Generating Code

### Rust

Generated automatically by `build.rs` using `tonic_build`.

### Go

```bash
protoc --proto_path=proto --go_out=sdks/go/pelicanq \
  --go_opt=module=github.com/Open-Collective-Labs/PelicanQ/sdks/go/pelicanq \
  --go-grpc_out=sdks/go/pelicanq \
  --go-grpc_opt=module=github.com/Open-Collective-Labs/PelicanQ/sdks/go/pelicanq \
  proto/pelicanq/v1/*.proto
```

### Python

```bash
python3 -m grpc_tools.protoc --proto_path=proto \
  --python_out=sdks/python --grpc_python_out=sdks/python \
  proto/pelicanq/v1/*.proto
```

### Java

```bash
mvn generate-sources  # uses protobuf-maven-plugin
```
