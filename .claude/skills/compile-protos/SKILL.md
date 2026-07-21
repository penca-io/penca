---
name: compile-protos
description: Regenerate Python protobuf bindings after .proto file changes
disable-model-invocation: true
allowed-tools: Bash Read Glob
---

Regenerate the Python protobuf bindings from `.proto` source files.

```bash
just compile-protos
```

Generated `*_pb2.py` and `*_pb2.pyi` files go to
`packages/penca-proto/src/penca_proto/`, mirroring the proto package
structure.

Report any compilation errors. On success, list the files that were updated.
