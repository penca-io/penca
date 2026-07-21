// Proto compilation for Penca's gRPC services.
//
// This build script compiles all .proto files under protos/ into Rust types
// at build time. It generates:
//   - Message structs (via prost) for all request/response/domain types
//   - gRPC server traits and client stubs (via tonic) for all 4 services:
//       * WriteService     — catalog/schema/table DDL, branching,
//                            transactions, data mutations
//       * QueryService     — catalog/schema/table reads, branch/tx reads,
//                            ReadData, AuditData
//       * LifecycleService — persist, compact, snapshot, dirty-set listing
//
// Uses protox (pure-Rust protobuf parser) instead of system protoc to avoid
// version issues with proto3 optional fields (requires protoc >= 3.15).
//
// The generated code is re-exported from lib.rs as:
//   penca_proto::external::v1::*  (common, lifecycle, query, write)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../protos";

    let protos = [
        format!("{proto_root}/penca_proto/external/v1/common.proto"),
        format!("{proto_root}/penca_proto/external/v1/lifecycle.proto"),
        format!("{proto_root}/penca_proto/external/v1/query.proto"),
        format!("{proto_root}/penca_proto/external/v1/write.proto"),
    ];
    let includes = [proto_root.to_string()];

    println!("cargo:rerun-if-changed=build.rs");
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(&protos, &includes)?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(file_descriptors)?;

    Ok(())
}
