// Compile the vendored Flight SQL SessionOptions proto subset
// (protos/flight_sql/session_options.proto) into Rust types via the same
// protox + tonic_prost_build toolchain penca-proto uses. Server/client
// stubs are not emitted — Flight SQL session options are exchanged as
// Action bodies on the existing FlightService, not as their own gRPC
// service.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../protos";
    let protos = [format!("{proto_root}/flight_sql/session_options.proto")];
    let includes = [proto_root.to_string()];

    println!("cargo:rerun-if-changed=build.rs");
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(&protos, &includes)?;

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_fds(file_descriptors)?;

    Ok(())
}
