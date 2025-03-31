fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../proto";

    let proto_files = ["iceberg.proto"];
    tonic_build::configure()
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("./src")
        .compile(&proto_files, &[proto_dir.to_owned()])
        .expect("Failed to compile protos");
    Ok(())
}
