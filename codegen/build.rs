fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::Path::new("../core/proto").canonicalize()?;
    let proto_file = proto_dir.join("iceberg.proto");
    
    tonic_build::configure()
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src")
        .compile(&[proto_file], &[proto_dir])
        .expect("Failed to compile protos");
    
    Ok(())
} 