fn main() {
    println!("cargo:rerun-if-changed=proto/crosscache.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/crosscache.proto"], &["proto"])
        .expect("compile crosscache protobuf schema");
}
