use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "a57b72dffb9cfe3dada3bf4dba8f6bbd0aec30f6d77d9c6ac708019f402eccea";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/crosscache.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
