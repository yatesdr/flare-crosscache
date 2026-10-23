use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "27c4d738b939ada33b8561fbff39744360ac9dc22cc8ab16d9ea3b338758c4c8";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/crosscache.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
