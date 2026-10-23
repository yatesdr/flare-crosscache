pub mod arena;
pub mod disk;
pub mod protocol;
pub mod record;
pub mod server;
pub mod tree;

use std::io;

pub type Result<T> = io::Result<T>;

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    pub consumed_tokens: u64,
    pub digest: [u8; 16],
}
