//! Standalone tests for the network-phase modules while P1 owns `lib.rs`.
//!
//! Each module is included by path so it compiles and its `#[cfg(test)]` tests run before the
//! modules are declared in the library. When P1 lands, delete this file and import the modules.

pub use selene::{Key, proto, record};

pub type Result<T> = std::io::Result<T>;

pub fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

#[allow(dead_code)]
#[path = "../src/arena.rs"]
mod arena;

#[path = "../src/regions.rs"]
mod regions;

#[path = "../src/config.rs"]
mod config;

#[path = "../src/admin.rs"]
mod admin;

#[path = "../src/data/mod.rs"]
mod data;

#[path = "../src/grants.rs"]
mod grants;

#[path = "../src/placement.rs"]
mod placement;

#[path = "../src/peer.rs"]
mod peer;

#[allow(dead_code)]
#[path = "../src/peer_client.rs"]
mod peer_client;
