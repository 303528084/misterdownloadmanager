//! eDonkey2000 (ed2k) downloads (M16): the eMule network, in Rust.
//!
//! An `ed2k://|file|name|size|hash|/` link names a file on the eDonkey2000/Kad network by its
//! MD4-based hash, not by where it is: the peers holding it are found in the Kad distributed
//! hash table, and the bytes are asked of each peer with the eMule peer protocol (parts of
//! 9,728,000 bytes, each verified against the hash set the file's hash is built from).
//!
//! What this crate does, for the app: parse the link ([`Ed2kLink`]), add it as a download
//! ([`Ed2k::add`]), watch it ([`Ed2k::stats`]), pause, resume and remove it, and serve the
//! parts it has to other peers (eMule rewards a peer that uploads: without it, a download sits
//! at the end of everyone's queue).

pub mod download;
pub mod engine;
pub mod link;
pub mod nodes;
pub mod parts;
pub mod settings;

pub use download::State;
pub use engine::{Ed2k, Spec, Stats};
pub use link::{Ed2kHash, Ed2kLink, LinkError, looks_like_link, parse, parse_wrapped, safe_name};
pub use settings::Settings;

/// The size of a part (eMule's 9,500 KiB): the unit an ed2k file is hashed, verified and asked
/// in, and the smallest piece a peer will hand out.
pub const PART_SIZE: u64 = rucio_emule::ed2k::CHUNK_SIZE as u64;
