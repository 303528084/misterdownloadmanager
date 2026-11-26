//! What an ed2k session is told once, before its downloads: its ports, its name on the network,
//! the identity eMule's credits are kept against, and where its working files live.
//!
//! The identity (`user_hash`) is the one thing worth keeping between runs: eMule gives a peer
//! that uploads a place further ahead in the queues, credited to this hash, and a new one every
//! start would put every download back at the end of everyone's queue. It is made once and saved
//! beside the rest (`ed2k.json`).

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rucio_emule::kad::packet::KadId;
use rucio_emule::kad::search::KadSource;
use serde::{Deserialize, Serialize};

use crate::Ed2kHash;

/// Where a download asks for the peers that hold its file.
///
/// The real one is the Kad distributed hash table ([`crate::engine`]); a test gives a fixed
/// list, so that the download path itself is exercised without a network.
pub type SourceQuery = Arc<
    dyn Fn(Ed2kHash, u64) -> Pin<Box<dyn Future<Output = Vec<KadSource>> + Send>> + Send + Sync,
>;

/// The Kad bootstrap list MDM fetches when its own copy is old (eMule's public `nodes.dat`).
pub const NODES_DAT_URL: &str = "http://upd.emule-security.org/nodes.dat";

/// Everything an ed2k session needs.
#[derive(Clone)]
pub struct Settings {
    /// The TCP port peers connect to for their parts (`0`: one the system finds free — LowID,
    /// but a download still works, only slower in queues).
    pub tcp_port: u16,
    /// The UDP port Kad runs on (`0`: free).
    pub udp_port: u16,
    /// The name other peers see.
    pub nick: String,
    /// eMule's credit identity for this installation (16 bytes, kept between runs).
    pub user_hash: [u8; 16],
    /// How many peers may be served a part at once.
    pub max_peers: usize,
    /// How many peers may pull parts from this installation at once.
    pub upload_slots: usize,
    /// Keep serving a finished file to other peers (eMule's credits come from this).
    pub seed: bool,
    /// Where `.part` files and their progress live while a download runs.
    pub temp: PathBuf,
    /// Where `nodes.dat` and `ed2k.json` are kept.
    pub dir: PathBuf,
    /// The bootstrap list's address.
    pub nodes_dat_url: String,
    /// How old a saved `nodes.dat` may be before it is fetched again.
    pub nodes_ttl: Duration,
    /// A fixed source list, for tests (the Kad table otherwise).
    pub sources: Option<SourceQuery>,
}

impl Settings {
    /// The defaults, with the working files under `dir` and a fresh identity.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            tcp_port: 4662,
            udp_port: 4672,
            nick: "MDM".to_string(),
            user_hash: fresh_identity(),
            max_peers: 20,
            upload_slots: 4,
            seed: true,
            temp: dir.clone(),
            dir,
            nodes_dat_url: NODES_DAT_URL.to_string(),
            nodes_ttl: Duration::from_secs(6 * 60 * 60),
            sources: None,
        }
    }

    /// The defaults, with the identity saved in `dir` reused (or made and saved the first time).
    pub fn load(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let mut settings = Self::new(dir.clone());
        let kept = kept::Kept::read(&dir);
        settings.user_hash = kept.user_hash;
        settings.nick = kept.nick;
        if let Some(port) = kept.tcp_port {
            settings.tcp_port = port;
        }
        if let Some(port) = kept.udp_port {
            settings.udp_port = port;
        }
        if let Err(e) = settings.save() {
            tracing::warn!(error = %e, "ed2k: could not save the settings");
        }
        settings
    }

    /// Save what is worth keeping between runs (the identity, the name, the ports).
    pub fn save(&self) -> std::io::Result<()> {
        kept::Kept {
            user_hash: self.user_hash,
            nick: self.nick.clone(),
            tcp_port: Some(self.tcp_port),
            udp_port: Some(self.udp_port),
        }
        .write(&self.dir)
    }

    /// The `.part` file of a download: named by the hash, as eMule names its own, so the upload
    /// side finds it (`<hash>.part`, and the progress in `<hash>.part.met` beside it).
    pub fn part_path(&self, hash: &Ed2kHash) -> PathBuf {
        self.temp.join(format!("{}.part", hash.to_hex()))
    }

    /// Where the progress of that download's parts is kept.
    pub fn met_path(&self, hash: &Ed2kHash) -> PathBuf {
        self.temp.join(format!("{}.part.met", hash.to_hex()))
    }

    /// Where the bootstrap list is cached.
    pub fn nodes_path(&self) -> PathBuf {
        self.dir.join("nodes.dat")
    }

    /// Make the folders this session writes into.
    pub fn prepare(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        std::fs::create_dir_all(&self.temp)
    }
}

/// A new credit identity: the Kad id generator, which reads the system's randomness, is as good
/// a source of 16 unpredictable bytes as this needs.
fn fresh_identity() -> [u8; 16] {
    *KadId::random().as_bytes()
}

/// What `ed2k.json` holds.
pub mod kept {
    use super::*;

    /// The kept settings, as the file writes them.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Kept {
        pub user_hash: [u8; 16],
        pub nick: String,
        #[serde(default)]
        pub tcp_port: Option<u16>,
        #[serde(default)]
        pub udp_port: Option<u16>,
    }

    impl Kept {
        const FILE: &'static str = "ed2k.json";

        /// The file in `dir`, or the defaults when there is none or it can't be read (a first
        /// run, a hand-edited file, a disk gone away).
        pub fn read(dir: &Path) -> Self {
            let fresh = Self {
                user_hash: super::fresh_identity(),
                nick: "MDM".to_string(),
                tcp_port: None,
                udp_port: None,
            };
            let Ok(text) = std::fs::read_to_string(dir.join(Self::FILE)) else {
                return fresh;
            };
            serde_json::from_str(&text).unwrap_or(fresh)
        }

        pub fn write(&self, dir: &Path) -> std::io::Result<()> {
            std::fs::create_dir_all(dir)?;
            let text = serde_json::to_string_pretty(self)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            std::fs::write(dir.join(Self::FILE), text)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_identity_is_kept_between_runs() {
        let dir = tempfile::tempdir().expect("a folder");
        let first = Settings::load(dir.path());
        let second = Settings::load(dir.path());
        assert_eq!(first.user_hash, second.user_hash);
        assert_ne!(first.user_hash, [0u8; 16]);
    }

    #[test]
    fn a_broken_file_falls_back_to_the_defaults() {
        let dir = tempfile::tempdir().expect("a folder");
        std::fs::write(dir.path().join("ed2k.json"), "{not json").expect("write");
        let s = Settings::load(dir.path());
        assert_eq!(s.tcp_port, 4662);
        assert_eq!(s.temp, dir.path());
    }

    #[test]
    fn working_files_are_named_by_the_hash_as_emule_names_them() {
        let dir = tempfile::tempdir().expect("a folder");
        let s = Settings::new(dir.path());
        let hash = Ed2kHash::from_hex("0123456789abcdef0123456789abcdef").expect("hash");
        assert_eq!(
            s.part_path(&hash),
            dir.path().join("0123456789abcdef0123456789abcdef.part")
        );
        assert_eq!(
            s.met_path(&hash),
            dir.path().join("0123456789abcdef0123456789abcdef.part.met")
        );
    }
}
