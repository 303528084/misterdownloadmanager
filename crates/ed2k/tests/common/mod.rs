//! What the ed2k tests share: a peer serving a file on loopback (the eMule peer protocol, the
//! same code eMule talks to), a session pointed at it, and a wait for a download to end.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mdm_ed2k::{Ed2k, PART_SIZE, Settings, State};
use rucio_emule::ed2k::{finalize_hashset, hash_reader_full};
use rucio_emule::kad::search::KadSource;
use rucio_emule::transfer::{UploadContext, UploadInfo, UploadQueue, serve_incoming};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, Semaphore};

/// A file of two parts: the first full, the second short (so the last-part arithmetic runs).
pub const SIZE: u64 = PART_SIZE + 2_000_000;

/// Write a file of `SIZE` bytes of pseudo-random data (not compressible, not a pattern).
pub fn make_file(path: &std::path::Path) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).expect("create");
    let mut seed = 0x1234_5678_9abc_def0u64;
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0u64;
    while written < SIZE {
        for byte in buf.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (seed >> 33) as u8;
        }
        let n = std::cmp::min(buf.len() as u64, SIZE - written) as usize;
        file.write_all(&buf[..n]).expect("write");
        written += n as u64;
    }
}

pub struct Serving {
    pub port: u16,
    pub uploaded: Arc<AtomicU64>,
}

/// Serve `path` as an eMule peer on a free loopback port; returns the port and the bytes it has
/// sent (to prove a resumed download did not ask for the parts it already had).
pub async fn serve(path: &std::path::Path, name: &str) -> Serving {
    let (hash, _) = hash_reader_full(std::fs::File::open(path).expect("open")).expect("hash");
    serve_claiming(path, name, hash).await
}

/// Serve `path`'s bytes as the peer's copy of the file with hash `claimed` (a test's lie: the
/// bytes are not that file's, and the hash set it offers is not that file's either).
pub async fn serve_claiming(
    path: &std::path::Path,
    name: &str,
    claimed: rucio_emule::ed2k::Ed2kHash,
) -> Serving {
    let (own_hash, parts) =
        hash_reader_full(std::fs::File::open(path).expect("open")).expect("hash");
    let info = UploadInfo {
        name: name.to_string(),
        total_size: SIZE,
        num_slices: parts.len(),
        path: path.to_path_buf(),
        complete: true,
        hashset: finalize_hashset(&parts, SIZE, &own_hash),
    };
    let downloads = Arc::new(RwLock::new(HashMap::from([(*claimed.as_bytes(), info)])));
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let uploaded = Arc::new(AtomicU64::new(0));
    let context = Arc::new(UploadContext {
        slots: Arc::new(Semaphore::new(4)),
        queue: UploadQueue::default(),
        temp_dir: path.parent().expect("parent").to_path_buf(),
        tcp_port: port,
        kad_udp_port: 0,
        user_hash: [9u8; 16],
        nick: "test-peer".to_string(),
        downloads,
        inbound_connections: Arc::new(AtomicU64::new(0)),
        last_inbound_at: Arc::new(AtomicU64::new(0)),
        uploaded_bytes: Arc::clone(&uploaded),
        chunks_served: Arc::new(AtomicU64::new(0)),
        upload_limiter: None,
        upload_observer: None,
    });
    tokio::spawn(serve_incoming(listener, context));
    Serving { port, uploaded }
}

/// A session whose "network" is the one peer at `port`.
pub fn settings_for(dir: &std::path::Path, port: u16) -> Settings {
    let mut settings = Settings::new(dir);
    settings.tcp_port = 0;
    settings.udp_port = 0;
    settings.max_peers = 2;
    settings.seed = false;
    settings.sources = Some(Arc::new(move |_hash, _size| {
        Box::pin(async move {
            vec![KadSource {
                ip: Ipv4Addr::LOCALHOST,
                tcp_port: port,
                udp_port: 0,
                user_hash: [9u8; 16],
            }]
        })
    }));
    settings
}

/// Wait for a download to leave `Running`.
pub async fn wait_finished(ed2k: &Arc<Ed2k>, id: u64, within: Duration) -> mdm_ed2k::Stats {
    let start = Instant::now();
    loop {
        let stats = ed2k.stats_of(id).await.expect("the download is in the list");
        if stats.state != State::Running {
            return stats;
        }
        assert!(
            start.elapsed() < within,
            "still running after {within:?}: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

