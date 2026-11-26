//! The session: one Kad table, one server for the parts this installation has, and the
//! downloads it is running.
//!
//! A session is started once (by the app, beside its torrent session) and asked for downloads
//! as the person adds links. Every download runs as its own tasks: a supervisor that keeps
//! asking the network which peers hold the file, and a few workers that fetch parts from them.
//! The parts server answers other peers with whatever this installation has — while a file is
//! downloading, and after it is done ([`Settings::seed`]) — because that is what keeps a peer
//! near the front of eMule's queues.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use rucio_emule::kad::packet::KadId;
use rucio_emule::kad::task::{KadHandle, KadTaskConfig};
use rucio_emule::transfer::{ActiveDownloads, UploadContext, UploadInfo, UploadQueue};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{OnceCell, RwLock, Semaphore};
use tokio::task::JoinSet;

use crate::download::{Download, State};
use crate::parts::count_of;
use crate::{Ed2kLink, Settings, nodes};

/// What to download: the link, and the folder its file goes into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spec {
    pub link: Ed2kLink,
    pub dir: PathBuf,
}

/// How one download is doing (what the app's list shows).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub id: u64,
    /// The file's hash: what the session and the app know it by.
    pub hash: crate::Ed2kHash,
    pub name: String,
    pub size: u64,
    /// The bytes here (whole parts only).
    pub downloaded: u64,
    pub parts: usize,
    pub done_parts: usize,
    /// Peers Kad (or the test's list) says hold the file.
    pub sources: usize,
    /// Peers being asked for a part right now.
    pub peers: usize,
    pub state: State,
    pub error: Option<String>,
    /// Where the finished file went (its name numbered when another file had it).
    pub path: Option<PathBuf>,
}

/// An ed2k session.
pub struct Ed2k {
    inner: Arc<Inner>,
}

struct Inner {
    settings: Settings,
    /// The Kad table: started the first time a download asks for peers, not at startup (an
    /// installation that only downloads over HTTP never touches UDP).
    kad: OnceCell<KadHandle>,
    uploads: OnceCell<Arc<UploadContext>>,
    downloads: tokio::sync::Mutex<HashMap<u64, Arc<Download>>>,
    /// What the parts server may serve: the downloads this session is running, and the files
    /// it finished and keeps seeding.
    served: ActiveDownloads,
    ids: AtomicU64,
    /// The port the parts server really listens on (0 until it starts, and when it found none).
    tcp_port: AtomicU16,
    stopped: AtomicBool,
}

/// The Kad key a file's sources are stored under is the file's hash exactly as the link writes
/// it, but the Kad task of `rucio-emule` reverses each 4-byte word of the hash it is handed (what
/// eMule does for a keyword's MD4, not for a file). Reversing the words here cancels that out:
/// without it a source search looks under a key nothing is ever published under and every link
/// finds 0 sources, while a publish of our own is still found (both sides being wrong alike).
///
/// Remove this when the Kad task stops reversing a file's hash.
fn kad_key(hash: crate::Ed2kHash) -> crate::Ed2kHash {
    let mut bytes = *hash.as_bytes();
    for word in bytes.chunks_mut(4) {
        word.reverse();
    }
    crate::Ed2kHash::from_bytes(bytes)
}

impl Ed2k {
    /// Start a session: its parts server first (so downloads can advertise the port), the Kad
    /// table when the first download needs it.
    pub async fn start(settings: Settings) -> Result<Arc<Self>> {
        settings
            .prepare()
            .with_context(|| format!("making the ed2k folder {}", settings.dir.display()))?;
        let inner = Arc::new(Inner {
            served: Arc::new(RwLock::new(HashMap::new())),
            settings,
            kad: OnceCell::new(),
            uploads: OnceCell::new(),
            downloads: tokio::sync::Mutex::new(HashMap::new()),
            ids: AtomicU64::new(1),
            tcp_port: AtomicU16::new(0),
            stopped: AtomicBool::new(false),
        });
        inner.start_serving().await?;
        Ok(Arc::new(Self { inner }))
    }

    /// The port peers connect to for their parts (`0`: none, and this installation is LowID).
    pub fn tcp_port(&self) -> u16 {
        self.inner.tcp_port.load(Ordering::Relaxed)
    }

    /// Add a download: its parts resume from what an earlier run left, and it starts at once.
    pub async fn add(&self, spec: Spec) -> Result<u64> {
        let settings = &self.inner.settings;
        let id = self.inner.ids.fetch_add(1, Ordering::Relaxed);
        let part_path = settings.part_path(&spec.link.hash);
        let met_path = settings.met_path(&spec.link.hash);
        let count = count_of(spec.link.size);

        let done = rucio_emule::progress::load_progress(&met_path, count);
        // A part file with no progress beside it (an interrupted first run, a lost `.part.met`):
        // what it holds is found again by hashing, once a peer's hash set is known.
        let rebuild = done.iter().all(|d| !*d) && part_path.exists();
        let download = Arc::new(Download::new(
            id,
            spec.link.clone(),
            spec.dir,
            part_path.clone(),
            met_path,
            done,
            rebuild,
        ));
        self.inner
            .downloads
            .lock()
            .await
            .insert(id, download.clone());
        self.inner.served.write().await.insert(
            *spec.link.hash.as_bytes(),
            UploadInfo {
                name: spec.link.name.clone(),
                total_size: spec.link.size,
                num_slices: count,
                path: part_path,
                complete: false,
                hashset: Vec::new(),
            },
        );

        let inner = Arc::clone(&self.inner);
        tokio::spawn(supervise(inner, download));
        Ok(id)
    }

    /// Stop a download; its parts stay for the next start.
    pub async fn pause(&self, id: u64) {
        if let Some(download) = self.get(id).await {
            download.set_state(State::Paused);
        }
    }

    /// Start a paused download again from its parts.
    pub async fn resume(&self, id: u64) {
        let Some(download) = self.get(id).await else {
            return;
        };
        if download.state() == State::Finished {
            return;
        }
        download.set_state(State::Running);
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // A run that is still winding down keeps the download's `running` flag; wait for it
            // rather than lose the resume.
            for _ in 0..600 {
                if !download.running.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            supervise(inner, download).await;
        });
    }

    /// Forget a download and delete its parts (`keep_file`: the finished file is kept).
    pub async fn remove(&self, id: u64, keep_file: bool) {
        let Some(download) = self.get(id).await else {
            return;
        };
        download.set_state(State::Paused);
        self.inner.downloads.lock().await.remove(&id);
        if !keep_file {
            let _ = std::fs::remove_file(&download.part_path);
        }
        let _ = std::fs::remove_file(&download.met_path);
        self.inner
            .served
            .write()
            .await
            .remove(download.link.hash.as_bytes());
    }

    /// Every download, oldest first.
    pub async fn stats(&self) -> Vec<Stats> {
        let map = self.inner.downloads.lock().await;
        let mut all: Vec<Stats> = map
            .values()
            .map(|download| Stats {
                id: download.id,
                hash: download.link.hash,
                name: download.link.name.clone(),
                size: download.link.size,
                downloaded: download.downloaded(),
                parts: download.parts.count(),
                done_parts: download.parts.done_count(),
                sources: download.sources(),
                peers: download.peers(),
                state: download.state(),
                error: download.error(),
                path: download.target(),
            })
            .collect();
        all.sort_by_key(|s| s.id);
        all
    }

    /// One download's state, if it is still in the list.
    pub async fn stats_of(&self, id: u64) -> Option<Stats> {
        self.stats().await.into_iter().find(|s| s.id == id)
    }

    /// How many Kad contacts this installation knows (0 before the first download starts the
    /// table, and when Kad could not be joined): what a settings page shows.
    pub async fn kad_contacts(&self) -> usize {
        match self.inner.kad().await {
            Some(kad) => kad.contact_count().await,
            None => 0,
        }
    }

    /// Which peers hold a file, without downloading it (what Add URL shows before a link is
    /// added).
    pub async fn sources(
        &self,
        hash: crate::Ed2kHash,
        size: u64,
    ) -> Vec<rucio_emule::kad::search::KadSource> {
        match self.inner.kad().await {
            Some(kad) => kad.search_sources(kad_key(hash), size).await,
            None => Vec::new(),
        }
    }

    /// Share a file this installation already has: hash it, keep it for the peers that ask for
    /// it, and publish it to Kad, so a link to it finds this machine. `Ok` is its ed2k hash — what
    /// an `ed2k://` link to it carries.
    pub async fn share(&self, path: std::path::PathBuf) -> Result<crate::Ed2kHash> {
        let file = std::fs::File::open(&path)?;
        let size = file.metadata()?.len();
        let (hash, parts) = rucio_emule::ed2k::hash_reader_full(file)?;
        let hashset = rucio_emule::ed2k::finalize_hashset(&parts, size, &hash);
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.inner.served.write().await.insert(
            *hash.as_bytes(),
            UploadInfo {
                name: name.clone(),
                total_size: size,
                num_slices: parts.len(),
                path,
                complete: true,
                hashset,
            },
        );
        if let Some(kad) = self.inner.kad().await {
            let nodes = kad.publish_source(kad_key(hash), size).await;
            tracing::info!(nodes, name = %name, "ed2k: shared, and published to Kad");
        }
        Ok(hash)
    }

    async fn get(&self, id: u64) -> Option<Arc<Download>> {
        self.inner.downloads.lock().await.get(&id).cloned()
    }

    /// Stop every download (the app is closing): their parts stay on disk.
    pub async fn stop(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        let map = self.inner.downloads.lock().await;
        for download in map.values() {
            download.set_state(State::Paused);
        }
    }
}

impl Inner {
    /// Bind the parts server and start answering peers.
    async fn start_serving(&self) -> Result<()> {
        let listener = bind_tcp(self.settings.tcp_port).await?;
        let port = listener
            .local_addr()
            .context("the parts server's port")?
            .port();
        self.tcp_port.store(port, Ordering::Relaxed);
        if port != self.settings.tcp_port && self.settings.tcp_port != 0 {
            tracing::info!(
                asked = self.settings.tcp_port,
                got = port,
                "ed2k: its port was taken, serving parts on another"
            );
        }
        let context = Arc::new(UploadContext {
            slots: Arc::new(Semaphore::new(self.settings.upload_slots.max(1))),
            queue: UploadQueue::default(),
            temp_dir: self.settings.temp.clone(),
            tcp_port: port,
            kad_udp_port: self.settings.udp_port,
            user_hash: self.settings.user_hash,
            nick: self.settings.nick.clone(),
            downloads: Arc::clone(&self.served),
            inbound_connections: Arc::new(AtomicU64::new(0)),
            last_inbound_at: Arc::new(AtomicU64::new(0)),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            chunks_served: Arc::new(AtomicU64::new(0)),
            upload_limiter: None,
            upload_observer: None,
        });
        tokio::spawn(rucio_emule::transfer::serve_incoming(
            listener,
            Arc::clone(&context),
        ));
        let _ = self.uploads.set(context);
        Ok(())
    }

    /// The Kad table, made and bootstrapped the first time it is asked for.
    async fn kad(&self) -> Option<&KadHandle> {
        let handle = self
            .kad
            .get_or_try_init(|| async {
                let socket = Arc::new(bind_udp(self.settings.udp_port).await?);
                let kad = rucio_emule::kad::task::spawn(
                    socket,
                    KadId::random(),
                    KadTaskConfig {
                        tcp_port: self.tcp_port.load(Ordering::Relaxed),
                        user_hash: self.settings.user_hash,
                        ..Default::default()
                    },
                );
                let seeds = nodes::contacts(&self.settings).await;
                let contacts = kad.bootstrap(seeds).await;
                tracing::info!(contacts, "ed2k: Kad table bootstrapped");
                if contacts == 0 {
                    anyhow::bail!("no contact answered: Kad could not be joined");
                }
                Ok::<KadHandle, anyhow::Error>(kad)
            })
            .await;
        match handle {
            Ok(kad) => Some(kad),
            Err(e) => {
                tracing::warn!(error = %e, "ed2k: Kad is not available");
                None
            }
        }
    }
}

/// Bind the port asked for, or any free one.
async fn bind_tcp(port: u16) -> Result<TcpListener> {
    match TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await {
        Ok(listener) => Ok(listener),
        Err(e) if port != 0 => {
            tracing::info!(port, error = %e, "ed2k: falling back to a free port");
            Ok(TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?)
        }
        Err(e) => Err(e.into()),
    }
}

/// Kad's own port. A taken port (another eMule, a second MDM) is not the end of Kad: a free one
/// works as well, since the port is told to the peers we talk to.
async fn bind_udp(port: u16) -> Result<UdpSocket> {
    match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await {
        Ok(socket) => Ok(socket),
        Err(e) if port != 0 => {
            tracing::info!(port, error = %e, "ed2k: Kad's port was taken, asking on a free one");
            Ok(UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?)
        }
        Err(e) => Err(e.into()),
    }
}

/// Keep one download going: refresh its sources, keep its workers busy, finish it.
async fn supervise(inner: Arc<Inner>, download: Arc<Download>) {
    if download.running.swap(true, Ordering::AcqRel) {
        return;
    }
    download.set_state(State::Running);
    let settings = inner.settings.clone();
    let result = run_until_done(&inner, &download, &settings).await;
    if let Err(e) = result {
        tracing::warn!(download = download.id, error = %e, "ed2k: download failed");
        download.set_error(Some(e.to_string()));
        download.set_state(State::Failed);
    }
    download.running.store(false, Ordering::Release);
}

async fn run_until_done(
    inner: &Arc<Inner>,
    download: &Arc<Download>,
    settings: &Settings,
) -> Result<()> {
    loop {
        if download.paused() || inner.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        if download.parts.all_done() {
            break;
        }

        let sources = match &settings.sources {
            Some(query) => query(download.link.hash, download.link.size).await,
            None => match inner.kad().await {
                Some(kad) => {
                    kad.search_sources(kad_key(download.link.hash), download.link.size)
                        .await
                }
                None => Vec::new(),
            },
        };
        download.set_sources(sources);
        if download.paused() {
            return Ok(());
        }

        if download.sources() == 0 {
            tracing::debug!(download = download.id, "ed2k: no sources yet, asking again");
            sleep_unless_paused(download, Duration::from_secs(15)).await;
            continue;
        }

        // A `.part` from an earlier run whose progress was lost: the hashes come first, so the
        // parts already on disk are known (and not fetched again) before any worker claims one.
        let _ = download
            .prime(settings, inner.tcp_port.load(Ordering::Relaxed))
            .await;

        let workers = settings
            .max_peers
            .max(1)
            .min(download.parts.remaining().max(1));
        let mut set = JoinSet::new();
        for _ in 0..workers {
            let download = Arc::clone(download);
            let settings = settings.clone();
            let port = inner.tcp_port.load(Ordering::Relaxed);
            set.spawn(async move { download.work(&settings, port).await });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok(Err(e)) = joined {
                tracing::debug!(download = download.id, error = %e, "ed2k: a worker stopped");
            }
        }

        if download.paused() || inner.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        if download.parts.all_done() {
            break;
        }
        sleep_unless_paused(download, Duration::from_secs(5)).await;
    }

    let (path, hashset) = download.finalize().await?;
    tracing::info!(download = download.id, path = %path.display(), "ed2k: finished");
    if settings.seed {
        let mut served = inner.served.write().await;
        if let Some(info) = served.get_mut(download.link.hash.as_bytes()) {
            info.path = path;
            info.complete = true;
            info.num_slices = download.parts.count();
            info.hashset = hashset;
        }
    } else {
        inner
            .served
            .write()
            .await
            .remove(download.link.hash.as_bytes());
    }
    // A finished download keeps its row: the app shows it as done, and (seeding) it serves it.
    Ok(())
}

/// Sleep, but wake at once when the download is paused.
async fn sleep_unless_paused(download: &Arc<Download>, time: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(time) => {}
        _ = download.changed.notified() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key a file's sources are published and searched under, as the Kad task of
    /// `rucio-emule` finally sees it (it reverses each word of what it is handed): the hash of the
    /// link, byte for byte — the form eMule stores a file's sources under.
    #[test]
    fn the_source_key_is_the_hash_as_written() {
        let hash = crate::Ed2kHash::from_hex("e3964138f86625da75103e53aa0af1a1").expect("hash");
        let mut reversed = *kad_key(hash).as_bytes();
        for word in reversed.chunks_mut(4) {
            word.reverse();
        }
        assert_eq!(reversed, *hash.as_bytes());
    }

    /// The reversal is its own undo, so nothing else that holds a hash is affected.
    #[test]
    fn the_key_reversal_undoes_itself() {
        let hash = crate::Ed2kHash::from_hex("0123456789abcdef0123456789abcdef").expect("hash");
        assert_eq!(kad_key(kad_key(hash)), hash);
        assert_ne!(kad_key(hash), hash);
    }
}

#[cfg(test)]
mod realnet {
    use super::*;

    /// One session shares a file, a second one joins Kad cold, and the search finds the first:
    /// the publish, the key it is stored under, and the search that reads it back. Needs the Kad
    /// network; run by hand:
    /// `cargo test -p mdm-ed2k --lib -- --ignored --nocapture a_published_file`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs the Kad network"]
    async fn a_published_file_is_found_from_a_cold_start() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();
        let dir = std::env::temp_dir().join("mdm-ed2k-cold");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let file = dir.join("published.bin");
        let bytes: Vec<u8> = (0..12_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&file, &bytes).expect("write");

        let mut seed_settings = Settings::load(dir.join("seed-state"));
        seed_settings.temp = dir.join("seed-parts");
        seed_settings.seed = true;
        let seed = Ed2k::start(seed_settings).await.expect("seed session");
        join(&seed).await;
        let hash = seed.share(file).await.expect("share");
        println!("shared {hash} ({} B)", bytes.len());

        let mut get_settings = Settings::load(dir.join("get-state"));
        get_settings.temp = dir.join("get-parts");
        get_settings.seed = false;
        let get = Ed2k::start(get_settings).await.expect("get session");
        join(&get).await;
        let sources = get.sources(hash, bytes.len() as u64).await;
        println!("cold search: {} source(s)", sources.len());
        assert!(!sources.is_empty(), "the published source was not found");
        get.stop().await;
        seed.stop().await;
    }

    /// The parts server and the peer client over loopback: one session shares a file, the other
    /// is told where it is and downloads it — hash set, part requests, the finished file. Needs
    /// two ports of its own, nothing else; run by hand:
    /// `cargo test -p mdm-ed2k --lib -- --ignored --nocapture a_file_comes_back`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs ports of its own, and nothing else"]
    async fn a_file_comes_back_over_loopback() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();
        let dir = std::env::temp_dir().join("mdm-ed2k-loop");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let file = dir.join("published.bin");
        let bytes: Vec<u8> = (0..12_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&file, &bytes).expect("write");

        let mut seed_settings = Settings::load(dir.join("seed-state"));
        seed_settings.temp = dir.join("seed-parts");
        seed_settings.seed = true;
        let seed_user_hash = seed_settings.user_hash;
        let seed = Ed2k::start(seed_settings).await.expect("seed session");
        let hash = seed.share(file).await.expect("share");
        let seed_port = seed.tcp_port();
        println!("shared {hash} on port {seed_port}");

        let mut get_settings = Settings::load(dir.join("get-state"));
        get_settings.temp = dir.join("get-parts");
        get_settings.seed = false;
        get_settings.sources = Some(std::sync::Arc::new(
            move |_hash: crate::Ed2kHash, _size: u64| {
                Box::pin(async move {
                    vec![rucio_emule::kad::search::KadSource {
                        ip: std::net::Ipv4Addr::LOCALHOST,
                        tcp_port: seed_port,
                        udp_port: 0,
                        user_hash: seed_user_hash,
                    }]
                })
            },
        ));
        let get = Ed2k::start(get_settings).await.expect("get session");
        let id = get
            .add(Spec {
                link: crate::Ed2kLink {
                    name: "published.bin".to_owned(),
                    size: bytes.len() as u64,
                    hash,
                },
                dir: dir.join("out"),
            })
            .await
            .expect("add");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let st = get.stats_of(id).await.expect("stats");
            if st.done_parts == st.parts {
                break;
            }
        }
        let got = std::fs::read(dir.join("out").join("published.bin")).expect("downloaded");
        assert_eq!(got, bytes, "the file came back whole");
        println!("downloaded and checked {} B", got.len());
        get.stop().await;
        seed.stop().await;
    }

    async fn join(engine: &Ed2k) {
        let mut contacts = 0;
        for _ in 0..90 {
            contacts = engine.kad_contacts().await;
            if contacts > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        println!("kad contacts: {contacts}");
        assert!(contacts > 0, "Kad could not be joined");
    }
}
