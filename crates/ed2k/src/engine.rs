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
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
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
        self.inner.downloads.lock().await.insert(id, download.clone());
        self.inner
            .served
            .write()
            .await
            .insert(
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
                name: download.link.name.clone(),
                size: download.link.size,
                downloaded: download.downloaded(),
                parts: download.parts.count(),
                done_parts: download.parts.done_count(),
                sources: download.sources(),
                peers: download.peers(),
                state: download.state(),
                error: download.error(),
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
    pub async fn sources(&self, hash: crate::Ed2kHash, size: u64) -> Vec<rucio_emule::kad::search::KadSource> {
        match self.inner.kad().await {
            Some(kad) => kad.search_sources(hash, size).await,
            None => Vec::new(),
        }
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
                let socket = Arc::new(
                    UdpSocket::bind((Ipv4Addr::UNSPECIFIED, self.settings.udp_port)).await?,
                );
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
                Some(kad) => kad.search_sources(download.link.hash, download.link.size).await,
                None => Vec::new(),
            },
        };
        download.set_sources(sources);
        if download.paused() {
            return Ok(());
        }

        if download.sources() == 0 {
            tracing::debug!(
                download = download.id,
                "ed2k: no sources yet, asking again"
            );
            sleep_unless_paused(download, Duration::from_secs(15)).await;
            continue;
        }

        // A `.part` from an earlier run whose progress was lost: the hashes come first, so the
        // parts already on disk are known (and not fetched again) before any worker claims one.
        let _ = download.prime(settings, inner.tcp_port.load(Ordering::Relaxed)).await;

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

