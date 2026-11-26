//! One download: the peers that hold its file, the parts they are asked for, and the file they
//! add up to.
//!
//! A peer is asked for one part at a time with the eMule peer protocol, and the part is checked
//! against the hash set the file's hash is built from before it counts. Several peers fetch
//! different parts at once ([`Download::work`]); a peer that fails one costs that one part, and
//! is left out for a minute.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use rucio_emule::kad::search::KadSource;
use rucio_emule::transfer::{DownloadEvent, DownloadOptions, Session};
use tokio::sync::Notify;

use crate::parts::Parts;
use crate::{Ed2kLink, Settings};

/// How a download is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Running,
    /// Stopped by the person; its parts stay for the next start.
    Paused,
    Finished,
    Failed,
}

/// A peer left out after a failure.
const FAILURE_COOLDOWN: Duration = Duration::from_secs(60);

/// One download's state, shared by the tasks working on it and the app reading its progress.
pub struct Download {
    pub id: u64,
    pub link: Ed2kLink,
    /// Where the finished file goes.
    pub dir: PathBuf,
    /// Where its bytes are put together meanwhile.
    pub part_path: PathBuf,
    /// Its part progress.
    pub met_path: PathBuf,
    pub parts: Arc<Parts>,
    pub(crate) peers: AtomicUsize,
    pub(crate) running: AtomicBool,
    /// Its parts on disk but its progress file lost: found again by hashing once a peer's hash
    /// set is known.
    pub(crate) rebuild: AtomicBool,
    /// The app waits when told (a part is done, the file finished, a failure).
    pub(crate) changed: Notify,
    state: Mutex<State>,
    error: Mutex<Option<String>>,
    sources: Mutex<Vec<KadSource>>,
    failures: Mutex<HashMap<Ipv4Addr, Instant>>,
    turn: AtomicUsize,
}

impl Download {
    pub fn new(
        id: u64,
        link: Ed2kLink,
        dir: PathBuf,
        part_path: PathBuf,
        met_path: PathBuf,
        done: Vec<bool>,
        rebuild: bool,
    ) -> Self {
        let parts = Arc::new(Parts::new(link.size, link.hash, done));
        Self {
            id,
            link,
            dir,
            part_path,
            met_path,
            parts,
            peers: AtomicUsize::new(0),
            running: AtomicBool::new(false),
            rebuild: AtomicBool::new(rebuild),
            changed: Notify::new(),
            state: Mutex::new(State::Running),
            error: Mutex::new(None),
            sources: Mutex::new(Vec::new()),
            failures: Mutex::new(HashMap::new()),
            turn: AtomicUsize::new(0),
        }
    }

    pub fn state(&self) -> State {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn set_state(&self, state: State) {
        if let Ok(mut s) = self.state.lock() {
            *s = state;
        }
        self.changed.notify_waiters();
    }

    pub fn paused(&self) -> bool {
        self.state() == State::Paused
    }

    pub(crate) fn set_error(&self, error: Option<String>) {
        if let Ok(mut e) = self.error.lock() {
            *e = error;
        }
    }

    pub fn error(&self) -> Option<String> {
        self.error.lock().ok().and_then(|e| e.clone())
    }

    pub(crate) fn set_sources(&self, sources: Vec<KadSource>) {
        if let Ok(mut s) = self.sources.lock() {
            *s = sources;
        }
        self.changed.notify_waiters();
    }

    pub fn sources(&self) -> usize {
        self.sources.lock().map(|s| s.len()).unwrap_or(0)
    }

    pub fn peers(&self) -> usize {
        self.peers.load(Ordering::Relaxed)
    }

    /// The bytes here: the parts done, each as long as it really is.
    pub fn downloaded(&self) -> u64 {
        let size = self.link.size;
        let mut got = 0;
        for i in 0..self.parts.count() {
            if self.parts.is_done(i) {
                let (start, end) = self.parts.range(i);
                got += end.min(size) - start.min(size);
            }
        }
        got
    }

    fn note_failure(&self, peer: Ipv4Addr) {
        if let Ok(mut f) = self.failures.lock() {
            f.insert(peer, Instant::now());
            // Keep that map from growing for a long session.
            if f.len() > 256 {
                let now = Instant::now();
                f.retain(|_, when| now.duration_since(*when) < FAILURE_COOLDOWN);
            }
        }
    }

    /// The next peer to ask: the sources in turn, skipping one that just failed.
    pub(crate) fn pick_peer(&self) -> Option<(SocketAddrV4, KadSource)> {
        let sources = self.sources.lock().ok()?.clone();
        if sources.is_empty() {
            return None;
        }
        let now = Instant::now();
        let cooling = |ip: Ipv4Addr| {
            self.failures
                .lock()
                .ok()
                .and_then(|f| f.get(&ip).copied())
                .is_some_and(|when| now.duration_since(when) < FAILURE_COOLDOWN)
        };
        for _ in 0..sources.len() {
            let i = self.turn.fetch_add(1, Ordering::Relaxed) % sources.len();
            let source = sources[i].clone();
            if cooling(source.ip) {
                continue;
            }
            let addr = SocketAddrV4::new(source.ip, source.tcp_port);
            return Some((addr, source));
        }
        None
    }

    /// Fetch parts until none is left, the download is paused, or no peer is left to ask.
    /// `our_tcp_port` is the port this session serves parts on (0 when it serves none).
    pub async fn work(self: &Arc<Self>, settings: &Settings, our_tcp_port: u16) -> Result<()> {
        let mut file = open_part(&self.part_path, self.link.size).await?;
        loop {
            if self.paused() {
                return Ok(());
            }
            let Some(index) = self.parts.claim() else {
                return Ok(());
            };
            let (start, end) = self.parts.range(index);
            let Some((addr, source)) = self.pick_peer() else {
                self.parts.release(index);
                return Ok(());
            };
            self.peers.fetch_add(1, Ordering::Relaxed);
            let result = self
                .fetch_part(settings, our_tcp_port, addr, &source, index, start, end, &mut file)
                .await;
            self.peers.fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(()) => {
                    self.parts.finish(index);
                    self.save_progress();
                    tracing::debug!(
                        download = self.id,
                        part = index,
                        "ed2k: part done ({} of {})",
                        self.parts.done_count(),
                        self.parts.count()
                    );
                    self.changed.notify_waiters();
                }
                Err(e) => {
                    self.parts.release(index);
                    self.note_failure(*addr.ip());
                    tracing::debug!(download = self.id, peer = %addr, error = %e, "ed2k: part failed");
                }
            }
        }
    }

    fn options(&self, settings: &Settings, our_tcp_port: u16, source: &KadSource) -> DownloadOptions {
        DownloadOptions {
            file_size: self.link.size,
            hash: self.link.hash,
            peer_hash: Some(source.user_hash),
            our_tcp_port,
            our_kad_udp_port: settings.udp_port,
            our_user_hash: settings.user_hash,
            our_nick: settings.nick.clone(),
            timeout: Duration::from_secs(60 * 60),
            op_timeout: Duration::from_secs(30),
            max_queue_waits: 10,
            download_id: self.id as i64,
            ..Default::default()
        }
    }

    /// Ask one peer for the part hashes and find what the `.part` on disk already holds, before
    /// any part is claimed: a resumed download then never fetches a part it already has, and
    /// never has to throw away bytes fetched before the hashes were known.
    pub(crate) async fn prime(self: &Arc<Self>, settings: &Settings, our_tcp_port: u16) -> bool {
        if !self.rebuild.load(Ordering::Acquire) {
            return true;
        }
        let Some((addr, source)) = self.pick_peer() else {
            return false;
        };
        let options = self.options(settings, our_tcp_port, &source);
        let mut hashset: Option<Vec<[u8; 16]>> = None;
        let mut on_event = |event: DownloadEvent| {
            if let DownloadEvent::Hashset(hashes) = event {
                hashset = Some(hashes);
            }
        };
        match Session::connect(addr, &options, &mut on_event).await {
            Ok(_session) => {
                if let Some(hashes) = hashset
                    && self.parts.offer_hashes(hashes)
                {
                    self.rebuild_from_disk().await;
                }
                true
            }
            Err(e) => {
                self.note_failure(*addr.ip());
                tracing::debug!(download = self.id, peer = %addr, error = %e, "ed2k: prime failed");
                false
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_part(
        &self,
        settings: &Settings,
        our_tcp_port: u16,
        addr: SocketAddrV4,
        source: &KadSource,
        index: usize,
        start: u64,
        end: u64,
        file: &mut tokio::fs::File,
    ) -> Result<()> {
        let options = self.options(settings, our_tcp_port, source);

        // The peer hands out the per-part hash set as it accepts the file; it is what every
        // later part is checked against, so it is taken (and checked) before asking for bytes.
        let mut hashset: Option<Vec<[u8; 16]>> = None;
        let mut on_event = |event: DownloadEvent| {
            if let DownloadEvent::Hashset(hashes) = event {
                hashset = Some(hashes);
            }
        };
        let mut session = Session::connect(addr, &options, &mut on_event).await?;
        if let Some(hashes) = hashset
            && self.parts.offer_hashes(hashes)
        {
            self.rebuild_from_disk().await;
            // The part claimed before the hash set was known may be one this run already has
            // on disk: then it is not asked for again.
            if self.parts.is_done(index) {
                return Ok(());
            }
        }

        let expected = self.parts.expected(index);
        let mut no_events = |_event: DownloadEvent| {};
        session
            .download_range(
                start,
                end,
                expected,
                file,
                &mut no_events,
                || self.paused(),
            )
            .await?;
        Ok(())
    }

    /// A `.part` left by an earlier run whose progress was lost: every part it holds is checked
    /// against the hash set and counted, so a resumed download never fetches them again.
    async fn rebuild_from_disk(&self) {
        if !self.rebuild.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(hashes) = self.parts.hashes() else {
            return;
        };
        let path = self.part_path.clone();
        let size = self.link.size;
        let count = self.parts.count();
        let done = tokio::task::spawn_blocking(move || {
            rucio_emule::ed2k::verify_part_against_hashset(&path, &hashes, count, size)
        })
        .await
        .unwrap_or_else(|_| vec![false; count]);
        for (i, ok) in done.into_iter().enumerate() {
            if ok {
                self.parts.finish(i);
            }
        }
        self.save_progress();
        self.changed.notify_waiters();
    }

    /// Write which parts are here, as eMule writes it (`progress.rs`): what the next run
    /// resumes from, and what this one serves from.
    pub(crate) fn save_progress(&self) {
        let snapshot = self.parts.snapshot();
        let path = self.met_path.clone();
        // A file of a few bytes; not worth a waiting thread of its own.
        rucio_emule::progress::save_progress(&path, &snapshot);
    }

    /// Put the file together: every part checked against the link's hash, then named what the
    /// link says and put in its folder. `Ok` leaves it finished; `Err` leaves it failed.
    pub(crate) async fn finalize(self: &Arc<Self>) -> Result<(PathBuf, Vec<u8>)> {
        let part = self.part_path.clone();
        let size = self.link.size;
        let expected = self.link.hash;
        let (hash, parts) = tokio::task::spawn_blocking(move || {
            std::fs::File::open(&part).and_then(rucio_emule::ed2k::hash_reader_full)
        })
        .await
        .map_err(|e| anyhow::anyhow!("reading the parts: {e}"))??;
        if hash != expected {
            anyhow::bail!(
                "the file is not the one the link names (its hash is {hash}, not {expected})"
            );
        }

        let name = crate::safe_name(&self.link.name);
        let mut target = self.dir.join(&name);
        let mut n = 1;
        while target.exists() {
            let stem = std::path::Path::new(&name)
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| name.clone());
            let ext = std::path::Path::new(&name)
                .extension()
                .map(|e| format!(".{}", e.to_string_lossy()))
                .unwrap_or_default();
            target = self.dir.join(format!("{stem} ({n}){ext}"));
            n += 1;
            if n > 1000 {
                anyhow::bail!("no free name for {}", self.link.name);
            }
        }
        if let Err(e) = std::fs::create_dir_all(&self.dir) {
            anyhow::bail!("making {}: {e}", self.dir.display());
        }
        std::fs::rename(&self.part_path, &target)
            .map_err(|e| anyhow::anyhow!("moving the file into place: {e}"))?;
        let _ = std::fs::remove_file(&self.met_path);

        // The hash set a finished file is served with: eMule peers ask for it to check their
        // own parts, and their own `.part.met` is built from it.
        let hashset = rucio_emule::ed2k::finalize_hashset(&parts, size, &expected);
        self.set_state(State::Finished);
        self.changed.notify_waiters();
        Ok((target, hashset))
    }

    /// The file's part count, for what serves it.
    pub fn num_parts(&self) -> usize {
        self.parts.count()
    }
}

/// The `.part` file of a download: made (and sized) if this is the first run, opened otherwise.
pub(crate) async fn open_part(path: &PathBuf, size: u64) -> Result<tokio::fs::File> {
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await.ok();
    }
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(path)
        .await?;
    // A sparse file: its length is the download's, its holes are the parts still missing (a
    // part is only counted once its bytes hash right, so a hole can't pass for data).
    if file.metadata().await?.len() != size {
        file.set_len(size).await?;
    }
    Ok(file)
}


