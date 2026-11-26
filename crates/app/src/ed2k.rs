//! eDonkey2000 (ed2k) downloads (M16): the app's side of `mdm-ed2k`.
//!
//! An ed2k link names a file by its hash, not by a place: the peers that hold it are found in
//! Kad, and its parts are asked of them with the eMule peer protocol. That work is `mdm-ed2k`'s;
//! what this module does is put it in the list: an address the app can hold ([`address`]), a
//! download the session runs ([`Run`], a [`crate::model::Job`]), and the numbers the window
//! shows, read without waiting on the network ([`Host`]).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use mdm_ed2k::{Ed2kLink, State as Ed2kState};
use mdm_engine::download::State;

/// The ed2k part of a download: the file's hash (32 hex digits), which is also what the session
/// knows it by. The name, the size and the folder are the row's own (`file_name`, `size`,
/// `dest`), as for any file.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ed2kSpec {
    pub hash: String,
    /// Peers Kad says hold the file, and peers being asked right now: live only, never saved.
    #[serde(skip)]
    pub sources: usize,
    #[serde(skip)]
    pub peers: usize,
}

/// The address the app keeps for an ed2k link: `ed2k://file/<hash>/<size>?name=<name>`.
///
/// An `ed2k://|file|…|/` link is no URL (`|` is a forbidden character in one), and the list, its
/// filters and the windows all hold a `url::Url`. This form carries everything the link says, is
/// the same address every time the same file is added (so the duplicate check sees one file),
/// and reads back into the link ([`link_of`]); the link a person copies is [`link_text`]'s.
pub fn address(link: &Ed2kLink) -> url::Url {
    let mut url = url::Url::parse(&format!("ed2k://file/{}", link.hash.to_hex()))
        .expect("an ed2k address is one MDM makes");
    url.path_segments_mut()
        .expect("the address has a path")
        .push(&link.size.to_string());
    url.query_pairs_mut().append_pair("name", &link.name);
    url
}

/// The link [`address`] was made from, if `url` is one of its addresses.
pub fn link_of(url: &url::Url) -> Option<Ed2kLink> {
    if url.scheme() != "ed2k" || url.host_str() != Some("file") {
        return None;
    }
    let mut segments = url.path_segments()?;
    let hash = mdm_ed2k::Ed2kHash::from_hex(segments.next()?).ok()?;
    let size: u64 = segments.next()?.parse().ok()?;
    let name = url
        .query_pairs()
        .find(|(key, _)| key == "name")
        .map(|(_, value)| value.into_owned())?;
    Some(Ed2kLink { name, size, hash })
}

/// Whether `url` is an ed2k link's address ([`address`]).
pub fn is_address(url: &url::Url) -> bool {
    url.scheme() == "ed2k" && link_of(url).is_some()
}

/// The link as a person writes it (`ed2k://|file|name|size|hash|/`), to show and to copy, from
/// the row's own name, size and hash.
pub fn link_text(name: &str, size: u64, hash: &str) -> String {
    format!("ed2k://|file|{name}|{size}|{hash}|/")
}

/// Where the session's own files are: the parts of what it is downloading, the Kad list, and the
/// identity eMule's credits are kept against.
pub fn state_dir() -> Option<PathBuf> {
    mdm_ipc::data_dir().map(|d| d.join("ed2k"))
}

fn settings() -> mdm_ed2k::Settings {
    let dir = state_dir().unwrap_or_else(|| std::env::temp_dir().join("mdm-ed2k"));
    let mut settings = mdm_ed2k::Settings::load(dir.join("state"));
    // The parts live beside the session's other files, never in a download's folder: a folder
    // the user moves, renames or watches stays theirs.
    settings.temp = dir.join("parts");
    settings
}

/// What the session last said about one download (the numbers the window shows).
#[derive(Clone, Debug)]
pub struct Seen {
    pub downloaded: u64,
    pub size: u64,
    pub sources: usize,
    pub peers: usize,
    pub state: Ed2kState,
    pub error: Option<String>,
    /// Where the finished file went.
    pub path: Option<PathBuf>,
}

/// What the session is asked to do, in order (the window never waits on the network).
enum Ask {
    /// Start, or go on with, the download of this link into this folder.
    Add(mdm_ed2k::Spec),
    Pause(String),
    Forget {
        hash: String,
        files: bool,
    },
}

/// The ed2k session, started the first time a download needs it, as the torrent one is.
///
/// One task owns it: it does what the window asks (add, pause, resume, forget) and writes down
/// what the session says a few times a second. The window reads those numbers from
/// [`Host::seen`], so it never waits on Kad, a peer, or a disk.
#[derive(Clone, Default)]
pub struct Host {
    inner: Arc<HostInner>,
}

#[derive(Default)]
struct HostInner {
    seen: Mutex<HashMap<String, Seen>>,
    todo: Mutex<Vec<Ask>>,
    ids: Mutex<HashMap<String, u64>>,
    /// Why the session couldn't start, once it couldn't.
    failed: Mutex<Option<String>>,
    started: AtomicBool,
}

impl Host {
    pub fn new() -> Self {
        Self::default()
    }

    /// What the session last said about `hash`.
    pub fn seen(&self, hash: &str) -> Option<Seen> {
        self.inner.seen.lock().ok()?.get(hash).cloned()
    }

    /// Why the session couldn't start (a port it may not use, a disk that refused).
    pub fn failed(&self) -> Option<String> {
        self.inner.failed.lock().ok()?.clone()
    }

    /// Start, or go on with, `spec`, in the background.
    pub fn request(&self, rt: &Handle, spec: mdm_ed2k::Spec) {
        self.ask(Ask::Add(spec));
        self.ensure(rt);
    }

    /// Stop a download; its parts stay for the next start.
    pub fn pause(&self, rt: &Handle, hash: &str) {
        self.ask(Ask::Pause(hash.to_owned()));
        self.ensure(rt);
    }

    /// Take `hash` off the session; with `files`, what it downloaded so far goes with it.
    pub fn forget(&self, rt: &Handle, hash: &str, files: bool) {
        self.ask(Ask::Forget {
            hash: hash.to_owned(),
            files,
        });
        self.ensure(rt);
    }

    fn ask(&self, ask: Ask) {
        if let Ok(mut todo) = self.inner.todo.lock() {
            todo.push(ask);
        }
    }

    /// Start the session's task, once.
    fn ensure(&self, rt: &Handle) {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let host = self.clone();
        rt.spawn(async move { drive(host).await });
    }

    fn take(&self) -> Vec<Ask> {
        self.inner
            .todo
            .lock()
            .map(|mut todo| std::mem::take(&mut *todo))
            .unwrap_or_default()
    }

    fn id(&self, hash: &str) -> Option<u64> {
        self.inner.ids.lock().ok()?.get(hash).copied()
    }

    fn put_id(&self, hash: String, id: u64) {
        if let Ok(mut ids) = self.inner.ids.lock() {
            ids.insert(hash, id);
        }
    }

    fn forget_id(&self, hash: &str) {
        if let Ok(mut ids) = self.inner.ids.lock() {
            ids.remove(hash);
        }
    }

    fn write(&self, stats: Vec<mdm_ed2k::Stats>) {
        let Ok(mut seen) = self.inner.seen.lock() else {
            return;
        };
        seen.clear();
        for st in stats {
            seen.insert(
                st.hash.to_hex(),
                Seen {
                    downloaded: st.downloaded,
                    size: st.size,
                    sources: st.sources,
                    peers: st.peers,
                    state: st.state,
                    error: st.error,
                    path: st.path,
                },
            );
        }
    }
}

/// The session's task: what the window asked, then the numbers.
async fn drive(host: Host) {
    let session = match mdm_ed2k::Ed2k::start(settings()).await {
        Ok(session) => session,
        Err(e) => {
            tracing::warn!("ed2k: the session could not start: {e}");
            if let Ok(mut failed) = host.inner.failed.lock() {
                *failed = Some(e.to_string());
            }
            return;
        }
    };
    loop {
        for ask in host.take() {
            match ask {
                Ask::Add(spec) => {
                    let hash = spec.link.hash.to_hex();
                    if let Some(id) = host.id(&hash) {
                        session.resume(id).await;
                    } else {
                        match session.add(spec).await {
                            Ok(id) => host.put_id(hash, id),
                            Err(e) => tracing::warn!("ed2k: {hash}: {e}"),
                        }
                    }
                }
                Ask::Pause(hash) => {
                    if let Some(id) = host.id(&hash) {
                        session.pause(id).await;
                    }
                }
                Ask::Forget { hash, files } => {
                    if let Some(id) = host.id(&hash) {
                        session.remove(id, !files).await;
                        host.forget_id(&hash);
                    }
                }
            }
        }
        host.write(session.stats().await);
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// One download while it is in the list: what the row's [`crate::model::Job`] holds, giving the
/// windows its progress ([`Run::snapshot`]) and stopping it where the person says.
pub struct Run {
    host: Host,
    rt: Handle,
    url: url::Url,
    link: Ed2kLink,
    /// Stopped from the list: the session is told to pause, and the snapshot says so at once.
    stopped: Mutex<bool>,
}

impl Run {
    /// Start the session's download of `link` into `dir` (or go on with its parts).
    pub fn start(host: &Host, rt: &Handle, url: url::Url, link: Ed2kLink, dir: PathBuf) -> Run {
        host.request(
            rt,
            mdm_ed2k::Spec {
                link: link.clone(),
                dir,
            },
        );
        Run {
            host: host.clone(),
            rt: rt.clone(),
            url,
            link,
            stopped: Mutex::new(false),
        }
    }

    pub fn hash(&self) -> String {
        self.link.hash.to_hex()
    }

    /// Stop it: the session pauses (its parts stay), and the row shows Stopped.
    pub fn cancel(&self) {
        if let Ok(mut stopped) = self.stopped.lock() {
            *stopped = true;
        }
        self.host.pause(&self.rt, &self.hash());
    }

    pub fn snapshot(&self) -> mdm_engine::Snapshot {
        let mut s = mdm_engine::Snapshot {
            state: State::Probing,
            url: self.url.clone(),
            file: None,
            size: Some(self.link.size),
            downloaded: 0,
            resumable: Some(true),
            segments: Vec::new(),
            connections: Vec::new(),
            error: None,
            failure: None,
            events: Vec::new(),
            server: None,
            at_service: None,
            finishing: None,
            kept: None,
        };
        let stopped = self.stopped.lock().map(|s| *s).unwrap_or(false);
        let Some(seen) = self.host.seen(&self.hash()) else {
            // Not in the session yet (it is starting), or no longer there.
            if let Some(why) = self.host.failed() {
                s.state = State::Failed;
                s.error = Some(why);
            } else if stopped {
                s.state = State::Cancelled;
            }
            return s;
        };
        s.downloaded = seen.downloaded;
        s.size = Some(seen.size);
        s.file = seen.path;
        s.state = match seen.state {
            Ed2kState::Running if stopped => State::Cancelled,
            Ed2kState::Running => State::Downloading,
            Ed2kState::Paused => State::Cancelled,
            Ed2kState::Finished => State::Done,
            Ed2kState::Failed => {
                s.error = seen.error;
                State::Failed
            }
        };
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(name: &str) -> Ed2kLink {
        Ed2kLink {
            name: name.to_owned(),
            size: 1_034_384_725,
            hash: mdm_ed2k::Ed2kHash::from_hex("e3964138f86625da75103e53aa0af1a1").expect("hash"),
        }
    }

    /// M16: the address the list holds is the link itself — name, size and hash — whatever the
    /// name holds (a `|`, a `/`, a `%`, Chinese, a `&`).
    #[test]
    fn an_address_carries_the_link_back() {
        for name in [
            "a.mp4",
            "【高清MP4电影www.domp4.cc】jtdyj.2024.HD1080p.gyzz.mp4",
            "a|b|c/d.txt",
            "100% done & more.bin",
            "x?y=z#f",
        ] {
            let url = address(&link(name));
            assert!(is_address(&url), "{url}");
            let back = link_of(&url).expect("the link back");
            assert_eq!(back.name, name, "{url}");
            assert_eq!(back.size, 1_034_384_725, "{url}");
            assert_eq!(
                back.hash.to_hex(),
                "e3964138f86625da75103e53aa0af1a1",
                "{url}"
            );
        }
    }

    #[test]
    fn only_an_ed2k_address_is_ed2k() {
        for other in [
            "https://example.org/a.zip",
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            "ed2k://file/not-a-hash/1?name=a",
            "ed2k://elsewhere/e3964138f86625da75103e53aa0af1a1/1?name=a",
        ] {
            let url = url::Url::parse(other).expect(other);
            assert!(!is_address(&url), "{other}");
            assert!(link_of(&url).is_none(), "{other}");
        }
    }

    /// M5: the window's numbers come from the session — taken in, running, stopped — and the
    /// window never waits on the network. Needs Kad, so it is run by hand:
    /// `cargo test -p mdm-app --lib -- --ignored ed2k`.
    #[test]
    #[ignore = "needs the Kad network"]
    fn the_session_gives_the_window_its_numbers() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let dir = tempfile::tempdir().expect("a folder");
        let host = Host::new();
        let l = Ed2kLink {
            name: "mdm-ed2k-integration-test.bin".to_owned(),
            size: 4096,
            hash: mdm_ed2k::Ed2kHash::from_hex("fedcba9876543210fedcba9876543210").expect("hash"),
        };
        let run = Run::start(
            &host,
            rt.handle(),
            address(&l),
            l.clone(),
            dir.path().to_path_buf(),
        );
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(120) && host.seen(&run.hash()).is_none() {
            std::thread::sleep(Duration::from_millis(200));
        }
        let seen = host.seen(&run.hash()).expect("the session takes it in");
        assert_eq!(
            seen.size, l.size,
            "the link's size, not the session's guess"
        );
        assert_eq!(seen.downloaded, 0, "no peer has it: nothing downloaded");
        let s = run.snapshot();
        assert_eq!(s.size, Some(l.size));
        assert_eq!(s.url, address(&l));
        assert!(
            matches!(s.state, State::Downloading | State::Probing),
            "{:?}",
            s.state
        );
        // Stopped from the list: the row says so at once, and the session pauses it.
        run.cancel();
        assert_eq!(run.snapshot().state, State::Cancelled);
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(
            host.seen(&run.hash()).expect("still in the session").state,
            Ed2kState::Paused
        );
    }

    #[test]
    fn the_link_text_is_the_one_a_person_writes() {
        assert_eq!(
            link_text("a.mp4", 7, "e3964138f86625da75103e53aa0af1a1"),
            "ed2k://|file|a.mp4|7|e3964138f86625da75103e53aa0af1a1|/"
        );
    }
}
