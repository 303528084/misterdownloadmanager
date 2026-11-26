//! The bootstrap list that puts this installation on the Kad network (`nodes.dat`).
//!
//! Kad finds peers from peers. The first few have to be known, so a public list of contacts is
//! fetched once into the session's folder and used for weeks, then fetched again
//! ([`Settings::nodes_ttl`]). What the table learns meanwhile is written back on the way out, so
//! the next run starts from a list this installation itself confirmed.

use std::time::{Duration, SystemTime};

use rucio_emule::kad::packet::Contact;
use rucio_emule::kad::routing::{parse_nodes_dat, write_nodes_dat};
use rucio_emule::kad::task::KadHandle;

use crate::Settings;

/// The contacts to bootstrap from: the saved list while it is young enough, a freshly fetched
/// one after that (or the stale one, or none at all, when the fetch fails — a download still
/// works from any contact left, and fails with "no sources" when there is none).
pub async fn contacts(settings: &Settings) -> Vec<Contact> {
    let path = settings.nodes_path();
    let saved = std::fs::read(&path).ok();
    let young = std::fs::metadata(&path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|when| SystemTime::now().duration_since(when).ok())
        .is_some_and(|age| age < settings.nodes_ttl);

    if young
        && let Some(bytes) = &saved
        && let Ok(list) = parse_nodes_dat(bytes)
    {
        return list;
    }

    if let Some(bytes) = fetch(&settings.nodes_dat_url, settings.nodes_ttl).await {
        if let Err(e) = std::fs::create_dir_all(&settings.dir) {
            tracing::warn!(error = %e, "ed2k: cannot make the folder for nodes.dat");
        }
        if let Err(e) = std::fs::write(&path, &bytes) {
            tracing::warn!(error = %e, "ed2k: cannot save nodes.dat");
        }
        if let Ok(list) = parse_nodes_dat(&bytes) {
            return list;
        }
    }

    saved
        .as_deref()
        .and_then(|bytes| parse_nodes_dat(bytes).ok())
        .unwrap_or_default()
}

/// Fetch `url`, no longer than `ttl` (a slow list is not worth a stuck start).
async fn fetch(url: &str, ttl: Duration) -> Option<Vec<u8>> {
    let client = reqwest::Client::builder()
        .timeout(ttl.min(Duration::from_secs(20)))
        .build()
        .ok()?;
    let answer = client.get(url).send().await.ok()?.error_for_status().ok()?;
    let bytes = answer.bytes().await.ok()?;
    // A `nodes.dat` is a few kilobytes; anything else is a page or a redirect notice.
    if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 {
        return None;
    }
    Some(bytes.to_vec())
}

/// Write what the routing table learned, so the next run bootstraps from contacts this one
/// answered. Best effort: a failure costs a slower start, nothing else.
pub fn save(settings: &Settings, _kad: &KadHandle, contacts: Vec<Contact>) {
    if contacts.is_empty() {
        return;
    }
    let bytes = write_nodes_dat(&contacts);
    if let Err(e) = std::fs::write(settings.nodes_path(), bytes) {
        tracing::debug!(error = %e, "ed2k: cannot update nodes.dat");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rucio_emule::kad::packet::KadId;

    #[test]
    fn a_list_written_is_a_list_read() {
        let dir = tempfile::tempdir().expect("a folder");
        let mut settings = Settings::new(dir.path());
        settings.nodes_ttl = Duration::from_secs(3600);
        let contact = Contact {
            id: KadId::random(),
            ip: "192.0.2.1".parse().expect("ip"),
            udp_port: 4672,
            tcp_port: 4662,
            version: 8,
            udp_key: None,
        };
        let bytes = write_nodes_dat(std::slice::from_ref(&contact));
        std::fs::write(settings.nodes_path(), bytes).expect("save");
        let list = parse_nodes_dat(&std::fs::read(settings.nodes_path()).expect("read"))
            .expect("parse");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].ip, contact.ip);
        assert_eq!(list[0].udp_port, 4672);
    }

    #[test]
    fn no_saved_list_and_no_answer_is_an_empty_bootstrap() {
        let dir = tempfile::tempdir().expect("a folder");
        let mut settings = Settings::new(dir.path());
        // Nothing saved, and an address nothing answers on.
        settings.nodes_dat_url = "http://127.0.0.1:9/nodes.dat".to_string();
        settings.nodes_ttl = Duration::from_millis(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let list = runtime.block_on(contacts(&settings));
        assert!(list.is_empty());
    }
}
