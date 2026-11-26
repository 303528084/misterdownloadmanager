//! An ed2k download, end to end, over loopback: a peer serves a file with the eMule peer
//! protocol, and MDM's session downloads it, part by part, verifies it against the link's hash
//! and puts it in its folder. Also resumed: a `.part` left by an earlier run (with its progress
//! file lost) is checked against the peer's hash set and not fetched again.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{SIZE, make_file, serve, serve_claiming, settings_for, wait_finished};
use mdm_ed2k::{Ed2k, Ed2kLink, PART_SIZE, Settings, Spec, State};
use rucio_emule::ed2k::hash_reader_full;

#[tokio::test(flavor = "multi_thread")]
async fn a_file_is_downloaded_verified_and_named_as_the_link_says() {
    let dir = tempfile::tempdir().expect("a folder");
    let source = dir.path().join("source.bin");
    make_file(&source);
    let serving = serve(&source, "a film.mkv").await;

    let link = {
        let (hash, _) = hash_reader_full(std::fs::File::open(&source).expect("open")).expect("hash");
        Ed2kLink {
            name: "a film.mkv".to_string(),
            size: SIZE,
            hash,
        }
    };

    let out = dir.path().join("out");
    let settings = settings_for(&dir.path().join("session"), serving.port);
    let ed2k = Ed2k::start(settings).await.expect("the session starts");
    let id = ed2k
        .add(Spec {
            link: link.clone(),
            dir: out.clone(),
        })
        .await
        .expect("the download is added");

    let stats = wait_finished(&ed2k, id, Duration::from_secs(120)).await;
    assert_eq!(stats.state, State::Finished, "{stats:?}");
    assert_eq!(stats.done_parts, 2);
    assert_eq!(stats.downloaded, SIZE);

    let target = out.join("a film.mkv");
    let got = std::fs::read(&target).expect("the file is in its folder");
    assert_eq!(got.len() as u64, SIZE);
    let (got_hash, _) = hash_reader_full(&got[..]).expect("hash");
    assert_eq!(got_hash, link.hash, "the bytes are the file the link names");
    assert_eq!(
        serving.uploaded.load(Ordering::Relaxed),
        SIZE,
        "the peer served the file once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_left_by_an_earlier_run_is_not_fetched_again() {
    let dir = tempfile::tempdir().expect("a folder");
    let source = dir.path().join("source.bin");
    make_file(&source);
    let serving = serve(&source, "resumed.mkv").await;

    let link = {
        let (hash, _) = hash_reader_full(std::fs::File::open(&source).expect("open")).expect("hash");
        Ed2kLink {
            name: "resumed.mkv".to_string(),
            size: SIZE,
            hash,
        }
    };

    let session = dir.path().join("session");
    let settings = settings_for(&session, serving.port);
    // An earlier run got the first part and lost its progress file: only the `.part` is there.
    std::fs::create_dir_all(&session).expect("session folder");
    {
        let part = settings.part_path(&link.hash);
        let bytes = std::fs::read(&source).expect("read");
        std::fs::write(&part, &bytes[..PART_SIZE as usize]).expect("write the first part");
        assert!(!settings.met_path(&link.hash).exists());
    }

    let out = dir.path().join("out");
    let ed2k = Ed2k::start(settings).await.expect("the session starts");
    let id = ed2k
        .add(Spec {
            link: link.clone(),
            dir: out.clone(),
        })
        .await
        .expect("the download is added");
    let stats = wait_finished(&ed2k, id, Duration::from_secs(120)).await;
    assert_eq!(stats.state, State::Finished, "{stats:?}");
    assert_eq!(stats.done_parts, 2);

    let got = std::fs::read(out.join("resumed.mkv")).expect("the file is in its folder");
    let (got_hash, _) = hash_reader_full(&got[..]).expect("hash");
    assert_eq!(got_hash, link.hash);
    assert_eq!(
        serving.uploaded.load(Ordering::Relaxed),
        SIZE - PART_SIZE,
        "only the missing part was asked for, not the one already on disk"
    );
}

/// A peer serving a different file than the link names: every part arrives, and the file is
/// refused at the end (its hash is the one thing the link guarantees).
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_serving_another_file_is_refused() {
    let dir = tempfile::tempdir().expect("a folder");
    let source = dir.path().join("source.bin");
    make_file(&source);

    // The link names a file of the same size whose hash is not what the peer sends.
    let other = dir.path().join("other.bin");
    std::fs::write(&other, vec![7u8; SIZE as usize]).expect("another file");
    let (other_hash, _) =
        hash_reader_full(std::fs::File::open(&other).expect("open")).expect("hash");
    let serving = serve_claiming(&source, "wrong.mkv", other_hash).await;
    let link = Ed2kLink {
        name: "wrong.mkv".to_string(),
        size: SIZE,
        hash: other_hash,
    };

    let out = dir.path().join("out");
    let settings = settings_for(&dir.path().join("session"), serving.port);
    let ed2k = Ed2k::start(settings).await.expect("the session starts");
    let id = ed2k
        .add(Spec {
            link,
            dir: out.clone(),
        })
        .await
        .expect("the download is added");
    let stats = wait_finished(&ed2k, id, Duration::from_secs(120)).await;
    assert_eq!(stats.state, State::Failed, "{stats:?}");
    assert!(
        stats.error.as_deref().unwrap_or("").contains("not the one"),
        "{:?}",
        stats.error
    );
    assert!(
        !out.join("wrong.mkv").exists(),
        "nothing is put in the folder when the file is not the one the link names"
    );
}

/// The real network, when it is asked for: `cargo test -p mdm-ed2k -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the network: Kad bootstrap from the public nodes.dat"]
async fn the_kad_network_can_be_joined() {
    let dir = tempfile::tempdir().expect("a folder");
    let mut settings = Settings::new(dir.path());
    settings.tcp_port = 0;
    settings.udp_port = 0;
    let ed2k = Ed2k::start(settings).await.expect("the session starts");
    let contacts = ed2k.kad_contacts().await;
    assert!(contacts > 10, "a Kad table of {contacts} contacts");
}
