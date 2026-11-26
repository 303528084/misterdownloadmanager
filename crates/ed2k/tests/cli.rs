//! `mdm-ed2k`, the command line downloader, on a link: the binary is the crate's own engine
//! with a terminal on it, so this runs it the way a person would, against a loopback peer.

mod common;

use common::{SIZE, make_file, serve};
use mdm_ed2k::Ed2kLink;
use rucio_emule::ed2k::hash_reader_full;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn the_command_line_downloads_a_link() {
    let dir = tempfile::tempdir().expect("a folder");
    let source = dir.path().join("source.bin");
    make_file(&source);
    let serving = serve(&source, "from-cli.mkv").await;
    let (hash, _) = hash_reader_full(std::fs::File::open(&source).expect("open")).expect("hash");
    let link = Ed2kLink {
        name: "from-cli.mkv".to_string(),
        size: SIZE,
        hash,
    };

    let out = dir.path().join("out");
    let state = dir.path().join("state");
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_mdm-ed2k"))
            .arg(link.to_string())
            .arg("--peer")
            .arg(format!("127.0.0.1:{}", serving.port))
            .arg("--out")
            .arg(&out)
            .arg("--state")
            .arg(&state)
            .arg("--port")
            .arg("0")
            .arg("--no-seed")
            .output(),
    )
    .await
    .expect("the command line ends")
    .expect("the binary runs");

    assert!(
        output.status.success(),
        "exit {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("finished"), "{stdout}");
    let got = std::fs::read(out.join("from-cli.mkv")).expect("the file is in its folder");
    assert_eq!(got.len() as u64, SIZE);
    let (got_hash, _) = hash_reader_full(&got[..]).expect("hash");
    assert_eq!(got_hash, hash);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_line_that_is_not_a_link_is_refused_before_anything_starts() {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_mdm-ed2k"))
        .arg("https://example.org/a.iso")
        .output()
        .await
        .expect("the binary runs");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not an ed2k link"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
