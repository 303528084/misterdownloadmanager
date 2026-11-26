//! `mdm-ed2k`: download an eMule link from the command line.
//!
//! ```text
//! mdm-ed2k ed2k://|file|name|size|hash|/ [--out DIR] [--state DIR] [--port N]
//!         [--no-seed] [--peer IP:PORT] [--nodes URL]
//!
//! Also reads the wrappers a download page may put around the link (`thunder://`,
//! `flashget://`, `qqdl://`).
//! ```
//!
//! The same session the app runs (Kad for peers, eMule's peer protocol for parts, the parts
//! server for what is already here), with a line of progress a second and an exit code a script
//! can read: 0 finished, 1 failed, 2 the line was not understood, 130 interrupted (parts kept).

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mdm_ed2k::{Ed2k, Settings, Spec, State, parse_wrapped};

/// What the command line said.
struct Args {
    link: String,
    out: std::path::PathBuf,
    state: std::path::PathBuf,
    port: u16,
    seed: bool,
    peer: Option<String>,
    nodes: Option<String>,
}

const USAGE: &str = "\
usage: mdm-ed2k <ed2k link> [--out DIR] [--state DIR] [--port N] [--no-seed]
                            [--peer IP:PORT] [--nodes URL]

  <ed2k link>  ed2k://|file|name|size|hash|/ , or a thunder://, flashget:// or
               qqdl:// link wrapping one

  --out DIR     where the file goes (default: the current folder)
  --state DIR   where .part files and the Kad list live (default: <out>/.ed2k)
  --port N      the TCP port peers use for their parts (default: 4662, else a free one)
  --no-seed     stop serving the file once it is here (default: keep serving)
  --peer ADDR   ask only this peer (a test, or a known source)
  --nodes URL   the Kad bootstrap list to fetch (default: eMule's public one)";

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut link = None;
    let mut out = std::env::current_dir().map_err(|e| e.to_string())?;
    let mut state = None;
    let mut port = 4662;
    let mut seed = true;
    let mut peer = None;
    let mut nodes = None;
    while let Some(arg) = args.next() {
        let mut value = |name: &str| {
            args.next()
                .ok_or_else(|| format!("{name} needs a value\n\n{USAGE}"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Err(USAGE.to_string()),
            "--out" => out = value("--out")?.into(),
            "--state" => state = Some(value("--state")?.into()),
            "--port" => {
                port = value("--port")?
                    .parse()
                    .map_err(|_| "--port wants a number")?
            }
            "--no-seed" => seed = false,
            "--peer" => peer = Some(value("--peer")?),
            "--nodes" => nodes = Some(value("--nodes")?),
            other if other.starts_with('-') => {
                return Err(format!("unknown option {other}\n\n{USAGE}"));
            }
            other => {
                if link.is_some() {
                    return Err(format!("one link at a time\n\n{USAGE}"));
                }
                link = Some(other.to_string());
            }
        }
    }
    let link = link.ok_or_else(|| USAGE.to_string())?;
    Ok(Args {
        link,
        state: state.unwrap_or_else(|| out.join(".ed2k")),
        out,
        port,
        seed,
        peer,
        nodes,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let link = match parse_wrapped(&args.link) {
        Ok(link) => link,
        Err(e) => {
            eprintln!("mdm-ed2k: {e}");
            return ExitCode::from(2);
        }
    };

    let mut settings = Settings::load(&args.state);
    settings.tcp_port = args.port;
    settings.seed = args.seed;
    if let Some(url) = args.nodes {
        settings.nodes_dat_url = url;
    }
    if let Some(peer) = args.peer {
        // A known peer only: no Kad, no network beyond it (what a test needs).
        let Some(addr) = peer
            .parse::<std::net::SocketAddrV4>()
            .ok()
        else {
            eprintln!("mdm-ed2k: --peer wants IP:PORT");
            return ExitCode::from(2);
        };
        let ip = *addr.ip();
        let port = addr.port();
        settings.sources = Some(Arc::new(move |_hash, _size| {
            Box::pin(async move {
                vec![rucio_emule::kad::search::KadSource {
                    ip,
                    tcp_port: port,
                    udp_port: 0,
                    user_hash: [0u8; 16],
                }]
            })
        }));
    }

    let ed2k = match Ed2k::start(settings).await {
        Ok(ed2k) => ed2k,
        Err(e) => {
            eprintln!("mdm-ed2k: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("{} ({} bytes)", link.name, link.size);
    let id = match ed2k
        .add(Spec {
            link: link.clone(),
            dir: args.out.clone(),
        })
        .await
    {
        Ok(id) => id,
        Err(e) => {
            eprintln!("mdm-ed2k: {e}");
            return ExitCode::FAILURE;
        }
    };

    let started = Instant::now();
    let mut last_line = Instant::now() - Duration::from_secs(10);
    let mut last_bytes = 0u64;
    loop {
        let Some(stats) = ed2k.stats_of(id).await else {
            eprintln!("mdm-ed2k: the download left the list");
            return ExitCode::FAILURE;
        };
        if last_line.elapsed() >= Duration::from_secs(1) {
            let speed = stats.downloaded.saturating_sub(last_bytes) as f64
                / last_line.elapsed().as_secs_f64().max(0.001);
            let percent = if stats.size == 0 {
                0.0
            } else {
                stats.downloaded as f64 * 100.0 / stats.size as f64
            };
            println!(
                "{:>5.1}%  {:>10} / {:>10}  {:>9}/s  parts {}/{}, peers {}, sources {}",
                percent,
                bytes(stats.downloaded),
                bytes(stats.size),
                bytes(speed as u64),
                stats.done_parts,
                stats.parts,
                stats.peers,
                stats.sources
            );
            last_line = Instant::now();
            last_bytes = stats.downloaded;
        }
        match stats.state {
            State::Finished => {
                println!("finished in {:?}", started.elapsed());
                if args.seed {
                    println!("serving it to other peers; Ctrl-C to stop");
                    let _ = tokio::signal::ctrl_c().await;
                    let _ = ed2k.stop().await;
                }
                return ExitCode::SUCCESS;
            }
            State::Failed => {
                eprintln!("mdm-ed2k: {}", stats.error.unwrap_or_else(|| "failed".into()));
                return ExitCode::FAILURE;
            }
            State::Paused | State::Running => {}
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = tokio::signal::ctrl_c() => {
                let _ = ed2k.pause(id).await;
                eprintln!("\nmdm-ed2k: stopped; its parts stay in {}", args.state.display());
                return ExitCode::from(130);
            }
        }
    }
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
