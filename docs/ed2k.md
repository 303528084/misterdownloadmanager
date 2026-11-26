# eDonkey2000 (ed2k) downloads

MDM's eMule engine, in `crates/ed2k`, downloads the files an `ed2k://` link names: it finds the
peers that hold a file in the Kad distributed hash table, asks each of them for parts with the
eMule peer protocol, and checks every part against the hash set the file's hash is built from.

An ed2k link is an address, not a place:

```text
ed2k://|file|name|size|hash|/
```

The `hash` is 32 hex digits of the MD4-based ed2k hash: MD4 of the whole file when it fits in one
part, else MD4 of the MD4 of each 9,728,000-byte part, hashed together. Whoever holds the file is
found by asking, never by a server, and the file is checked against that hash at the end — a peer
that sends something else costs nothing but the bytes it sent.

## What is here

| Piece | What it does |
|---|---|
| `link.rs` | Reads a link (scheme in any case, spaces around it), refuses a server entry, and turns a name from the network into one the system accepts (`safe_name`) |
| `parts.rs` | The parts of a file, which one a peer is fetching, what each must hash to, and what a resumed run already has |
| `download.rs` | One download: a peer is picked, a part asked for and verified, a failure costs that part and a minute of that peer; the finished file is verified whole against the link and put in its folder |
| `nodes.rs` | The Kad bootstrap list (`nodes.dat`), fetched when old and written back with what the routing table learned |
| `engine.rs` | The session: one Kad table, the parts server, pause, resume, remove, and the progress the app reads |
| `settings.rs` | Ports, name, credit identity (kept between runs, in `ed2k.json`), how many peers, whether to keep serving a finished file |

The protocol itself (Kad packets, the eMule TCP handshake, RC4 obfuscation, queue ranks, part
hashes) is [`rucio-emule`](https://github.com/ogarcia/rucio), GPL-3.0-or-later like MDM, pinned by
commit; it has no coupling to the project it comes from.

## Downloading from the command line

```sh
mdm-ed2k "ed2k://|file|name|size|hash|/" --out ~/Downloads
mdm-ed2k "thunder://QUFlZDJrOi8vfGZpbGV84oCm" --out ~/Downloads   # a page's wrapper too
```

Progress goes to the terminal a line a second; a finished download is checked against the link's
hash before it is named and put in its folder. `--no-seed` stops serving it afterwards, `--port`
sets the port peers use, `--state` where the `.part` files and the Kad list live. Interrupting it
keeps every part it has: running it again fetches only what is missing.

Exit codes: `0` finished, `1` failed, `2` the line was not understood, `130` interrupted.

## How a download goes

1. **Sources.** The link's hash is looked up in Kad, which returns the addresses of peers that
   announced it. Sources come and go; the search runs again while the download needs parts.
2. **Parts.** Each peer is asked for one part at a time (`OP_STARTUPLOAD_REQ`, then
   `OP_REQUESTPARTS`), with as many peers at once as the settings allow. A peer that queues the
   request (eMule's queue) is tried later rather than held.
3. **Verification.** A part counts when its MD4 matches the hash set the file's hash is built
   from; the set itself is checked against that hash before it is used, so a lying peer can't get
   anything accepted.
4. **Resuming.** Every finished part is written to `<hash>.part.met` beside `<hash>.part`, and the
   parts on disk are checked again (against the peer's hash set) when that file is lost, so a
   download that was stopped, or one MDM was closed on, goes on where it was.
5. **Serving.** Parts are served to other peers while downloading and, with seeding on, after the
   file is finished. eMule credits a peer that uploads with a better place in the queues: without
   it, a download sits at the end of everyone's.

## What a firewall or a router changes

Peers reach MDM on its TCP port (`4662` by default, else a free one) and Kad uses UDP `4672`. With
neither forwarded, MDM is a **LowID** peer: it still downloads, but it can't accept connections
from peers that are also behind a firewall, which means fewer sources and longer queues. Forwarding
both ports to this computer makes it **HighID**.

## For the app (what is still to wire)

The engine is complete and tested; putting it in the window is the remaining work:

- **Add URL**, Link grabber and the clipboard: an `ed2k://` link is an address MDM accepts (also
  inside a `thunder://` wrapper, which the app already unwraps), and adds a download held by the
  ed2k session instead of the HTTP engine.
- **The list**: a download's progress comes from `Ed2k::stats()` (`downloaded`, `sources`, `peers`)
  rather than from the HTTP engine's; **Pause**, **Resume**, **Stop** and **Remove** call
  `pause`, `resume`, `remove`.
- **Settings**: `Settings` (ports, name, seeding, peers) in a page of its own, saved with the rest.
- **Naming**: the file is put in the download's folder by the engine; a finished download needs no
  move, and a name another file has gets ` (1)`.

`cargo test -p mdm-ed2k` runs the engine against a loopback peer: a file downloaded, verified,
named and resumed, a peer serving another file refused, and the command line doing the same. The
one test that needs the network (`the_kad_network_can_be_joined`) is `#[ignore]`d:
`cargo test -p mdm-ed2k -- --ignored`.
