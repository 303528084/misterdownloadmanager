//! An `ed2k://` link, and the name its file takes on this system.
//!
//! The parsing and the hash itself are the protocol crate's ([`rucio_emule::ed2k`]): a link is
//! `ed2k://|file|<name>|<size>|<hash>|/`, where `<hash>` is 32 hex digits of the MD4-based
//! ed2k hash (MD4 of the file, or of the MD4 of each 9,728,000-byte part, hashed together).
//! What this module adds is what a person copying a link from a page runs into: spaces or a
//! newline around it, the scheme in capitals, and a file name no system would accept.

pub use rucio_emule::ed2k::{Ed2kHash, Ed2kLink};

/// Why a link could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LinkError {
    /// Not an ed2k link at all (another program's, or something else copied by mistake).
    #[error("not an ed2k link")]
    NotEd2k,
    /// An ed2k link, but not to a file: a server list entry (`ed2k://|server|…|/`) or one this
    /// program can't read.
    #[error("not an ed2k file link: {0}")]
    NotAFile(String),
}

/// Read `text` as an ed2k link: what a person copies, with what surrounds it trimmed and the
/// scheme in any case (pages write `ED2K://` too).
pub fn parse(text: &str) -> Result<Ed2kLink, LinkError> {
    let text = text.trim();
    let lower = text.to_ascii_lowercase();
    if !lower.starts_with("ed2k://") {
        return Err(LinkError::NotEd2k);
    }
    // The protocol crate reads the canonical form; a lead in capitals is all MDM forgives.
    let canonical = format!("ed2k://{}", &text[7..]);
    Ed2kLink::parse(&canonical).map_err(|e| LinkError::NotAFile(e.to_string()))
}

/// Something MDM could read as an ed2k link, without reading it (`Add URL`'s first look).
pub fn looks_like_link(text: &str) -> bool {
    text.trim_start()
        .to_ascii_lowercase()
        .starts_with("ed2k://")
}

/// Read `text` as an ed2k link, also when it comes wrapped the way download pages wrap
/// addresses for Thunder (Xunlei), FlashGet and QQ's downloader: base64 around the address,
/// with `AA`…`ZZ` or `[FLASHGET]`…`[FLASHGET]` around that. The wrapper is a download page's
/// doing, not the network's: what is downloaded is the ed2k link inside.
pub fn parse_wrapped(text: &str) -> Result<Ed2kLink, LinkError> {
    if let Ok(link) = parse(text) {
        return Ok(link);
    }
    let inner = unwrap(text).ok_or(LinkError::NotEd2k)?;
    parse(&inner)
}

/// The address inside a wrapper link, if `text` is one.
fn unwrap(text: &str) -> Option<String> {
    const BASE64: base64::engine::general_purpose::GeneralPurpose =
        base64::engine::general_purpose::STANDARD;
    let text = text.trim();
    let lower = text.to_ascii_lowercase();
    let (scheme, opening, closing) = if lower.starts_with("thunder://") {
        ("thunder://", "AA", "ZZ")
    } else if lower.starts_with("flashget://") {
        ("flashget://", "[FLASHGET]", "[FLASHGET]")
    } else if lower.starts_with("qqdl://") {
        ("qqdl://", "", "")
    } else {
        return None;
    };
    let rest = text.get(scheme.len()..)?;
    // FlashGet's links end in `&<something>` that is not part of the address.
    let body = rest.split('&').next().unwrap_or(rest);
    use base64::Engine as _;
    let decoded = BASE64.decode(body.trim_end_matches('/')).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let inner = if opening.is_empty() {
        decoded.as_str()
    } else {
        decoded.strip_prefix(opening)?.strip_suffix(closing)?
    };
    Some(inner.to_string())
}

/// `name` as a file name this system accepts: the link's name comes from whoever shared the
/// file, over the network, and may hold any character (also `/`, `\`, `:` and the null byte)
/// or be a Windows device name (`CON`, `NUL`, `COM1`…). Characters are replaced, never
/// dropped, so two different names don't become the same file; a name that is empty or all
/// dots and spaces afterwards becomes `file`.
pub fn safe_name(name: &str) -> String {
    const BAD: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
    let mut out: String = name
        .chars()
        .map(|c| {
            if BAD.contains(&c) || (c as u32) < 0x20 || c == '\u{7f}' {
                '_'
            } else {
                c
            }
        })
        .collect();
    // Windows refuses a name ending in a dot or a space, and silently drops them.
    while out.ends_with([' ', '.']) {
        out.pop();
    }
    // A device name is a device whatever its extension: `NUL.mp4` is still `NUL`.
    let stem = out.split('.').next().unwrap_or("").to_ascii_uppercase();
    let device = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit());
    if device {
        out.insert(0, '_');
    }
    // 255 bytes is what most systems take for one name; cut on a character boundary.
    const LIMIT: usize = 200;
    if out.len() > LIMIT {
        let mut cut = LIMIT;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        let tail = out[cut..].to_string();
        out.truncate(cut);
        // Keep the extension: it is what the system opens the file with.
        if let Some(dot) = tail.find('.') {
            out.push_str(&tail[dot..]);
        }
    }
    if out.is_empty() || out.chars().all(|c| c == '.' || c == ' ') {
        return "file".to_string();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    const LINK: &str = "ed2k://|file|a.iso|733468672|0123456789ABCDEF0123456789ABCDEF|/";

    #[test]
    fn a_link_reads_and_the_scheme_may_be_capitalised() {
        let l = parse(LINK).expect("the canonical form");
        assert_eq!(l.name, "a.iso");
        assert_eq!(l.size, 733_468_672);
        assert_eq!(l.hash.to_hex(), "0123456789abcdef0123456789abcdef");
        assert_eq!(parse(&format!("  {}  \n", LINK)).expect("surrounded"), l);
        assert_eq!(
            parse(&LINK.replace("ed2k://", "ED2K://")).expect("capitals"),
            l
        );
    }

    #[test]
    fn what_is_not_a_file_link_is_not_one() {
        assert_eq!(parse("https://example.org/a.iso"), Err(LinkError::NotEd2k));
        assert_eq!(parse(""), Err(LinkError::NotEd2k));
        // Server list entries share the scheme but name no file.
        assert!(matches!(
            parse("ed2k://|server|45.82.80.155|4232|/"),
            Err(LinkError::NotAFile(_))
        ));
        // A hash that is not 32 hex digits.
        assert!(matches!(
            parse("ed2k://|file|a.iso|1|xyz|/"),
            Err(LinkError::NotAFile(_))
        ));
        assert!(!looks_like_link("thunder://QUFlZDJrOi8vfA=="));
        assert!(looks_like_link(" ED2K://|file|a|1|b|/"));
    }

    /// The link a download page gave, wrapped for Thunder: what MDM must read it as.
    const THUNDER: &str = "thunder://QUFlZDJrOi8vfGZpbGV844CQ6auY5riFTVA055S15b2xd3d3LmRvbXA0LmNj44CRanRkeWouMjAyNC5IRDEwODBwLmd5enoubXA0fDEwMzQzODQ3MjV8ZTM5NjQxMzhmODY2MjVkYTc1MTAzZTUzYWEwYWYxYTF8L1pa";

    #[test]
    fn a_link_wrapped_for_thunder_is_the_link_inside() {
        let link = parse_wrapped(THUNDER).expect("the wrapped link");
        assert_eq!(link.size, 1_034_384_725);
        assert_eq!(link.hash.to_hex(), "e3964138f86625da75103e53aa0af1a1");
        assert_eq!(
            link.name,
            "【高清MP4电影www.domp4.cc】jtdyj.2024.HD1080p.gyzz.mp4"
        );
        // And a plain link still reads as itself.
        assert_eq!(
            parse_wrapped(LINK).expect("plain"),
            parse(LINK).expect("plain")
        );
        assert!(matches!(
            parse_wrapped("thunder://not*base64"),
            Err(LinkError::NotEd2k)
        ));
        // A wrapper around something MDM doesn't download is still not an ed2k file link.
        let magnet =
            base64::engine::general_purpose::STANDARD.encode("AAmagnet:?xt=urn:btih:abcZZ");
        assert!(parse_wrapped(&format!("thunder://{magnet}")).is_err());
    }

    #[test]
    fn a_name_the_system_refuses_becomes_one_it_takes() {
        assert_eq!(
            safe_name("a/b\\c:d*e?f\"g<h>i|j.iso"),
            "a_b_c_d_e_f_g_h_i_j.iso"
        );
        assert_eq!(safe_name("dots...   "), "dots");
        assert_eq!(safe_name(""), "file");
        assert_eq!(safe_name("   "), "file");
        assert_eq!(safe_name("NUL.mp4"), "_NUL.mp4");
        assert_eq!(safe_name("com1"), "_com1");
        assert_eq!(safe_name("CONSOLE.exe"), "CONSOLE.exe");
        assert_eq!(safe_name("lot\u{7f}of\ncontrol"), "lot_of_control");
        let long = "x".repeat(400) + ".mkv";
        let cut = safe_name(&long);
        assert!(cut.len() <= 200 + 4, "{}", cut.len());
        assert!(cut.ends_with(".mkv"));
    }
}
