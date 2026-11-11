//! Upgrading to a published release (`pumasi upgrade latest`, 1.3).
//!
//! The control plane looks up the newest release on the release site
//! (GitHub: `<source>/latest` redirects to `<source>/tag/<tag>`), reads
//! the release's `SHA256SUMS`, downloads the binary of every platform its
//! nodes run, and checks each one before it is used: the SHA-256 must match
//! `SHA256SUMS`, the file must be a pumasi binary for that platform (its
//! executable header), and its build marker must carry the release's
//! version. The binaries then go into the release store and the rollout
//! starts exactly as for `pumasi upgrade rollout`: the control plane signs
//! each with the cluster key and agents verify that signature, the size
//! and the hash again before they run anything.

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::release::{
    FetchState, LatestReleaseView, MAX_BINARY_BYTES, ReleaseFile, Target, asset_name,
    detect_target, hex, parse_sha256sums, read_build_info, same_version, sha256, sha256_file,
};

use crate::rollout::ReleaseStore;

/// Largest `SHA256SUMS` accepted.
const MAX_SUMS_BYTES: usize = 64 * 1024;
/// How long looking up the newest release may take.
const CHECK_TIMEOUT: Duration = Duration::from_secs(20);
/// How long one binary's download may take.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// What the control plane knows about published releases.
#[derive(Debug, Clone)]
pub(crate) struct LatestState {
    pub latest: Option<String>,
    pub checked_unix: u64,
    pub check_error: Option<String>,
    pub fetch: FetchState,
}

impl Default for LatestState {
    fn default() -> Self {
        Self {
            latest: None,
            checked_unix: 0,
            check_error: None,
            fetch: FetchState::Idle,
        }
    }
}

impl LatestState {
    pub fn view(&self, source: &str) -> LatestReleaseView {
        LatestReleaseView {
            source: source.to_string(),
            latest: self.latest.clone(),
            checked_unix: self.checked_unix,
            check_error: self.check_error.clone(),
            control_plane: pumasi_proto::version().to_string(),
            fetch: self.fetch.clone(),
        }
    }
}

fn source_url(source: &str, path: &str) -> String {
    format!("{}/{path}", source.trim_end_matches('/'))
}

fn no_redirects() -> Result<reqwest::Client> {
    // The same TLS roots as everything else; only redirects differ.
    let roots = pumasi_proto::registry::root_certs();
    let mut tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .user_agent(concat!("pumasi/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build HTTP client")
}

/// The newest release's tag: where `<source>/latest` redirects to.
pub(crate) async fn latest_tag(source: &str) -> Result<String> {
    let url = source_url(source, "latest");
    let http = no_redirects()?;
    let resp = tokio::time::timeout(CHECK_TIMEOUT, http.get(&url).send())
        .await
        .map_err(|_| anyhow::anyhow!("GET {url}: timed out"))?
        .with_context(|| format!("GET {url}"))?;
    ensure!(
        resp.status().is_redirection(),
        "GET {url}: expected a redirect to the newest release, got {}",
        resp.status()
    );
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .context("the release site sent no Location")?;
    tag_from_location(location)
        .with_context(|| format!("the release site redirected to {location}, not to a release"))
}

fn tag_from_location(location: &str) -> Option<String> {
    let path = location.split(['?', '#']).next()?;
    let (_, tag) = path.rsplit_once("/tag/")?;
    let tag = tag.trim_end_matches('/');
    let ok = !tag.is_empty()
        && tag.len() <= 64
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'));
    ok.then(|| tag.to_string())
}

/// Checks a tag someone typed, so it can go into a URL.
pub(crate) fn check_tag(tag: &str) -> Result<String> {
    let tag = tag.trim();
    tag_from_location(&format!("/tag/{tag}"))
        .filter(|t| t == tag)
        .with_context(|| format!("{tag:?} is not a release tag (like v1.2.3)"))
}

async fn get_capped(
    http: &reqwest::Client,
    url: &str,
    cap: u64,
    progress: &(dyn Fn(u64, Option<u64>) + Send + Sync),
) -> Result<Vec<u8>> {
    let mut resp = http
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    ensure!(resp.status().is_success(), "GET {url}: {}", resp.status());
    let total = resp.content_length();
    if let Some(n) = total {
        ensure!(n <= cap, "GET {url}: {n} bytes is too large");
    }
    let mut out = Vec::with_capacity(total.unwrap_or(0).min(cap) as usize);
    let mut last = 0u64;
    while let Some(chunk) = resp.chunk().await.with_context(|| format!("GET {url}"))? {
        ensure!(
            out.len() as u64 + chunk.len() as u64 <= cap,
            "GET {url}: response too large"
        );
        out.extend_from_slice(&chunk);
        if out.len() as u64 - last >= 1 << 20 {
            last = out.len() as u64;
            progress(last, total);
        }
    }
    Ok(out)
}

/// Downloads and checks the binaries of release `tag` for `targets` into
/// the release store. Platforms the release has no binary for are left
/// out; `progress` hears what it is doing.
pub(crate) async fn fetch_release(
    source: &str,
    tag: &str,
    targets: &BTreeSet<Target>,
    releases: &ReleaseStore,
    progress: &(dyn Fn(String) + Send + Sync),
) -> Result<(Vec<ReleaseFile>, Vec<Target>)> {
    let http = pumasi_proto::registry::http_client()?;
    progress("reading SHA256SUMS".into());
    let sums_url = source_url(source, &format!("download/{tag}/SHA256SUMS"));
    let sums = get_capped(&http, &sums_url, MAX_SUMS_BYTES as u64, &|_, _| {}).await?;
    let sums = parse_sha256sums(&String::from_utf8_lossy(&sums));
    ensure!(!sums.is_empty(), "{sums_url} lists no files");
    let mut out = Vec::new();
    let mut missing = Vec::new();
    for target in targets {
        let name = asset_name(target);
        let Some((_, want)) = sums.iter().find(|(n, _)| *n == name) else {
            missing.push(target.clone());
            continue;
        };
        let want = *want;
        let (bytes, reused) = match stored(releases, &want) {
            Some(bytes) => (bytes, true),
            None => {
                let url = source_url(source, &format!("download/{tag}/{name}"));
                progress(format!("downloading {name}"));
                let report = |have: u64, total: Option<u64>| {
                    let mib = |b: u64| b as f64 / (1 << 20) as f64;
                    progress(match total {
                        Some(t) => {
                            format!("downloading {name} ({:.0}/{:.0} MiB)", mib(have), mib(t))
                        }
                        None => format!("downloading {name} ({:.0} MiB)", mib(have)),
                    })
                };
                let bytes = tokio::time::timeout(
                    DOWNLOAD_TIMEOUT,
                    get_capped(&http, &url, MAX_BINARY_BYTES, &report),
                )
                .await
                .map_err(|_| anyhow::anyhow!("GET {url}: timed out"))??;
                (bytes, false)
            }
        };
        let file = check_binary(&name, target, tag, &want, &bytes)?;
        if !reused {
            releases.upload(&file.sha256, file.size, 0, &bytes)?;
        }
        out.push(file);
    }
    Ok((out, missing))
}

/// A binary already in the release store under this hash.
fn stored(releases: &ReleaseStore, sha: &[u8; 32]) -> Option<Vec<u8>> {
    let path = releases.path_of(sha);
    if sha256_file(&path).ok()? != *sha {
        return None;
    }
    std::fs::read(path).ok()
}

/// The checks a downloaded binary must pass before it is rolled out.
pub(crate) fn check_binary(
    name: &str,
    target: &Target,
    tag: &str,
    want: &[u8; 32],
    bytes: &[u8],
) -> Result<ReleaseFile> {
    let got = sha256(bytes);
    if got != *want {
        bail!(
            "{name}: SHA-256 {} does not match SHA256SUMS ({})",
            hex(&got),
            hex(want)
        );
    }
    let found = detect_target(bytes).with_context(|| format!("{name} is not an executable"))?;
    ensure!(
        found == *target,
        "{name} is a binary for {found}, not {target}"
    );
    let info =
        read_build_info(bytes).with_context(|| format!("{name} has no pumasi build marker"))?;
    ensure!(
        same_version(&info.version, tag),
        "{name} says it is version {}, but the release is {tag}",
        info.version
    );
    Ok(ReleaseFile {
        target: target.clone(),
        version: info.version,
        protocol: info.protocol,
        sha256: got,
        size: bytes.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_from_redirects() {
        assert_eq!(
            tag_from_location("https://github.com/syang0531/pumasi/releases/tag/v1.2.3").as_deref(),
            Some("v1.2.3")
        );
        assert_eq!(
            tag_from_location("/releases/tag/v1.3.0-rc1/").as_deref(),
            Some("v1.3.0-rc1")
        );
        assert_eq!(
            tag_from_location("https://github.com/syang0531/pumasi/releases"),
            None
        );
        assert_eq!(tag_from_location("/releases/tag/v1/../../x"), None);
        assert_eq!(tag_from_location("/releases/tag/"), None);
        assert!(check_tag("v1.2.3").is_ok());
        assert!(check_tag("v1.2.3/../x").is_err());
        assert!(check_tag("").is_err());
    }

    #[test]
    fn binaries_must_match_their_sums_platform_and_version() {
        // This test binary is a real executable for the host, with the
        // build marker of this source tree.
        pumasi_proto::build_info();
        let exe = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&exe).unwrap();
        let host = Target::host();
        let sha = sha256(&bytes);
        let version = pumasi_proto::version();
        let tag = format!("v{version}");
        let f = check_binary("pumasi", &host, &tag, &sha, &bytes).unwrap();
        assert_eq!(f.target, host);
        assert_eq!(f.version, version);
        assert_eq!(f.sha256, sha);

        let wrong_sha = sha256(b"not it");
        let e = check_binary("pumasi", &host, &tag, &wrong_sha, &bytes).unwrap_err();
        assert!(e.to_string().contains("does not match SHA256SUMS"), "{e}");

        let other = if host.os == "windows" {
            Target::new("linux", &host.arch)
        } else {
            Target::new("windows", &host.arch)
        };
        let e = check_binary("pumasi", &other, &tag, &sha, &bytes).unwrap_err();
        assert!(e.to_string().contains("is a binary for"), "{e}");

        let e = check_binary("pumasi", &host, "v0.0.1", &sha, &bytes).unwrap_err();
        assert!(e.to_string().contains("but the release is v0.0.1"), "{e}");

        let text = b"#!/bin/sh\necho hi\n".to_vec();
        let e = check_binary("pumasi", &host, &tag, &sha256(&text), &text).unwrap_err();
        assert!(e.to_string().contains("not an executable"), "{e}");
    }
}
