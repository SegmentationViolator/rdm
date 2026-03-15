use anyhow::{Context, Result};
use reqwest::header::CONTENT_LENGTH;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::cli;
use crate::config::Config;
use crate::queue;
use crate::scrape;

pub async fn run(
    cfg: &Config,
    url: &str,
    connections: usize,
    parallel: usize,
    delete: bool,
    ext_filter: Option<HashSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    {
        let q = queue::Queue::load_readonly();
        if q.pending_count() > 0 {
            anyhow::bail!(
                "Queue has {} pending item(s).\n  \
                 Run 'rdm queue start' to finish them, or 'rdm queue clear' to reset.",
                q.pending_count()
            );
        }
    }

        let files = scrape::discover_files(url, false)
        .await
        .context("Failed to scan remote directory")?;

    let files = match files {
        Some(f) if !f.is_empty() => f,
        _ => {
            eprintln!("  ❌ No files found at {}", url);
            return Ok(());
        }
    };

    if cancel.is_cancelled() {
        eprintln!("  ⚠ Cancelled during scan.");
        return Ok(());
    }

    let total_before_filter = files.len();

    let files: Vec<scrape::DiscoveredFile> = match &ext_filter {
        Some(exts) => files
            .into_iter()
            .filter(|f| {
                let name = extract_filename(&f.relative_path).to_lowercase();
                file_has_ext(&name, exts)
            })
            .collect(),
        None => files,
    };

    if let Some(exts) = ext_filter.as_ref() {
        let mut sorted: Vec<&str> = exts.iter().map(|s| s.as_str()).collect();
        sorted.sort();
        eprintln!(
            "  🔎 Filter: {} → {} file(s) matching .{}",
            total_before_filter,
            files.len(),
            sorted.join(", ."),
        );
    }

    if files.is_empty() {
        eprintln!("  ❌ No files match the extension filter");
        return Ok(());
    }

    let remote_decoded: HashSet<String> = files
        .iter()
        .map(|f| cli::percent_decode(&f.relative_path))
        .collect();

    let sync_root_result = derive_sync_root(cfg, &files);

    if delete {
        match &sync_root_result {
            SyncRoot::MixedRoots => {
                eprintln!("  ⚠ Cannot use --delete: files have mixed folder roots.");
                eprintln!("    Delete must be performed manually.");
            }
            SyncRoot::Empty => {
                eprintln!("  ⚠ Cannot use --delete: unable to determine sync root.");
            }
            SyncRoot::Ok(_) => {}
        }
    }

    eprintln!("  🔍 Checking {} file(s)...", files.len());

    let mut needs_head: Vec<(String, String, PathBuf, u64)> = Vec::new();
    let mut to_download: Vec<(String, String)> = Vec::new();

    for f in &files {
        let path = local_path(cfg, &f.relative_path);
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() && m.len() > 0 => {
                needs_head.push((f.url.clone(), f.relative_path.clone(), path, m.len()));
            }
            _ => {
                to_download.push((f.url.clone(), f.relative_path.clone()));
            }
        }
    }

    let mut up_to_date = 0u64;

    if !needs_head.is_empty() {
        eprintln!("  📡 Verifying {} existing file(s)...", needs_head.len());

        let client = reqwest::Client::builder()
            .user_agent("rdm")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .context("Failed to build HTTP client")?;

        let sem = Arc::new(Semaphore::new(8));
        let mut tasks = JoinSet::new();

        for (file_url, relative, path, size) in needs_head {
            let client = client.clone();
            let sem = sem.clone();
            let cancel = cancel.clone();

            tasks.spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                if cancel.is_cancelled() {
                    return None;
                }
                let status = head_compare(&client, &file_url, size).await;
                Some((file_url, relative, path, status))
            });
        }

        while let Some(joined) = tasks.join_next().await {
            if cancel.is_cancelled() {
                tasks.abort_all();
                eprintln!("  ⚠ Cancelled during verification.");
                return Ok(());
            }
            let (file_url, relative, _path, status) =
                match joined.context("Task panicked")? {
                    Some(v) => v,
                    None => continue,
                };
            match status {
                HeadStatus::UpToDate | HeadStatus::HeadFailed => {
                    up_to_date += 1;
                }
                HeadStatus::SizeMismatch | HeadStatus::NoContentLength => {
                    to_download.push((file_url, relative));
                }
            }
        }
    }

    to_download.sort_by(|a, b| a.1.cmp(&b.1));

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        if let SyncRoot::Ok(ref root) = sync_root_result {
            let root_path = Path::new(root);
            if root_path.is_dir() {
                collect_orphan_files(
                    root_path,
                    root_path,
                    &remote_decoded,
                    &ext_filter,
                    &mut to_delete,
                );
                to_delete.sort();
            }
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", files.len());
    eprintln!("  Up to date : {}", up_to_date);
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }

    if to_download.is_empty() && to_delete.is_empty() {
        eprintln!();
        eprintln!("  ✅ Everything is up to date!");
        return Ok(());
    }

    if !to_download.is_empty() {
        eprintln!();
        for (_, relative) in &to_download {
            eprintln!("     + {}", cli::percent_decode(relative));
        }
    }

    if !to_delete.is_empty() {
        eprintln!();
        for path in &to_delete {
            eprintln!("     - {}", path);
        }
    }

    eprintln!();

    if !to_download.is_empty() {
        for (_, relative) in &to_download {
            let path = local_path(cfg, relative);
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(format!("{}.part", path.display()));
            let meta = crate::resume::ResumeMetadata::meta_path(&path.to_string_lossy());
            let _ = std::fs::remove_file(&meta);
        }

        for (_, relative) in &to_download {
            let path = local_path(cfg, relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
        }

        queue::Queue::locked(|q| {
            for (file_url, relative) in &to_download {
                let decoded = cli::percent_decode(relative);
                q.add(file_url.clone(), Some(decoded), Some(connections));
            }
            Ok(())
        })?;

        eprintln!(
            "  📥 Starting download ({} file(s), {} parallel)...",
            to_download.len(),
            parallel,
        );
        eprintln!();

        let result = queue::start(cfg, cancel.clone(), parallel).await;
        let _ = queue::Queue::locked(|q| Ok(q.clear_finished()));

        if let Err(e) = result {
            eprintln!("  ⚠ Some downloads failed — skipping delete phase.");
            return Err(e);
        }

        let q = queue::Queue::load_readonly();
        if q.failed_count() > 0 {
            eprintln!(
                "  ⚠ {} download(s) failed — skipping delete phase.",
                q.failed_count()
            );
            return Ok(());
        }
    }

    if !to_delete.is_empty() {
        if cancel.is_cancelled() {
            eprintln!("  ⚠ Cancelled before delete phase.");
            return Ok(());
        }

        let total_local = up_to_date as usize + to_delete.len();
        if total_local > 0 {
            let pct = (to_delete.len() as f64 / total_local as f64) * 100.0;
            if to_delete.len() > 10 && pct > 50.0 {
                eprintln!(
                    "  ⚠ Warning: about to delete {} of {} local files ({:.0}%)",
                    to_delete.len(),
                    total_local,
                    pct,
                );
                eprintln!("    This usually means the remote listing is incomplete.");
                eprint!("    Continue? [y/N]: ");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input).ok();
                if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
                    eprintln!("  ⛔ Aborted.");
                    return Ok(());
                }
            }
        }

        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        if let SyncRoot::Ok(ref root) = sync_root_result {
            for relative in &to_delete {
                let full_path = Path::new(root).join(relative);
                match std::fs::remove_file(&full_path) {
                    Ok(_) => {
                        deleted += 1;
                        let _ =
                            std::fs::remove_file(format!("{}.part", full_path.display()));
                        let meta = crate::resume::ResumeMetadata::meta_path(
                            &full_path.to_string_lossy(),
                        );
                        let _ = std::fs::remove_file(&meta);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        delete_failed += 1;
                        eprintln!("  ⚠ Failed to delete {}: {}", relative, e);
                    }
                }
            }
            remove_empty_dirs(Path::new(root));
        }

        eprintln!("  🗑  Deleted {} file(s)", deleted);
        if delete_failed > 0 {
            eprintln!("  ⚠  Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  ✅ Sync complete!");
    Ok(())
}

enum HeadStatus {
    UpToDate,
    SizeMismatch,
    HeadFailed,
    NoContentLength,
}

enum SyncRoot {
    Ok(String),
    Empty,
    MixedRoots,
}

fn local_path(cfg: &Config, relative: &str) -> PathBuf {
    let decoded = cli::percent_decode(relative);
    PathBuf::from(cfg.resolve_output_path(&decoded))
}

fn extract_filename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn file_has_ext(filename: &str, exts: &HashSet<String>) -> bool {
    let lower = filename.to_lowercase();
    exts.iter().any(|ext| lower.ends_with(&format!(".{}", ext)))
}

async fn head_compare(
    client: &reqwest::Client,
    url: &str,
    local_size: u64,
) -> HeadStatus {
    let resp = match client.head(url).send().await {
        Ok(r) if r.status().is_success() => r,
        _ => return HeadStatus::HeadFailed,
    };
    let remote_size = resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    match remote_size {
        Some(rs) if rs == local_size => HeadStatus::UpToDate,
        Some(_) => HeadStatus::SizeMismatch,
        None => HeadStatus::NoContentLength,
    }
}

fn derive_sync_root(cfg: &Config, files: &[scrape::DiscoveredFile]) -> SyncRoot {
    let first = match files.first() {
        Some(f) => f,
        None => return SyncRoot::Empty,
    };
    let prefix = match first.relative_path.split('/').next() {
        Some(p) if !p.is_empty() => p,
        _ => return SyncRoot::Empty,
    };
    if files
        .iter()
        .any(|f| f.relative_path.split('/').next() != Some(prefix))
    {
        return SyncRoot::MixedRoots;
    }
    let decoded_prefix = cli::percent_decode(prefix);
    SyncRoot::Ok(cfg.resolve_output_path(&decoded_prefix))
}

fn collect_orphan_files(
    dir: &Path,
    base: &Path,
    remote_decoded: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".part") || name.ends_with(".rdm") {
                continue;
            }
        }
        if path.is_dir() {
            collect_orphan_files(&path, base, remote_decoded, ext_filter, out);
        } else if path.is_file() {
            if let Some(exts) = ext_filter.as_ref() {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                if !file_has_ext(name, exts) {
                    continue;
                }
            }
            let relative = match path.strip_prefix(base) {
                Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
                Err(_) => continue,
            };
            if relative.is_empty() {
                continue;
            }
            let folder = match base.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => return,
            };
            let full = format!("{}/{}", folder, relative);
            if !remote_decoded.contains(&full) {
                out.push(relative);
            }
        }
    }
}

fn remove_empty_dirs(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_empty_dirs(&path);
            let _ = std::fs::remove_dir(&path);
        }
    }
}
