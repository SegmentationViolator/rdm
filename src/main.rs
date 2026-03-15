use rdm::cli;
use rdm::config;
use rdm::queue;
use rdm::scrape;
use rdm::signal;
use rdm::sync;

use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use tokio_util::sync::CancellationToken;

fn parse_download_args(args: &[String]) -> (Option<String>, Option<usize>) {
    let mut output = None;
    let mut connections = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-o" | "--output" => {
                output = args.get(i + 1).cloned();
                i += 2;
            }
            "-c" | "--connections" => {
                connections = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            _ => i += 1,
        }
    }

    (output, connections)
}

fn parse_parallel_flag(args: &[String]) -> Option<usize> {
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--parallel" => {
                return args.get(i + 1).and_then(|v| v.parse().ok());
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn parse_delete_flag(args: &[String]) -> bool {
    args.iter().any(|a| a == "--delete" || a == "-d")
}

fn parse_ext_filter(args: &[String]) -> Option<HashSet<String>> {
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--ext" | "-e" => {
                return args.get(i + 1).map(|v| {
                    v.split(',')
                        .map(|e| e.trim().trim_start_matches('.').to_lowercase())
                        .filter(|e| !e.is_empty())
                        .collect()
                });
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn resolve_output(output: Option<String>, url: &str, cfg: &config::Config) -> String {
    let filename_from_url = || -> String {
        let name = url
            .split('?')
            .next()
            .unwrap_or(url)
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("download.bin");
        cli::percent_decode(name)
    };

    match output {
        Some(o) => {
            let path = Path::new(&o);
            if o.ends_with('/') || o.ends_with('\\') || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches('\\');
                format!("{}/{}", dir, filename_from_url())
            } else if path.is_absolute() {
                o
            } else {
                cfg.resolve_output_path(&o)
            }
        }
        None => cfg.resolve_output_path(&filename_from_url()),
    }
}

fn looks_like_directory(url: &str) -> bool {
    if url.ends_with('/') {
        return true;
    }
    let last_segment = url
        .split('?')
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");

    if last_segment.is_empty() {
        return true;
    }

    if last_segment.contains('.') {
        return false;
    }

    let is_hex_like = last_segment.len() > 16
        && last_segment.chars().all(|c| c.is_ascii_hexdigit() || c == '-' || c == '_');

    !is_hex_like
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut cfg = config::Config::load();

    match args.get(1).map(|s| s.as_str()) {
        Some("download") | Some("d") => {
            let url = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("Usage: rdm download <URL> [-o name] [-c N]"))?
                .clone();
            let (output, connections) = parse_download_args(&args[3..]);
            let connections = connections.unwrap_or(cfg.connections);
            let output_path = resolve_output(output, &url, &cfg);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());
                    let result =
                        cli::run_download(url, Some(output_path), connections, cancel, false)
                            .await;
                    sh.abort();
                    result
                })
        }

        Some("sync") => {
            let url = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("Usage: rdm sync <URL> [-o dir] [-c N] [-p N] [--delete] [--ext flac,mp3]"))?
                .clone();
            let (output, connections) = parse_download_args(&args[3..]);
            let connections = connections.unwrap_or(cfg.connections);
            let parallel = parse_parallel_flag(&args[3..]).unwrap_or(cfg.queue_parallel);
            let delete = parse_delete_flag(&args[3..]);
            let ext_filter = parse_ext_filter(&args[3..]);

            if let Some(dir) = output {
                cfg.download_dir = dir;
            }

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());
                    let result =
                        sync::run(&cfg, &url, connections, parallel, delete, ext_filter, cancel)
                            .await;
                    sh.abort();
                    result
                })
        }

        Some("config") => {
            cfg.print();
            Ok(())
        }

        Some("queue") | Some("q") => {
            match args.get(2).map(|s| s.as_str()) {
                Some("add") | Some("a") => {
                    let url = args
                        .get(3)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Usage: rdm queue add <URL> [-o name] [-c N]")
                        })?
                        .clone();
                    let (output, connections) = parse_download_args(&args[4..]);

                    let files = if looks_like_directory(&url) {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?
                            .block_on(scrape::discover_files(&url, true))

                    } else {
                        Ok(None)
                    };

                    match files {
                        Ok(Some(urls)) => {
                            let count = urls.len();
                            queue::Queue::locked(|q| {
                                for f in &urls {
                                    q.add(
                                        f.url.clone(),
                                        Some(f.relative_path.clone()),
                                        connections,
                                    );
                                }
                                Ok(())
                            })?;

                            eprintln!("  📁 Found {} file(s):", count);
                            eprintln!();
                            for f in &urls {
                                eprintln!("     + {}", cli::percent_decode(&f.relative_path));
                            }
                            let q = queue::Queue::load_readonly();
                            eprintln!();
                            eprintln!("  {} item(s) pending.", q.pending_count());
                            Ok(())
                        }
                        
                        Ok(None) if looks_like_directory(&url) => {
                        let resolved = output.map(|o| {
                            let path = Path::new(&o);
                            if path.is_absolute() {
                                o
                            } else {
                                cfg.resolve_output_path(&o)
                            }
                        });
                        let id = queue::Queue::locked(|q| {
                            Ok(q.add(url.clone(), resolved, connections))
                        })?;
                        let q = queue::Queue::load_readonly();
                        eprintln!("  ✅ Added #{}: {}", id, cli::percent_decode(&url));
                        eprintln!("  {} item(s) pending.", q.pending_count());
                        Ok(())
                    }

                        _ => {
                            let resolved = output.map(|o| {
                                let path = Path::new(&o);
                                if path.is_absolute() {
                                    o
                                } else {
                                    cfg.resolve_output_path(&o)
                                }
                            });
                            let id = queue::Queue::locked(|q| {
                                Ok(q.add(url.clone(), resolved, connections))
                            })?;
                            let q = queue::Queue::load_readonly();
                            eprintln!(
                                "  ✅ Added #{}: {}",
                                id,
                                cli::percent_decode(&url)
                            );
                            eprintln!("  {} item(s) pending.", q.pending_count());
                            Ok(())
                        }
                    }
                }

                Some("list") | Some("ls") | Some("l") => {
                    queue::Queue::load_readonly().print_list();
                    Ok(())
                }

                Some("start") | Some("run") | Some("s") => {
                    let parallel =
                        parse_parallel_flag(&args[3..]).unwrap_or(cfg.queue_parallel);

                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()?
                        .block_on(async {
                            let cancel = CancellationToken::new();
                            let sh = signal::spawn_signal_handler(cancel.clone());
                            let result = queue::start(&cfg, cancel, parallel).await;
                            sh.abort();
                            result
                        })
                }

                Some("stop") => {
                    queue::send_signal("stop")?;
                    eprintln!(
                        "  ⏹  Stop signal sent. Queue will stop after current download."
                    );
                    Ok(())
                }

                Some("skip") | Some("next") | Some("n") => {
                    queue::send_signal("skip")?;
                    eprintln!("  ⏭  Skip signal sent.");
                    Ok(())
                }

                Some("remove") | Some("rm") => {
                    let id: u64 = args
                        .get(3)
                        .ok_or_else(|| anyhow::anyhow!("Usage: rdm queue remove <ID>"))?
                        .parse()
                        .map_err(|_| anyhow::anyhow!("Invalid ID — must be a number"))?;
                    let removed = queue::Queue::locked(|q| Ok(q.remove(id)))?;
                    if removed {
                        eprintln!("  Removed #{}", id);
                    } else {
                        eprintln!("  No item with ID #{}", id);
                    }
                    Ok(())
                }

                Some("retry") | Some("r") => match args.get(3).map(|s| s.as_str()) {
                    Some("failed") | Some("f") => {
                        let n = queue::Queue::locked(|q| Ok(q.retry_failed()))?;
                        eprintln!("  Requeued {} failed item(s).", n);
                        Ok(())
                    }
                    Some("skipped") | Some("s") => {
                        let n = queue::Queue::locked(|q| Ok(q.retry_skipped()))?;
                        eprintln!("  Requeued {} skipped item(s).", n);
                        Ok(())
                    }
                    Some(id_str) => {
                        let id: u64 = id_str.parse().map_err(|_| {
                            anyhow::anyhow!(
                                "Usage: rdm queue retry <ID|failed|skipped>"
                            )
                        })?;
                        let ok = queue::Queue::locked(|q| Ok(q.retry_item(id)))?;
                        if ok {
                            eprintln!("  ✅ #{} requeued.", id);
                        } else {
                            eprintln!("  #{} is not failed or skipped.", id);
                        }
                        Ok(())
                    }
                    None => {
                        let n = queue::Queue::locked(|q| {
                            Ok(q.retry_failed() + q.retry_skipped())
                        })?;
                        eprintln!("  Requeued {} item(s).", n);
                        Ok(())
                    }
                },

                Some("clear") | Some("c") => match args.get(3).map(|s| s.as_str()) {
                    Some("pending") | Some("p") => {
                        let n = queue::Queue::locked(|q| Ok(q.clear_pending()))?;
                        eprintln!("  Cleared {} pending item(s).", n);
                        Ok(())
                    }
                    Some("done") | Some("finished") | Some("d") => {
                        let n = queue::Queue::locked(|q| Ok(q.clear_finished()))?;
                        eprintln!("  Cleared {} finished item(s).", n);
                        Ok(())
                    }
                    _ => {
                        let n = queue::Queue::locked(|q| Ok(q.clear_all()))?;
                        eprintln!("  Cleared {} item(s). Queue is empty.", n);
                        Ok(())
                    }
                },

                _ => {
                    eprintln!("RDM — Queue");
                    eprintln!();
                    eprintln!("Usage:");
                    eprintln!(
                        "  rdm queue add <URL> [-o name] [-c N]   Add download"
                    );
                    eprintln!(
                        "  rdm queue list                         Show queue"
                    );
                    eprintln!(
                        "  rdm queue start [-p N]                 Start processing"
                    );
                    eprintln!(
                        "  rdm queue stop                         Stop after current"
                    );
                    eprintln!(
                        "  rdm queue skip                         Skip current download(s)"
                    );
                    eprintln!(
                        "  rdm queue remove <ID>                  Remove item"
                    );
                    eprintln!(
                        "  rdm queue retry [ID|failed|skipped]    Requeue items"
                    );
                    eprintln!(
                        "  rdm queue clear [pending|done]         Clear queue (all by default)"
                    );
                    eprintln!();
                    eprintln!("Shortcuts: q, a, ls, s, n, rm, r, c");
                    Ok(())
                }
            }
        }

        // Quick URL — directory or single file
        Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            let url = url.to_string();
            let (output, connections) = parse_download_args(&args[2..]);
            let connections = connections.unwrap_or(cfg.connections);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());

                    if output.is_none() && looks_like_directory(&url) {
                        match scrape::discover_files(&url, true).await {
                            Ok(Some(files)) => {
                                eprintln!("  📁 Found {} file(s):", files.len());
                                eprintln!();
                                for f in &files {
                                    eprintln!(
                                        "     + {}",
                                        cli::percent_decode(&f.relative_path)
                                    );
                                }
                                eprintln!();

                                queue::Queue::locked(|q| {
                                    for f in &files {
                                        q.add(
                                            f.url.clone(),
                                            Some(f.relative_path.clone()),
                                            Some(connections),
                                        );
                                    }
                                    Ok(())
                                })?;

                                let result =
                                    queue::start(&cfg, cancel, cfg.queue_parallel).await;
                                sh.abort();
                                return result;
                            }
                            Ok(None) => {
                            }
                            _ => {}
                        }
                    }

                    let output_path = resolve_output(output, &url, &cfg);
                    let result = cli::run_download(
                        url,
                        Some(output_path),
                        connections,
                        cancel.clone(),
                        false,
                    )
                    .await;
                    sh.abort();
                    result
                })
        }

        _ => {
            eprintln!("RDM — Rust Download Manager");
            eprintln!();
            eprintln!("Usage:");
            eprintln!(
                "  rdm <URL>                                Quick download"
            );
            eprintln!(
                "  rdm download <URL> [-o name] [-c N]      Download with options"
            );
            eprintln!(
                "  rdm sync <URL> [-d] [-e flac,mkv]        Sync remote → local"
            );
            eprintln!(
                "  rdm queue <command>                      Manage download queue"
            );
            eprintln!(
                "  rdm config                               Show configuration"
            );
            eprintln!();
            eprintln!("Queue commands:");
            eprintln!(
                "  rdm queue add <URL> [-o name] [-c N]     Add to queue"
            );
            eprintln!(
                "  rdm queue list                           Show queue"
            );
            eprintln!(
                "  rdm queue start [-p N]                   Start processing"
            );
            eprintln!(
                "  rdm queue stop / skip                    Live control"
            );
            eprintln!();
            eprintln!("Options:");
            eprintln!(
                "  -c, --connections N    Connections per file (default: {})",
                cfg.connections
            );
            eprintln!(
                "  -o, --output NAME      Output file or directory"
            );
            eprintln!(
                "  -p, --parallel N       Parallel queue downloads (default: {})",
                cfg.queue_parallel
            );
            eprintln!(
                "  -e, --ext EXT,...      Sync: only sync these file extensions"
            );
            eprintln!(
                "  -d, --delete           Sync: remove local files not on remote"
            );
            eprintln!();
            eprintln!("Config: {}", config::config_path().display());
            std::process::exit(1);
        }
    }
}
