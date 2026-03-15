use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio_util::sync::CancellationToken;

use crate::retry::{is_transient_status, TransientError};

#[derive(Debug)]
pub enum DownloadStatus {
    Complete { bytes_written: u64 },
    Cancelled { bytes_on_disk: u64 },
}

pub async fn download_range(
    client: &Client,
    url: &str,
    file_path: &str,
    start: u64,
    end: u64,
    resume_from: u64,
    chunk_progress: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> Result<DownloadStatus> {
    if end < start {
        anyhow::bail!("Invalid byte range: start ({}) > end ({})", start, end);
    }

    let full_chunk_len = end - start + 1;

    if resume_from >= full_chunk_len {
        return Ok(DownloadStatus::Complete { bytes_written: 0 });
    }

    let effective_start = start + resume_from;
    let expected_len = end - effective_start + 1;
    let range_value = format!("bytes={}-{}", effective_start, end);

    let response = tokio::select! {
        biased;

        _ = cancel.cancelled() => {
            return Ok(DownloadStatus::Cancelled {
                bytes_on_disk: resume_from
            });
        }

        result = client
            .get(url)
            .header(header::RANGE, &range_value)
            .send() =>
        {
            result.with_context(|| format!("Range GET failed for {}", range_value))?
        }
    };

    let status = response.status();

    if status == StatusCode::OK && effective_start == 0 {
        // Server doesn't support ranges but we're downloading from the start.
    } else if status == StatusCode::OK && effective_start > 0 {
        anyhow::bail!(
            "Server does not support range requests — cannot resume from byte {}",
            effective_start,
        );
    } else if status != StatusCode::PARTIAL_CONTENT {
        if is_transient_status(status) {
            return Err(anyhow::Error::new(TransientError {
                message: format!("Transient HTTP {} for range {}", status.as_u16(), range_value),
            }));
        }
        anyhow::bail!(
            "Permanent HTTP error for range {}: {} {}",
            range_value, status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown"),
        );
    } else {
        validate_content_range(response.headers(), effective_start, end)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .await
        .with_context(|| format!("Failed to open file: {}", file_path))?;

    let mut file = BufWriter::with_capacity(2 * 512 * 1024, file);

    file.seek(SeekFrom::Start(effective_start))
        .await
        .with_context(|| format!("Failed to seek to offset {}", effective_start))?;

    let mut stream = response.bytes_stream();
    let mut bytes_written: u64 = 0;
    let mut bytes_since_flush: u64 = 0;

        loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = cancel.cancelled() => {
                file.flush().await.ok();
                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                return Ok(DownloadStatus::Cancelled {
                    bytes_on_disk: resume_from + bytes_written,
                });
            }
        };

        match chunk {
            Some(Ok(data)) => {
                let data_len: u64 = data.len() as u64;

                if bytes_written + data_len > expected_len {
                    file.flush().await.ok();
                    chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                    drop(stream);
                    anyhow::bail!(
                        "Server sent excess data for range {}: expected {} bytes, got at least {}",
                        range_value, expected_len, bytes_written + data_len,
                    );
                }

                file.write_all(&data)
                    .await
                    .with_context(|| {
                        format!("Write failed at offset {} in {}", effective_start + bytes_written, file_path)
                    })?;

                bytes_written += data_len;
                bytes_since_flush += data_len;

                if bytes_since_flush >= 16 * 1024 * 1024 {
                    file.flush().await.with_context(|| {
                        format!("Periodic flush failed at offset {}", effective_start + bytes_written)
                    })?;
                    bytes_since_flush = 0;
                }

                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
            }
            Some(Err(e)) => {
                file.flush().await.ok();
                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                return Err(e).context(format!(
                    "Stream error at byte {} of range {}", bytes_written, range_value,
                ));
            }
            None => break,
        }
    }

    file.flush().await.context("Failed to flush file after range write")?;

    if bytes_written != expected_len {
        chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
        anyhow::bail!(
            "Truncated range {}: expected {} bytes, wrote {}", range_value, expected_len, bytes_written,
        );
    }

    chunk_progress.store(full_chunk_len, Ordering::SeqCst);
    Ok(DownloadStatus::Complete { bytes_written })
}

fn validate_content_range(headers: &header::HeaderMap, expected_start: u64, expected_end: u64) -> Result<()> {
    let value = headers.get(header::CONTENT_RANGE).context("Server returned 206 without Content-Range")?
        .to_str().context("Content-Range is not valid UTF-8")?;

    let rest = value.strip_prefix("bytes ")
        .with_context(|| format!("Unexpected Content-Range format: '{}'", value))?;

    let (range_part, _) = rest.split_once('/')
        .with_context(|| format!("Content-Range missing '/': '{}'", value))?;

    let dash = range_part.find('-').with_context(|| format!("Content-Range missing '-': '{}'", value))?;

    let actual_start: u64 = range_part[..dash].parse()
        .with_context(|| format!("Invalid start in Content-Range: '{}'", value))?;
    let actual_end: u64 = range_part[dash + 1..].parse()
        .with_context(|| format!("Invalid end in Content-Range: '{}'", value))?;

    if actual_start != expected_start || actual_end != expected_end {
        anyhow::bail!("Content-Range mismatch: requested {}-{}, got '{}'", expected_start, expected_end, value);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_valid_content_range() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_RANGE, HeaderValue::from_static("bytes 0-999/8000"));
        assert!(validate_content_range(&h, 0, 999).is_ok());
    }

    #[test]
    fn test_resumed_content_range() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_RANGE, HeaderValue::from_static("bytes 500-999/8000"));
        assert!(validate_content_range(&h, 500, 999).is_ok());
    }

    #[test]
    fn test_content_range_mismatch() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/8000"));
        assert!(validate_content_range(&h, 0, 999).is_err());
    }

    #[test]
    fn test_content_range_wildcard() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_RANGE, HeaderValue::from_static("bytes 100-199/*"));
        assert!(validate_content_range(&h, 100, 199).is_ok());
    }

    #[test]
    fn test_content_range_missing() {
        assert!(validate_content_range(&HeaderMap::new(), 0, 99).is_err());
    }

    #[test]
    fn test_content_range_bad_prefix() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_RANGE, HeaderValue::from_static("octets 0-99/100"));
        assert!(validate_content_range(&h, 0, 99).is_err());
    }
}
