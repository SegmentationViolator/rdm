use anyhow::{Context, Result};
use reqwest::{header, Client, StatusCode};

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub size: Option<u64>,
    pub supports_range: bool,
    pub suggested_filename: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl FileInfo {
    pub fn can_chunk(&self) -> bool {
        self.size.is_some() && self.supports_range
    }
}

fn filename_from_content_disposition(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let val = headers.get("content-disposition")?.to_str().ok()?;

    // Try filename*=UTF-8''name first (RFC 5987)
    if let Some(pos) = val.find("filename*=UTF-8''") {
        let name = &val[pos + 17..];
        let name = name.split(';').next().unwrap_or(name).trim();
        let decoded = crate::cli::percent_decode(name);
        if !decoded.is_empty() {
            return Some(decoded);
        }
    }

    // Fallback: filename="name" or filename=name
    if let Some(pos) = val.find("filename=") {
        let name = &val[pos + 9..];
        let name = name.split(';').next().unwrap_or(name).trim();
        let name = name.trim_matches('"');
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }

    None
}

fn extract_etag(headers: &header::HeaderMap) -> Option<String> {
    headers.get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn extract_last_modified(headers: &header::HeaderMap) -> Option<String> {
    headers.get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

pub async fn inspect_url(client: &Client, url: &str) -> Result<FileInfo> {
    let resp = client
        .get(url)
        .header(header::RANGE, "bytes=0-0")
        .send()
        .await
        .context("Request failed — check the URL and your network connection")?;

    let status = resp.status();
    let suggested_filename = filename_from_content_disposition(resp.headers());
    let etag = extract_etag(resp.headers());
    let last_modified = extract_last_modified(resp.headers());

    if status == StatusCode::PARTIAL_CONTENT {
        let size = extract_size_from_content_range(resp.headers())
            .or_else(|| extract_content_length(resp.headers()));
        return Ok(FileInfo {
            size,
            supports_range: true,
            suggested_filename,
            etag,
            last_modified,
        });
    }

    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        let size = extract_size_from_content_range(resp.headers());
        return Ok(FileInfo {
            size,
            supports_range: false,
            suggested_filename,
            etag,
            last_modified,
        });
    }

    if status.is_success() {
        let size = extract_content_length(resp.headers());
        return Ok(FileInfo {
            size,
            supports_range: false,
            suggested_filename,
            etag,
            last_modified,
        });
    }

    // Some servers reject GET with Range — fall back to HEAD
    let head_resp = client
        .head(url)
        .send()
        .await
        .context("HEAD request failed — check the URL and your network connection")?;

    let head_status = head_resp.status();
    if !head_status.is_success() {
        anyhow::bail!(
            "Server returned non-success status: {} {}",
            head_status.as_u16(),
            head_status.canonical_reason().unwrap_or("Unknown")
        );
    }

    let size = extract_content_length(head_resp.headers());

    let supports_range = head_resp
        .headers()
        .get(header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("bytes"))
        .unwrap_or(false);

    let head_etag = extract_etag(head_resp.headers());
    let head_last_modified = extract_last_modified(head_resp.headers());

    Ok(FileInfo {
        size,
        supports_range,
        suggested_filename,
        etag: head_etag,
        last_modified: head_last_modified,
    })
}

fn extract_content_length(headers: &header::HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&len| len > 0)
}

fn extract_size_from_content_range(headers: &header::HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.rsplit('/').next())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&len| len > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_extract_content_length_present() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("12345"));
        assert_eq!(extract_content_length(&headers), Some(12345));
    }

    #[test]
    fn test_extract_content_length_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_extract_content_length_zero() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_extract_content_length_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("abc"));
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_extract_size_from_content_range() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-0/98765"),
        );
        assert_eq!(extract_size_from_content_range(&headers), Some(98765));
    }

    #[test]
    fn test_extract_size_from_content_range_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_size_from_content_range(&headers), None);
    }

    #[test]
    fn test_extract_size_from_content_range_star() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes */0"),
        );
        assert_eq!(extract_size_from_content_range(&headers), None);
    }

    #[test]
    fn test_file_info_can_chunk() {
        let info = FileInfo { size: Some(1024), supports_range: true, suggested_filename: None, etag: None, last_modified: None };
        assert!(info.can_chunk());

        let info = FileInfo { size: None, supports_range: true, suggested_filename: None, etag: None, last_modified: None };
        assert!(!info.can_chunk());

        let info = FileInfo { size: Some(1024), supports_range: false, suggested_filename: None, etag: None, last_modified: None };
        assert!(!info.can_chunk());
    }
}
