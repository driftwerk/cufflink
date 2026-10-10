use crate::config::CliConfig;
use serde_json::Value;
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone, Default)]
pub(crate) struct Asked(Arc<Mutex<Vec<(String, Value)>>>);

impl Asked {
    fn recorded(&self) -> Vec<(String, Value)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn requests(&self) -> Vec<String> {
        self.recorded()
            .into_iter()
            .map(|(asked, _)| asked)
            .collect()
    }

    pub(crate) fn bodies(&self) -> Vec<Value> {
        self.recorded().into_iter().map(|(_, body)| body).collect()
    }

    pub(crate) fn count(&self) -> usize {
        self.recorded().len()
    }

    fn record(&self, request: &[u8]) {
        let (head, body) =
            head_end(request).map_or((request, &[][..]), |end| request.split_at(end));
        let asked = String::from_utf8_lossy(head)
            .lines()
            .next()
            .and_then(|line| line.rsplit_once(' '))
            .map(|(asked, _)| asked.to_string())
            .unwrap_or_default();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((asked, serde_json::from_slice(body).unwrap_or(Value::Null)));
    }
}

fn head_end(request: &[u8]) -> Option<usize> {
    request
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|head| head + 4)
}

fn content_length(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, length)| length.trim().parse().ok())
        .unwrap_or(0)
}

pub(crate) async fn answering(answers: Vec<Option<(u16, Value)>>) -> (CliConfig, Asked) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let asked = Asked::default();
    let recorded = asked.clone();
    tokio::spawn(async move {
        for answer in answers {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            let read = |request: &[u8]| {
                head_end(request)
                    .is_some_and(|end| request.len() >= end + content_length(&request[..end]))
            };
            while !read(&request) {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => request.extend_from_slice(&buffer[..n]),
                }
            }
            recorded.record(&request);
            if let Some((status, body)) = answer {
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        }
    });
    let config = CliConfig {
        api_url: format!("http://{address}"),
        tenant_slug: "acme".into(),
        api_key: Some("key".into()),
        env_name: None,
        keycloak_url: None,
        keycloak_realm: None,
        keycloak_client_id: None,
        tenant_override: None,
    };
    (config, asked)
}
