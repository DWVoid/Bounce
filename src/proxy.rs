use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use http_body_util::combinators::BoxBody;
use hyper::{Request, Response};
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::net::TcpListener;
use crate::types::{ProxyConfig, ProxyEntry, ProxyEvent};

const MAX_BODY: usize = 1024 * 1024; // 1 MB

fn bytes_to_string(b: &Bytes) -> String {
    if b.len() > MAX_BODY {
        format!("[truncated {} bytes]", b.len())
    } else {
        String::from_utf8_lossy(b).to_string()
    }
}

fn boxed_full(body: Full<Bytes>) -> BoxBody<Bytes, hyper::Error> {
    body.map_err(|never| match never {}).boxed()
}

pub async fn run_proxy(
    config: ProxyConfig,
    event_tx: tokio::sync::mpsc::Sender<ProxyEvent>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let addr = format!("{}:{}", config.bind_addr, config.bind_port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = event_tx.send(ProxyEvent::Error(format!("Bind error: {e}"))).await;
            return;
        }
    };
    let _ = event_tx.send(ProxyEvent::Started).await;

    let upstream_url = Arc::new(config.upstream_url.clone());
    let client: Arc<Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>> =
        Arc::new(Client::builder(TokioExecutor::new()).build_http());
    let counter = Arc::new(AtomicUsize::new(0));

    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                let _ = event_tx.send(ProxyEvent::Stopped).await;
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let io = TokioIo::new(stream);
                        let upstream_url = Arc::clone(&upstream_url);
                        let client = Arc::clone(&client);
                        let event_tx = event_tx.clone();
                        let counter = Arc::clone(&counter);

                        tokio::spawn(async move {
                            let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                                let upstream_url = Arc::clone(&upstream_url);
                                let client = Arc::clone(&client);
                                let event_tx = event_tx.clone();
                                let id = counter.fetch_add(1, Ordering::Relaxed);
                                async move {
                                    handle_request(req, upstream_url, client, event_tx, id).await
                                }
                            });
                            let _ = AutoBuilder::new(TokioExecutor::new())
                                .serve_connection(io, svc)
                                .await;
                        });
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn error_response(msg: String) -> Response<BoxBody<Bytes, hyper::Error>> {
    Response::builder()
        .status(502)
        .body(boxed_full(Full::new(Bytes::from(msg))))
        .unwrap()
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    upstream_url: Arc<String>,
    client: Arc<Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>>,
    event_tx: tokio::sync::mpsc::Sender<ProxyEvent>,
    id: usize,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, std::convert::Infallible> {
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return Ok(error_response(format!("Body read error: {e}"))),
    };

    let method = parts.method.to_string();
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let req_headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("<binary>").to_string()))
        .collect();
    let req_body_str = bytes_to_string(&body_bytes);

    let upstream_uri = format!("{}{}", upstream_url, path);

    let mut upstream_builder = Request::builder()
        .method(&parts.method)
        .uri(&upstream_uri[..]);
    for (name, value) in parts.headers.iter() {
        if name != hyper::header::HOST {
            upstream_builder = upstream_builder.header(name, value);
        }
    }
    if let Ok(upstream_host) = upstream_url.parse::<hyper::Uri>() {
        if let Some(authority) = upstream_host.authority() {
            upstream_builder =
                upstream_builder.header(hyper::header::HOST, authority.as_str());
        }
    }
    let upstream_req = match upstream_builder.body(Full::new(body_bytes)) {
        Ok(r) => r,
        Err(e) => return Ok(error_response(format!("Request build error: {e}"))),
    };

    let response = match client.request(upstream_req).await {
        Ok(r) => r,
        Err(e) => return Ok(error_response(format!("Upstream error: {e}"))),
    };
    let status = response.status().as_u16();
    let resp_headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("<binary>").to_string()))
        .collect();
    let (resp_parts, resp_body) = response.into_parts();
    let resp_bytes = match resp_body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return Ok(error_response(format!("Response body error: {e}"))),
    };
    let resp_body_str = bytes_to_string(&resp_bytes);

    let entry = ProxyEntry {
        id,
        method,
        path,
        request_headers: req_headers,
        request_body: req_body_str,
        response_status: status,
        response_headers: resp_headers,
        response_body: resp_body_str,
        timestamp: std::time::SystemTime::now(),
    };
    let _ = event_tx.send(ProxyEvent::Entry(Box::new(entry))).await;

    Ok(Response::from_parts(
        resp_parts,
        boxed_full(Full::new(resp_bytes)),
    ))
}
