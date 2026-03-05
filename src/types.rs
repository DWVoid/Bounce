use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct ProxyEntry {
    pub id: usize,
    pub method: String,
    pub path: String,
    pub request_headers: Vec<(String, String)>,
    pub request_body: String,
    pub response_status: u16,
    pub response_headers: Vec<(String, String)>,
    pub response_body: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub upstream_url: String,
    pub bind_addr: String,
    pub bind_port: u16,
}

#[derive(Debug, Clone)]
pub enum ProxyEvent {
    Entry(Box<ProxyEntry>),
    Error(String),
    Started,
    Stopped,
}
