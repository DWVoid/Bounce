use std::hash::{Hash, Hasher};
use std::sync::Arc;
use futures::SinkExt;
use iced::widget::{button, column, container, row, scrollable, text, text_input, Column};
use iced::{Element, Length, Subscription, Task};
use crate::types::{ProxyEntry, ProxyEvent};

/// Passed to `Subscription::run_with`; hashed by `id` so the subscription
/// is replaced whenever the server is restarted.
struct SubData {
    id: u64,
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<ProxyEvent>>>,
}

impl Hash for SubData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

fn build_event_stream(
    data: &SubData,
) -> futures::stream::BoxStream<'static, Message> {
    let rx = Arc::clone(&data.rx);
    Box::pin(iced::stream::channel(
        100,
        move |mut sender: futures::channel::mpsc::Sender<Message>| async move {
            loop {
                let event = rx.lock().await.recv().await;
                match event {
                    Some(e) => {
                        if sender.send(Message::ProxyEvent(e)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        },
    ))
}

pub struct App {
    upstream_url: String,
    bind_addr: String,
    bind_port: String,
    server_running: bool,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    event_rx: Option<Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<ProxyEvent>>>>,
    subscription_id: u64,
    entries: Vec<ProxyEntry>,
    selected_entry: Option<usize>,
    status_message: String,
}

#[derive(Debug, Clone)]
pub enum Message {
    UpstreamUrlChanged(String),
    BindAddrChanged(String),
    BindPortChanged(String),
    StartServer,
    StopServer,
    SelectEntry(usize),
    ProxyEvent(ProxyEvent),
}

impl App {
    pub fn new() -> (Self, Task<Message>) {
        (
            App {
                upstream_url: "http://localhost:8080".to_string(),
                bind_addr: "0.0.0.0".to_string(),
                bind_port: "3000".to_string(),
                server_running: false,
                stop_sender: None,
                event_rx: None,
                subscription_id: 0,
                entries: Vec::new(),
                selected_entry: None,
                status_message: "Ready".to_string(),
            },
            Task::none(),
        )
    }

    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::UpstreamUrlChanged(v) => self.upstream_url = v,
            Message::BindAddrChanged(v) => self.bind_addr = v,
            Message::BindPortChanged(v) => self.bind_port = v,
            Message::StartServer => {
                let port: u16 = match self.bind_port.parse() {
                    Ok(p) => p,
                    Err(_) => {
                        self.status_message =
                            format!("Invalid port '{}', using 3000", self.bind_port);
                        self.bind_port = "3000".to_string();
                        3000
                    }
                };
                let config = crate::types::ProxyConfig {
                    upstream_url: self.upstream_url.clone(),
                    bind_addr: self.bind_addr.clone(),
                    bind_port: port,
                };
                let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
                let (event_tx, event_rx) = tokio::sync::mpsc::channel(256);
                self.stop_sender = Some(stop_tx);
                self.event_rx = Some(Arc::new(tokio::sync::Mutex::new(event_rx)));
                self.subscription_id += 1;
                self.server_running = true;
                self.status_message = "Starting…".to_string();
                tokio::spawn(crate::proxy::run_proxy(config, event_tx, stop_rx));
            }
            Message::StopServer => {
                if let Some(tx) = self.stop_sender.take() {
                    let _ = tx.send(());
                }
                self.server_running = false;
                self.status_message = "Stopped".to_string();
            }
            Message::SelectEntry(id) => {
                self.selected_entry = Some(id);
            }
            Message::ProxyEvent(event) => match event {
                ProxyEvent::Entry(entry) => {
                    self.entries.push(*entry);
                }
                ProxyEvent::Error(e) => {
                    self.status_message = format!("Error: {e}");
                    self.server_running = false;
                }
                ProxyEvent::Started => {
                    self.status_message = format!(
                        "Listening on {}:{}",
                        self.bind_addr, self.bind_port
                    );
                }
                ProxyEvent::Stopped => {
                    self.status_message = "Stopped".to_string();
                    self.server_running = false;
                }
            },
        }
        Task::none()
    }

    pub fn view(&self) -> Element<'_, Message> {
        // ── Config row ─────────────────────────────────────────────────────
        let upstream_input = text_input("http://localhost:8080", &self.upstream_url)
            .on_input(Message::UpstreamUrlChanged)
            .padding(5);
        let addr_input = text_input("0.0.0.0", &self.bind_addr)
            .on_input(Message::BindAddrChanged)
            .padding(5);
        let port_input = text_input("3000", &self.bind_port)
            .on_input(Message::BindPortChanged)
            .padding(5)
            .width(80);

        let start_btn = if !self.server_running {
            button("Start").on_press(Message::StartServer).padding(5)
        } else {
            button("Start").padding(5)
        };
        let stop_btn = if self.server_running {
            button("Stop").on_press(Message::StopServer).padding(5)
        } else {
            button("Stop").padding(5)
        };

        let config_row = row![
            text("Upstream:").size(14),
            upstream_input,
            text("Bind:").size(14),
            addr_input,
            text(":").size(14),
            port_input,
            start_btn,
            stop_btn,
            text(&self.status_message).size(14),
        ]
        .spacing(8)
        .align_y(iced::Alignment::Center);

        // ── Request list (left pane) ────────────────────────────────────────
        let entry_list: Column<Message> =
            self.entries.iter().fold(column![].spacing(2), |col, entry| {
                let label = format!(
                    "[{}] {} {} {}",
                    entry.id, entry.method, entry.path, entry.response_status
                );
                let selected = self.selected_entry == Some(entry.id);
                let btn = button(text(label).size(13))
                    .on_press(Message::SelectEntry(entry.id))
                    .padding(4)
                    .width(Length::Fill);
                if selected {
                    col.push(
                        container(btn)
                            .style(container::rounded_box)
                            .width(Length::Fill),
                    )
                } else {
                    col.push(btn)
                }
            });

        let left_pane = container(
            scrollable(entry_list.width(Length::Fill)).height(Length::Fill),
        )
        .width(Length::FillPortion(2))
        .height(Length::Fill)
        .padding(5);

        // ── Detail pane (right pane) ────────────────────────────────────────
        let detail: Element<Message> = if let Some(sel_id) = self.selected_entry {
            if let Some(entry) = self.entries.iter().find(|e| e.id == sel_id) {
                let req_headers_text = entry
                    .request_headers
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let resp_headers_text = entry
                    .response_headers
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join("\n");

                scrollable(
                    column![
                        text("Request Headers").size(14),
                        container(text(req_headers_text).size(12)).padding(5),
                        text("Request Body").size(14),
                        container(text(entry.request_body.clone()).size(12)).padding(5),
                        text("Response Headers").size(14),
                        container(text(resp_headers_text).size(12)).padding(5),
                        text("Response Body").size(14),
                        container(text(entry.response_body.clone()).size(12)).padding(5),
                    ]
                    .spacing(6)
                    .padding(5),
                )
                .height(Length::Fill)
                .into()
            } else {
                text("Select a request to inspect").into()
            }
        } else {
            text("Select a request to inspect").into()
        };

        let right_pane = container(detail)
            .width(Length::FillPortion(3))
            .height(Length::Fill)
            .padding(5);

        // ── Full layout ─────────────────────────────────────────────────────
        column![
            config_row,
            row![left_pane, right_pane]
                .height(Length::Fill)
                .spacing(5),
        ]
        .spacing(8)
        .padding(10)
        .into()
    }

    pub fn subscription(&self) -> Subscription<Message> {
        if let Some(rx) = &self.event_rx {
            let data = SubData {
                id: self.subscription_id,
                rx: Arc::clone(rx),
            };
            Subscription::run_with(data, build_event_stream)
        } else {
            Subscription::none()
        }
    }
}

pub fn run_app() -> iced::Result {
    iced::application(App::new, App::update, App::view)
        .title(|_: &App| String::from("Bounce - HTTP Reverse Proxy"))
        .subscription(App::subscription)
        .run()
}
