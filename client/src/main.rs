//! WebSocket chat client with a desktop GUI built on egui.
//!
//! The client connects to the server via WebSocket, handles authentication
//! (login/register), room management, real-time messaging, and automatic
//! reconnection with exponential backoff.

mod app;
mod events;
mod utils;

use app::App;
use clap::Parser;
use std::sync::{Arc, Mutex};

/// Client CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "chatter-client", about = "WebSocket chat client")]
struct Args {
    /// Server URL (e.g. ws://localhost:8080).
    #[arg(long, default_value = "ws://localhost:8080")]
    url: String,

    /// Server port (overrides the port in --url if provided).
    #[arg(short, long)]
    port: Option<u16>,
}

fn main() -> eframe::Result<()> {
    color_eyre::install().ok();

    // Keep logs minimal — only warnings and errors.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Warn)
        .init();

    let args = Args::parse();
    let mut url = args.url;

    // If --port is provided, override the port in the URL.
    if let Some(port) = args.port {
        url = override_url_port(&url, port);
    }

    // Load the app icon from disk (relative to client/ crate root).
    let icon_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("assets")
        .join("icon.rgba");
    let icon_bytes = std::fs::read(&icon_path).unwrap_or_default();
    let icon_data = egui::IconData {
        rgba: icon_bytes,
        width: 512,
        height: 512,
    };

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_resizable(true)
            .with_title("chatter")
            .with_icon(icon_data),
        ..Default::default()
    };

    let runtime = tokio::runtime::Runtime::new().expect("failed to create Tokio runtime");
    let _runtime_guard = runtime.enter();
    let app = tokio::runtime::Handle::current().block_on(App::new(url));
    let app = Arc::new(Mutex::new(app));

    eframe::run_native(
        "chatter",
        options,
        Box::new(move |_cc| Ok(Box::new(AppWrapper { inner: app.clone() }))),
    )
}

/// Wraps `App` in an `Arc<Mutex<>>` for `eframe::App` trait implementation.
struct AppWrapper {
    /// Shared mutable application state.
    inner: Arc<Mutex<App>>,
}

impl eframe::App for AppWrapper {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Ok(mut app) = self.inner.lock() {
            app.logic(ctx);
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        if let Ok(mut app) = self.inner.lock() {
            app.ui(ui, frame);
        }
    }
}

/// Extract URL port override logic into a testable function.
fn override_url_port(url: &str, port: u16) -> String {
    let mut url = url.to_owned();
    if let Some(scheme_end) = url.find("://").map(|i| i + 3) {
        // Find the port separator ":" after the scheme.
        // Handle IPv6: skip past "]".
        let host_part = &url[scheme_end..];
        let colon_pos = if let Some(bracket_end) = host_part.find(']') {
            // IPv6 address — look for ":" after the closing bracket.
            host_part[bracket_end..].find(':').map(|i| bracket_end + i)
        } else {
            // IPv4 or hostname — look for first ":".
            host_part.find(':')
        };

        if let Some(colon_pos) = colon_pos {
            let colon_abs = scheme_end + colon_pos;
            // Replace from colon to the first "/" (or end of string).
            let path_start = url[colon_abs..].find('/').map(|i| i + colon_abs);
            match path_start {
                Some(end) => {
                    url = format!("{}:{}{}", &url[..colon_abs], port, &url[end..]);
                }
                None => {
                    url = format!("{}:{}", &url[..colon_abs], port);
                }
            }
        } else {
            // No port in URL — append ":port" after host.
            let path_start = url[scheme_end..].find('/').map(|i| i + scheme_end);
            match path_start {
                Some(end) => {
                    url = format!("{}:{}{}", &url[..end], port, &url[end..]);
                }
                None => {
                    url = format!("{}:{}", url, port);
                }
            }
        }
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_port_replaces_existing_port() {
        assert_eq!(
            override_url_port("ws://localhost:8080", 9090),
            "ws://localhost:9090"
        );
    }

    #[test]
    fn override_port_with_path() {
        assert_eq!(
            override_url_port("ws://localhost:8080/chat", 9090),
            "ws://localhost:9090/chat"
        );
    }

    #[test]
    fn override_port_with_wss() {
        assert_eq!(
            override_url_port("wss://example.com:443/app", 8443),
            "wss://example.com:8443/app"
        );
    }

    #[test]
    fn override_port_without_existing_port() {
        assert_eq!(
            override_url_port("ws://localhost/chat", 9090),
            "ws://localhost:9090/chat"
        );
    }

    #[test]
    fn override_port_no_path_no_existing_port() {
        assert_eq!(
            override_url_port("ws://localhost", 9090),
            "ws://localhost:9090"
        );
    }

    #[test]
    fn override_port_with_ipv6() {
        assert_eq!(
            override_url_port("ws://[::1]:8080", 9090),
            "ws://[::1]:9090"
        );
    }

    #[test]
    fn no_port_override_unchanged() {
        assert_eq!(
            override_url_port("ws://localhost:8080", 0),
            "ws://localhost:0"
        );
    }
}
