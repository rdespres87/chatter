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
    if let Some(port) = args.port
        && let Some(pos) = url.find("://")
    {
        let scheme_end = pos + 3;
        if let Some(host_start) = url[scheme_end..].find(':') {
            let host_part_end = scheme_end + host_start;
            if let Some(path_start) = url[host_part_end..].find('/') {
                let path_pos = host_part_end + path_start;
                url = format!("{}:{}{}", &url[..host_part_end], port, &url[path_pos..]);
            } else {
                url = format!("{}:{}", &url[..host_part_end], port);
            }
        } else if let Some(pos) = url.find('/') {
            url = format!("{}:{}{}", &url[..pos], port, &url[pos..]);
        } else {
            url = format!("{}:{}", url, port);
        }
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

struct AppWrapper {
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
