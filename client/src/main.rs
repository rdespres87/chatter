mod app;
mod events;

use app::App;
use clap::Parser;

/// Client CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "chatter-client", about = "WebSocket chat TUI client")]
struct Cli {
    /// Server URL (e.g. ws://localhost:8080).
    #[arg(long, default_value = "ws://localhost:8080")]
    url: String,

    /// Server port (overrides the port in --url if provided).
    #[arg(short, long)]
    port: Option<u16>,

    /// Default username for login.
    #[arg(long)]
    user: Option<String>,
}

/// Guard that restores the terminal on drop (including panic unwind).
/// Without this, a panic during `run()` would leave the TTY in raw mode
/// with no echo or line editing — a broken terminal for the user.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = ratatui::restore();
    }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let cli = Cli::parse();
    color_eyre::install()?;

    // Keep logs minimal for the TUI client — only warnings and errors.
    // Sending log::info! or log::error! to stderr would leak text outside
    // the alternate screen (which only redirects stdout) and clutter
    // the terminal.  Prefer log-to-file for production diagnostics.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Warn)
        .init();

    let mut url = cli.url;

    // If --port is provided, override the port in the URL.
    if let Some(port) = cli.port {
        // Replace the port in the URL string.
        // Handle both ws://host:port and wss://host:port formats.
        if let Some(pos) = url.find("://") {
            let scheme_end = pos + 3;
            if let Some(host_start) = url[scheme_end..].find(':') {
                let host_part_end = scheme_end + host_start;
                // Find the end of the port number (next '/' or end of string).
                if let Some(path_start) = url[host_part_end..].find('/') {
                    let path_pos = host_part_end + path_start;
                    url = format!("{}:{}{}", &url[..host_part_end], port, &url[path_pos..]);
                } else {
                    url = format!("{}:{}", &url[..host_part_end], port);
                }
            } else {
                // No port in URL — append one.
                if let Some(pos) = url.find('/') {
                    url = format!("{}:{}{}", &url[..pos], port, &url[pos..]);
                } else {
                    url = format!("{}:{}", url, port);
                }
            }
        }
    }

    // Connect BEFORE entering raw mode: a slow or failed connection
    // must never leave the user staring at an unresponsive TUI.
    let app = App::new(url, cli.user).await?;

    // Initialize the terminal — enters raw mode + alternate screen.
    let terminal = ratatui::init();

    // Clear the screen before first draw for terminals that don't
    // auto-clear on alternate screen entry.
    //terminal.clear()?;

    // Safety net: if run() panics, TerminalGuard::drop restores
    // the terminal even if the terminal's own Drop does not fire.
    let _guard = TerminalGuard;

    // Run the app — terminal is consumed, its Drop calls restore().
    let result = app.run(terminal).await;

    result
}
