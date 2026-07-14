mod app;
mod events;

use app::App;

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
    color_eyre::install()?;

    // Keep logs minimal for the TUI client — only warnings and errors.
    // Sending log::info! or log::error! to stderr would leak text outside
    // the alternate screen (which only redirects stdout) and clutter
    // the terminal.  Prefer log-to-file for production diagnostics.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Warn)
        .init();

    let url = "ws://localhost:8080".to_string();

    // Connect BEFORE entering raw mode: a slow or failed connection
    // must never leave the user staring at an unresponsive TUI.
    let app = App::new(url).await?;

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
