use chrono::{DateTime, Utc};

/// Resolve sender name for display.
pub fn resolve_sender(current_login: &str, remote_login: &str) -> String {
    if current_login == remote_login {
        "You".to_string()
    } else {
        remote_login.to_string()
    }
}

/// Format a Unix timestamp as "HH:MM" for use inside chat message bubbles.
pub fn format_timestamp_bubble(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map_or_else(
            || "??:??".to_string(),
            |dt| dt.format("%H:%M").to_string(),
        )
}

/// Return a date separator label for chat messages.
/// - "Today" if the timestamp is today
/// - "Yesterday" if the timestamp is yesterday
/// - "DD/MM/YYYY" for any other date
pub fn format_date_separator(timestamp: i64) -> String {
    let now = Utc::now().naive_utc().date();
    let msg_date = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map_or_else(|| now, |dt| dt.naive_utc().date());

    let diff = now.signed_duration_since(msg_date).num_days();

    match diff {
        0 => "Today".to_string(),
        1 => "Yesterday".to_string(),
        _ => msg_date.format("%d/%m/%Y").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{format_timestamp_bubble, format_date_separator};

    #[test]
    fn bubble_uses_only_time() {
        let result = format_timestamp_bubble(1700000000);
        // Should be HH:MM format, 5 chars
        assert_eq!(result.len(), 5);
        assert_eq!(result.chars().nth(2), Some(':'));
    }

    #[test]
    fn date_separator_returns_string() {
        let now = chrono::Utc::now().timestamp();
        let sep = format_date_separator(now);
        assert_eq!(sep, "Today");
    }

    #[test]
    fn date_separator_yesterday() {
        let yesterday = chrono::Utc::now().timestamp() - 86400;
        let sep = format_date_separator(yesterday);
        assert_eq!(sep, "Yesterday");
    }

    #[test]
    fn date_separator_old_date() {
        let old = 1609459200i64; // 2021-01-01
        let sep = format_date_separator(old);
        assert_eq!(sep, "01/01/2021");
    }
}
