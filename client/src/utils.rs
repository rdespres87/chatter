use chrono::TimeZone;

/// Resolve sender name for display.
pub fn resolve_sender(current_login: &str, remote_login: &str) -> String {
    if current_login == remote_login {
        "You".to_string()
    } else {
        remote_login.to_string()
    }
}

/// Format a Unix timestamp into a human-readable string.
pub fn format_timestamp(_timestamp: i64) -> String {
    chrono::Local
        .timestamp_opt(_timestamp, 0)
        .single()
        .map_or_else(
            || "unknown time".to_string(),
            |time| time.format("%Y-%m-%d %H:%M:%S").to_string(),
        )
}

#[cfg(test)]
mod tests {
    use super::format_timestamp;

    #[test]
    fn timestamp_uses_the_supplied_unix_time() {
        assert_ne!(format_timestamp(0), "unknown time");
    }
}
