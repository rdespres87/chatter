use anyhow::{Context, Result, bail};
use bcrypt::{DEFAULT_COST, hash, verify};
use rusqlite::Connection;
use std::sync::{Arc, MutexGuard};
use zeroize::Zeroize;

const DUMMY_BCRYPT_HASH: &str = "$2b$12$C6UzMDM.H6dfI/f/IKcEe.7kcMRTwcbP4fjUu6zIlrC7L3Od1h7dW";

/// Login used by the server as the "not authenticated yet" sentinel on peers.
/// It must never become a real account name.
pub const RESERVED_ANONYMOUS_LOGIN: &str = "anonymous";

/// Thread-safe wrapper around a SQLite connection.
#[derive(Clone)]
pub struct Account {
    connection: Arc<std::sync::Mutex<Connection>>,
}

impl Account {
    pub fn new(database_name: String) -> Result<Account> {
        let conn = Connection::open(&database_name)
            .with_context(|| format!("Failed to open database '{}'", database_name))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS account
            (name TEXT NOT NULL PRIMARY KEY, passwd TEXT NOT NULL)",
            [],
        )?;

        // Messages table with room, sender, content and Unix timestamp.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
             room TEXT NOT NULL,
             sender TEXT NOT NULL,
             content TEXT NOT NULL,
             timestamp INTEGER NOT NULL DEFAULT (unixepoch()))",
            [],
        )?;

        Ok(Self {
            connection: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    fn lock_connection(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn validate_password_len(passwd_plain: &str) -> Result<()> {
        if passwd_plain.len() > chatter_protocol::MAX_PASSWORD_LEN {
            bail!(
                "Password exceeds {} bytes",
                chatter_protocol::MAX_PASSWORD_LEN
            );
        }
        Ok(())
    }

    /// Store a new account. The password is hashed with bcrypt before storing.
    ///
    /// Returns `Ok(true)` when the account is created and `Ok(false)` when the
    /// username already exists.
    pub fn insert_account(&self, name: String, mut passwd_plain: String) -> Result<bool> {
        // "anonymous" is the server's unauthenticated-peer sentinel; a real
        // account with that name would be permanently locked out of every
        // authenticated action while still receiving LoginOk.
        if name == RESERVED_ANONYMOUS_LOGIN {
            passwd_plain.zeroize();
            bail!("Username '{}' is reserved", RESERVED_ANONYMOUS_LOGIN);
        }

        if let Err(error) = Self::validate_password_len(&passwd_plain) {
            passwd_plain.zeroize();
            return Err(error);
        }

        let exists: bool = {
            let conn = self.lock_connection();
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM account WHERE name = ?1)",
                [&name],
                |row| row.get(0),
            )?
        };
        if exists {
            passwd_plain.zeroize();
            return Ok(false);
        }

        let hash_result =
            hash(&passwd_plain, DEFAULT_COST).with_context(|| "Failed to hash password");
        passwd_plain.zeroize();
        let mut hashed = hash_result?;
        let insert_result = {
            let conn = self.lock_connection();
            conn.execute(
                "INSERT INTO account (name, passwd) VALUES (?1, ?2)",
                rusqlite::params![&name, &hashed],
            )
        };
        hashed.zeroize();

        match insert_result {
            Ok(1) => Ok(true),
            Ok(n) => bail!("Unexpected account insert row count: {n}"),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
            {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Verify credentials by comparing the plaintext password against the stored bcrypt hash.
    pub fn verify_credentials(&self, name: String, mut passwd_plain: String) -> Result<bool> {
        if passwd_plain.len() > chatter_protocol::MAX_PASSWORD_LEN {
            passwd_plain.zeroize();
            return Ok(false);
        }

        let stored_hash: Option<String> = {
            let conn = self.lock_connection();
            let mut stmt = conn.prepare("SELECT passwd FROM account WHERE name = ?1")?;
            stmt.query_row([name], |row| row.get(0)).ok()
        };

        let verified = match stored_hash.as_deref() {
            Some(hash) => verify(&passwd_plain, hash).unwrap_or(false),
            None => {
                let _ = verify(&passwd_plain, DUMMY_BCRYPT_HASH);
                false
            }
        };
        passwd_plain.zeroize();
        Ok(verified)
    }

    /// Insert a chat message into the database.
    pub fn insert_message(&self, room: String, sender: String, content: String) -> Result<()> {
        let conn = self.lock_connection();
        conn.execute(
            "INSERT INTO messages (room, sender, content) VALUES (?1, ?2, ?3)",
            (&room, &sender, &content),
        )?;
        Ok(())
    }

    /// Get the last N messages from a room.
    pub fn get_room_history(
        &self,
        room: String,
        limit: i32,
    ) -> Result<Vec<chatter_protocol::HistoryEntry>> {
        let conn = self.lock_connection();
        let mut stmt = conn.prepare(
            "SELECT sender, content,
                    CASE
                        WHEN typeof(timestamp) = 'integer' THEN timestamp
                        ELSE unixepoch(timestamp)
                    END AS timestamp_unix
             FROM messages
             WHERE room = ?1
             ORDER BY id DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![room, limit], |row| {
            Ok(chatter_protocol::HistoryEntry {
                login: row.get(0)?,
                message: row.get(1)?,
                timestamp: row.get::<_, Option<i64>>(2)?.unwrap_or_default(),
            })
        })?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        messages.reverse();
        Ok(messages)
    }

    /// Get the list of rooms that have messages.
    pub fn get_rooms(&self) -> Result<Vec<String>> {
        let conn = self.lock_connection();
        let mut stmt = conn.prepare("SELECT DISTINCT room FROM messages ORDER BY room")?;
        let rooms = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut result = Vec::new();
        // Always include default rooms
        result.push("general".to_string());
        result.push("random".to_string());
        result.push("france".to_string());

        for room in rooms {
            let r = room?;
            if !result.contains(&r) {
                result.push(r);
            }
        }
        Ok(result)
    }

    pub fn list_accounts(&self) -> Result<()> {
        let conn = self.lock_connection();
        let mut stmt = conn.prepare("SELECT name FROM account")?;
        let accounts = stmt.query_map([], |row| row.get::<_, String>(0))?;

        for account in accounts {
            let acc = account?;
            println!("Found account: {}", acc);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a fresh in-memory Account for testing. Each call gets a new DB.
    fn test_account() -> Account {
        Account::new(":memory:".to_string()).expect("Failed to create in-memory account DB")
    }

    // --- Account::new ---

    #[test]
    fn test_new_creates_tables() {
        let account = test_account();
        let conn = account.lock_connection();
        let table_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('account', 'messages')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            table_count, 2,
            "Both account and messages tables should exist"
        );
    }

    #[test]
    fn test_new_with_file_db() {
        let db_path = "/tmp/chatter_test_file.db";
        let _ = std::fs::remove_file(db_path);
        let _account = Account::new(db_path.to_string()).expect("Should create file-based DB");
        assert!(std::path::Path::new(db_path).exists());
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_new_clone_is_independent() {
        let account = test_account();
        let account2 = account.clone();
        // Both share the same Arc<Mutex<Connection>>, so changes are visible
        account
            .insert_account("alice".to_string(), "pass".to_string())
            .unwrap();
        let verified = account2
            .verify_credentials("alice".to_string(), "pass".to_string())
            .unwrap();
        assert!(verified, "Cloned account should see the same data");
    }

    // --- insert_account ---

    #[test]
    fn test_insert_account_success() {
        let account = test_account();
        let result = account.insert_account("alice".to_string(), "password123".to_string());
        assert!(result.unwrap());
    }

    #[test]
    fn test_insert_account_stores_hashed_password() {
        let account = test_account();
        account
            .insert_account("bob".to_string(), "secret".to_string())
            .unwrap();

        let conn = account.lock_connection();
        let mut stmt = conn
            .prepare("SELECT passwd FROM account WHERE name = ?1")
            .unwrap();
        let stored_hash: String = stmt.query_row(["bob"], |row| row.get(0)).unwrap();

        // Password should be hashed (bcrypt format)
        assert!(stored_hash.starts_with("$2a$") || stored_hash.starts_with("$2b$"));
        // Raw password should NOT be stored
        assert_ne!(stored_hash, "secret");
    }

    #[test]
    fn test_insert_account_rejects_duplicate() {
        let account = test_account();
        let created = account
            .insert_account("alice".to_string(), "pass1".to_string())
            .unwrap();
        assert!(created);

        let result = account.insert_account("alice".to_string(), "pass2".to_string());
        assert!(!result.unwrap());

        // Only one record should exist
        let conn = account.lock_connection();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM account WHERE name = 'alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_insert_account_rejects_reserved_anonymous() {
        let account = test_account();
        let result = account.insert_account("anonymous".to_string(), "password123".to_string());
        assert!(
            result.is_err(),
            "The unauthenticated-peer sentinel must not be creatable as an account"
        );

        let conn = account.lock_connection();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM account WHERE name = 'anonymous'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "No 'anonymous' row should have been inserted");
    }

    #[test]
    fn test_insert_account_rejects_overlong_password() {
        let account = test_account();
        let password = "x".repeat(chatter_protocol::MAX_PASSWORD_LEN + 1);
        let result = account.insert_account("alice".to_string(), password);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_multiple_accounts() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "pass1".to_string())
            .unwrap();
        account
            .insert_account("bob".to_string(), "pass2".to_string())
            .unwrap();
        account
            .insert_account("charlie".to_string(), "pass3".to_string())
            .unwrap();

        let conn = account.lock_connection();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM account", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    // --- verify_credentials ---

    #[test]
    fn test_verify_credentials_correct() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "password123".to_string())
            .unwrap();
        let result = account
            .verify_credentials("alice".to_string(), "password123".to_string())
            .unwrap();
        assert!(result, "Correct credentials should verify");
    }

    #[test]
    fn test_verify_credentials_wrong_password() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "password123".to_string())
            .unwrap();
        let result = account
            .verify_credentials("alice".to_string(), "wrongpassword".to_string())
            .unwrap();
        assert!(!result, "Wrong password should fail");
    }

    #[test]
    fn test_verify_credentials_nonexistent_user() {
        let account = test_account();
        let result = account
            .verify_credentials("nobody".to_string(), "password".to_string())
            .unwrap();
        assert!(!result, "Non-existent user should fail");
    }

    #[test]
    fn test_verify_credentials_empty_password() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "realpass".to_string())
            .unwrap();
        let result = account
            .verify_credentials("alice".to_string(), "".to_string())
            .unwrap();
        assert!(!result, "Empty password should fail");
    }

    #[test]
    fn test_verify_credentials_rejects_overlong_password() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "realpass".to_string())
            .unwrap();

        let password = "x".repeat(chatter_protocol::MAX_PASSWORD_LEN + 1);
        let result = account
            .verify_credentials("alice".to_string(), password)
            .unwrap();

        assert!(
            !result,
            "Overlong password should fail before bcrypt truncation"
        );
    }

    #[test]
    fn test_verify_credentials_case_sensitive() {
        let account = test_account();
        account
            .insert_account("alice".to_string(), "Password".to_string())
            .unwrap();
        let wrong = account
            .verify_credentials("alice".to_string(), "password".to_string())
            .unwrap();
        let correct = account
            .verify_credentials("alice".to_string(), "Password".to_string())
            .unwrap();
        assert!(!wrong, "Lowercase should fail");
        assert!(correct, "Exact case should succeed");
    }

    // --- insert_message ---

    #[test]
    fn test_insert_message_success() {
        let account = test_account();
        let result = account.insert_message(
            "general".to_string(),
            "alice".to_string(),
            "Hello!".to_string(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_multiple_messages() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "Hello".to_string(),
            )
            .unwrap();
        account
            .insert_message(
                "general".to_string(),
                "bob".to_string(),
                "Hi there".to_string(),
            )
            .unwrap();
        account
            .insert_message(
                "random".to_string(),
                "alice".to_string(),
                "Random msg".to_string(),
            )
            .unwrap();

        let conn = account.lock_connection();
        let general_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE room='general'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let random_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE room='random'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(general_count, 2);
        assert_eq!(random_count, 1);
    }

    #[test]
    fn test_insert_message_stores_timestamp() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "Hello".to_string(),
            )
            .unwrap();

        let conn = account.lock_connection();
        let timestamp: i64 = conn
            .query_row(
                "SELECT timestamp FROM messages WHERE content='Hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(timestamp > 0);
    }

    // --- get_room_history ---

    #[test]
    fn test_get_room_history_empty_room() {
        let account = test_account();
        let history = account.get_room_history("general".to_string(), 50).unwrap();
        assert!(history.is_empty());
    }

    #[test]
    fn test_get_room_history_returns_messages() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "First".to_string(),
            )
            .unwrap();
        account
            .insert_message(
                "general".to_string(),
                "bob".to_string(),
                "Second".to_string(),
            )
            .unwrap();

        let history = account.get_room_history("general".to_string(), 50).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].message, "First");
        assert_eq!(history[1].message, "Second");
    }

    #[test]
    fn test_get_room_history_returns_structured_entries() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "Hello".to_string(),
            )
            .unwrap();

        let history = account.get_room_history("general".to_string(), 50).unwrap();
        let msg = &history[0];
        assert_eq!(msg.login, "alice");
        assert_eq!(msg.message, "Hello");
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_get_room_history_limit() {
        let account = test_account();
        for i in 0..10 {
            account
                .insert_message(
                    "general".to_string(),
                    "user".to_string(),
                    format!("msg{}", i),
                )
                .unwrap();
        }

        let history = account.get_room_history("general".to_string(), 3).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].message, "msg7");
        assert_eq!(history[1].message, "msg8");
        assert_eq!(history[2].message, "msg9");
    }

    #[test]
    fn test_get_room_history_chronological_order() {
        let account = test_account();
        account
            .insert_message("general".to_string(), "alice".to_string(), "A".to_string())
            .unwrap();
        account
            .insert_message("general".to_string(), "bob".to_string(), "B".to_string())
            .unwrap();
        account
            .insert_message(
                "general".to_string(),
                "charlie".to_string(),
                "C".to_string(),
            )
            .unwrap();

        let history = account.get_room_history("general".to_string(), 50).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].message, "A");
        assert_eq!(history[1].message, "B");
        assert_eq!(history[2].message, "C");
    }

    #[test]
    fn test_get_room_history_different_rooms() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "General msg".to_string(),
            )
            .unwrap();
        account
            .insert_message(
                "random".to_string(),
                "bob".to_string(),
                "Random msg".to_string(),
            )
            .unwrap();

        let general_history = account.get_room_history("general".to_string(), 50).unwrap();
        let random_history = account.get_room_history("random".to_string(), 50).unwrap();

        assert_eq!(general_history.len(), 1);
        assert_eq!(general_history[0].message, "General msg");

        assert_eq!(random_history.len(), 1);
        assert_eq!(random_history[0].message, "Random msg");
    }

    // --- get_rooms ---

    #[test]
    fn test_get_rooms_default() {
        let account = test_account();
        let rooms = account.get_rooms().unwrap();
        assert!(rooms.contains(&"general".to_string()));
        assert!(rooms.contains(&"random".to_string()));
        assert!(rooms.contains(&"france".to_string()));
        assert!(rooms.len() >= 3);
    }

    #[test]
    fn test_get_rooms_includes_message_rooms() {
        let account = test_account();
        account
            .insert_message(
                "custom-room".to_string(),
                "alice".to_string(),
                "Hello".to_string(),
            )
            .unwrap();

        let rooms = account.get_rooms().unwrap();
        assert!(rooms.contains(&"custom-room".to_string()));
        assert!(rooms.contains(&"general".to_string()));
    }

    #[test]
    fn test_get_rooms_no_duplicates() {
        let account = test_account();
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "Hello".to_string(),
            )
            .unwrap();

        let rooms = account.get_rooms().unwrap();
        let general_count = rooms.iter().filter(|r| *r == "general").count();
        assert_eq!(
            general_count, 1,
            "Default room with messages should appear only once"
        );
    }

    // --- Full auth flow ---

    #[test]
    fn test_full_auth_flow() {
        let account = test_account();

        // Create account with plaintext password
        account
            .insert_account("alice".to_string(), "password123".to_string())
            .unwrap();

        // Login with correct plaintext password
        assert!(
            account
                .verify_credentials("alice".to_string(), "password123".to_string())
                .unwrap(),
            "Login with correct password should succeed"
        );

        // Login with wrong password
        assert!(
            !account
                .verify_credentials("alice".to_string(), "wrong".to_string())
                .unwrap(),
            "Login with wrong password should fail"
        );

        // Non-existent user
        assert!(
            !account
                .verify_credentials("nobody".to_string(), "password".to_string())
                .unwrap(),
            "Non-existent user should fail"
        );
    }

    #[test]
    fn test_full_chat_flow() {
        let account = test_account();

        // Alice sends a message
        account
            .insert_message(
                "general".to_string(),
                "alice".to_string(),
                "Hello everyone!".to_string(),
            )
            .unwrap();

        // Bob sends a reply
        account
            .insert_message(
                "general".to_string(),
                "bob".to_string(),
                "Hi Alice!".to_string(),
            )
            .unwrap();

        // Get history
        let history = account.get_room_history("general".to_string(), 50).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].login, "alice");
        assert_eq!(history[1].login, "bob");

        // Get rooms
        let rooms = account.get_rooms().unwrap();
        assert!(rooms.contains(&"general".to_string()));
    }
}
