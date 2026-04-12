// Package persistence provides SQLite-backed persistence for usage records,
// account states, and key-value settings for the CLI Proxy API server.
package persistence

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Open opens (or creates) the SQLite database at the given path and initialises
// the schema. The caller is responsible for closing the returned *sql.DB.
func Open(dataDir string) (*sql.DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("persistence: create data dir %s: %w", dataDir, err)
	}
	dbPath := filepath.Join(dataDir, "stats.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("persistence: open db %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer
	if err := InitSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// InitSchema creates all tables and indexes if they do not yet exist.
func InitSchema(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS usage_records (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_hash       TEXT NOT NULL UNIQUE,
    api_key          TEXT NOT NULL DEFAULT '',
    model            TEXT NOT NULL DEFAULT '',
    timestamp        TEXT NOT NULL,
    source           TEXT,
    auth_index       TEXT,
    auth_id          TEXT,
    provider         TEXT,
    input_tokens     INTEGER DEFAULT 0,
    output_tokens    INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cached_tokens    INTEGER DEFAULT 0,
    total_tokens     INTEGER DEFAULT 0,
    failed           INTEGER DEFAULT 0,
    is_keepalive     INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ur_timestamp  ON usage_records(timestamp);
CREATE INDEX IF NOT EXISTS idx_ur_auth_index ON usage_records(auth_index);

CREATE TABLE IF NOT EXISTS account_states (
    auth_index              TEXT PRIMARY KEY,
    auth_id                 TEXT NOT NULL,
    email                   TEXT,
    name                    TEXT,
    label                   TEXT,
    unavailable             INTEGER DEFAULT 0,
    disabled                INTEGER DEFAULT 0,
    next_retry_after        TEXT,
    quota_backoff_lvl       INTEGER DEFAULT 0,
    status                  TEXT DEFAULT 'unknown',
    status_message          TEXT,
    updated_at              TEXT NOT NULL,
    last_keepalive_sent_at  TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS account_reset_history (
    auth_index        TEXT NOT NULL,
    rl_reset_requests TEXT NOT NULL,
    recorded_at       TEXT NOT NULL,
    UNIQUE(auth_index, rl_reset_requests)
);
CREATE INDEX IF NOT EXISTS idx_arh_auth_index ON account_reset_history(auth_index);
`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("persistence: init schema: %w", err)
	}

	// Migration: add last_keepalive_sent_at column if it doesn't exist
	var colExists bool
	err := db.QueryRow(`
		SELECT COUNT(*) > 0 FROM pragma_table_info('account_states')
		WHERE name = 'last_keepalive_sent_at'
	`).Scan(&colExists)
	if err == nil && !colExists {
		if _, err := db.Exec(`ALTER TABLE account_states ADD COLUMN last_keepalive_sent_at TEXT`); err != nil {
			return fmt.Errorf("persistence: add last_keepalive_sent_at column: %w", err)
		}
	}

	// Migration: add rate limit columns to account_states
	rlColumns := []struct {
		name string
		ddl  string
	}{
		{"rl_limit_requests", "ALTER TABLE account_states ADD COLUMN rl_limit_requests INTEGER DEFAULT 0"},
		{"rl_remaining_requests", "ALTER TABLE account_states ADD COLUMN rl_remaining_requests INTEGER DEFAULT 0"},
		{"rl_reset_requests", "ALTER TABLE account_states ADD COLUMN rl_reset_requests TEXT"},
		{"rl_limit_tokens", "ALTER TABLE account_states ADD COLUMN rl_limit_tokens INTEGER DEFAULT 0"},
		{"rl_remaining_tokens", "ALTER TABLE account_states ADD COLUMN rl_remaining_tokens INTEGER DEFAULT 0"},
		{"rl_reset_tokens", "ALTER TABLE account_states ADD COLUMN rl_reset_tokens TEXT"},
		{"rl_updated_at", "ALTER TABLE account_states ADD COLUMN rl_updated_at TEXT"},
	}
	for _, col := range rlColumns {
		var exists bool
		if qErr := db.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('account_states')
			WHERE name = ?
		`, col.name).Scan(&exists); qErr == nil && !exists {
			if _, aErr := db.Exec(col.ddl); aErr != nil {
				return fmt.Errorf("persistence: add %s column: %w", col.name, aErr)
			}
		}
	}

	// Migration: seed account_reset_history from existing account_states
	_, _ = db.Exec(`
		INSERT OR IGNORE INTO account_reset_history (auth_index, rl_reset_requests, recorded_at)
		SELECT auth_index, rl_reset_requests, datetime('now')
		FROM account_states
		WHERE rl_reset_requests IS NOT NULL AND rl_reset_requests != ''
	`)

	// Migration: add window_type column to account_reset_history for dual-window support (Plus accounts).
	// 'primary' = short-term window (default), 'secondary' = weekly quota window.
	var wtExists bool
	if qErr := db.QueryRow(`
		SELECT COUNT(*) > 0 FROM pragma_table_info('account_reset_history')
		WHERE name = 'window_type'
	`).Scan(&wtExists); qErr == nil && !wtExists {
		if _, aErr := db.Exec(`ALTER TABLE account_reset_history ADD COLUMN window_type TEXT NOT NULL DEFAULT 'primary'`); aErr != nil {
			return fmt.Errorf("persistence: add window_type column to account_reset_history: %w", aErr)
		}
	}

	// Seed secondary window history from existing account_states rl_reset_tokens
	_, _ = db.Exec(`
		INSERT OR IGNORE INTO account_reset_history (auth_index, rl_reset_requests, recorded_at, window_type)
		SELECT auth_index, rl_reset_tokens, datetime('now'), 'secondary'
		FROM account_states
		WHERE rl_reset_tokens IS NOT NULL AND rl_reset_tokens != ''
		  AND rl_limit_requests = 100 AND rl_limit_tokens = 100
	`)

	return nil
}
