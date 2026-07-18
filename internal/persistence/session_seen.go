package persistence

import (
	"database/sql"
	"time"
)

const createSessionSeenTable = `
CREATE TABLE IF NOT EXISTS session_seen (
    session_id  TEXT PRIMARY KEY,
    provider    TEXT NOT NULL DEFAULT '',
    workspace   TEXT NOT NULL DEFAULT '',
    last_seen   TEXT NOT NULL
);`

// SessionSeen represents a recently observed session.
type SessionSeen struct {
	SessionID string `json:"session_id"`
	Provider  string `json:"provider"`
	Workspace string `json:"workspace"`
	LastSeen  string `json:"last_seen"`
}

// InitSessionSeenTable creates the session_seen table if it doesn't exist.
func InitSessionSeenTable(db *sql.DB) error {
	_, err := db.Exec(createSessionSeenTable)
	return err
}

// UpsertSessionSeen records or updates a session sighting.
func UpsertSessionSeen(db *sql.DB, sessionID, provider, workspace string) {
	if db == nil || sessionID == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, _ = db.Exec(`
		INSERT INTO session_seen (session_id, provider, workspace, last_seen)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			workspace = CASE WHEN excluded.workspace != '' THEN excluded.workspace ELSE session_seen.workspace END,
			last_seen = excluded.last_seen`,
		sessionID, provider, workspace, now,
	)
}

// ListRecentSessions returns recent sessions, optionally filtered by provider.
func ListRecentSessions(db *sql.DB, provider string, limit int) ([]SessionSeen, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if provider != "" {
		rows, err = db.Query(`
			SELECT session_id, provider, workspace, last_seen
			FROM session_seen
			WHERE provider = ?
			ORDER BY last_seen DESC
			LIMIT ?`, provider, limit)
	} else {
		rows, err = db.Query(`
			SELECT session_id, provider, workspace, last_seen
			FROM session_seen
			ORDER BY last_seen DESC
			LIMIT ?`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionSeen
	for rows.Next() {
		var s SessionSeen
		if err := rows.Scan(&s.SessionID, &s.Provider, &s.Workspace, &s.LastSeen); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	if results == nil {
		results = []SessionSeen{}
	}
	return results, rows.Err()
}

// CleanOldSessions removes sessions not seen in the last 7 days.
func CleanOldSessions(db *sql.DB) {
	if db == nil {
		return
	}
	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour).Format(time.RFC3339)
	_, _ = db.Exec(`DELETE FROM session_seen WHERE last_seen < ?`, cutoff)
}
