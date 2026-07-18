package persistence

import (
	"database/sql"
	"time"
)

const createSessionBindingsTable = `
CREATE TABLE IF NOT EXISTS session_bindings (
    session_id  TEXT PRIMARY KEY,
    auth_index  TEXT NOT NULL,
    provider    TEXT NOT NULL DEFAULT '',
    note        TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);`

// SessionBinding represents a persistent session-to-relay binding.
type SessionBinding struct {
	SessionID string `json:"session_id"`
	AuthIndex string `json:"auth_index"`
	Provider  string `json:"provider"`
	Note      string `json:"note"`
	CreatedAt string `json:"created_at"`
}

// InitSessionBindingsTable creates the session_bindings table if it doesn't exist.
func InitSessionBindingsTable(db *sql.DB) error {
	_, err := db.Exec(createSessionBindingsTable)
	return err
}

// ListSessionBindings returns all session bindings ordered by creation time descending.
func ListSessionBindings(db *sql.DB) ([]SessionBinding, error) {
	rows, err := db.Query(`
		SELECT session_id, auth_index, provider, note, created_at
		FROM session_bindings
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionBinding
	for rows.Next() {
		var b SessionBinding
		if err := rows.Scan(&b.SessionID, &b.AuthIndex, &b.Provider, &b.Note, &b.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, b)
	}
	if results == nil {
		results = []SessionBinding{}
	}
	return results, rows.Err()
}

// UpsertSessionBinding creates or updates a session binding.
func UpsertSessionBinding(db *sql.DB, sessionID, authIndex, provider, note string) error {
	_, err := db.Exec(`
		INSERT INTO session_bindings (session_id, auth_index, provider, note, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			auth_index = excluded.auth_index,
			provider   = excluded.provider,
			note       = excluded.note`,
		sessionID,
		authIndex,
		provider,
		note,
		time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

// DeleteSessionBinding removes a session binding by session_id.
func DeleteSessionBinding(db *sql.DB, sessionID string) error {
	_, err := db.Exec(`DELETE FROM session_bindings WHERE session_id = ?`, sessionID)
	return err
}

// GetSessionBinding looks up the auth_index bound to a session.
// Returns ("", false, nil) when not found.
func GetSessionBinding(db *sql.DB, sessionID string) (authIndex string, found bool, err error) {
	row := db.QueryRow(`SELECT auth_index FROM session_bindings WHERE session_id = ?`, sessionID)
	err = row.Scan(&authIndex)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return authIndex, true, nil
}
