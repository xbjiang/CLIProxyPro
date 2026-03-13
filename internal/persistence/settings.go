package persistence

import (
	"database/sql"
	"errors"
)

// GetSetting returns the value for the given key, or "" if not found.
func GetSetting(db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

// SetSetting upserts a key-value pair into the settings table.
func SetSetting(db *sql.DB, key, value string) error {
	_, err := db.Exec(
		`INSERT INTO settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value,
	)
	return err
}
