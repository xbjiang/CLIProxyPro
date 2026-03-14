package persistence

import (
	"database/sql"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// AccountStateRow mirrors the account_states table.
type AccountStateRow struct {
	AuthIndex            string
	AuthID               string
	Email                string
	Name                 string
	Label                string
	Unavailable          bool
	Disabled             bool
	NextRetryAfter       *time.Time // nil = no restriction
	QuotaBackoffLvl      int
	Status               string
	StatusMessage        string
	UpdatedAt            time.Time
	LastKeepaliveSentAt  *time.Time // nil = never sent
}

// UpsertAccountStates writes the given auth entries, applying the CASE logic for
// next_retry_after: keep the stored value when the live value is zero but the stored
// value hasn't expired yet.
func UpsertAccountStates(db *sql.DB, auths []*coreauth.Auth) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT INTO account_states
			(auth_index, auth_id, email, name, label, unavailable, disabled,
			 next_retry_after, quota_backoff_lvl, status, status_message, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(auth_index) DO UPDATE SET
			auth_id          = excluded.auth_id,
			email            = excluded.email,
			name             = excluded.name,
			label            = excluded.label,
			unavailable      = excluded.unavailable,
			disabled         = excluded.disabled,
			next_retry_after = CASE
				WHEN excluded.next_retry_after IS NOT NULL THEN excluded.next_retry_after
				WHEN account_states.next_retry_after IS NOT NULL
				     AND account_states.next_retry_after > datetime('now') THEN account_states.next_retry_after
				ELSE NULL
			END,
			quota_backoff_lvl = excluded.quota_backoff_lvl,
			status           = excluded.status,
			status_message   = excluded.status_message,
			updated_at       = excluded.updated_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for _, a := range auths {
		if a == nil {
			continue
		}
		idx := a.EnsureIndex()
		if idx == "" {
			continue
		}
		email := ""
		if a.Metadata != nil {
			if v, ok := a.Metadata["email"].(string); ok {
				email = v
			}
		}
		var nra *string
		if !a.NextRetryAfter.IsZero() {
			s := a.NextRetryAfter.UTC().Format(time.RFC3339)
			nra = &s
		}
		unav := 0
		if a.Unavailable {
			unav = 1
		}
		dis := 0
		if a.Disabled {
			dis = 1
		}
		if _, err := stmt.Exec(
			idx, a.ID, email, a.Label, a.Label,
			unav, dis, nra,
			a.Quota.BackoffLevel,
			string(a.Status), a.StatusMessage,
			now,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadAccountStates returns all stored account states keyed by auth_index.
func LoadAccountStates(db *sql.DB) (map[string]*AccountStateRow, error) {
	rows, err := db.Query(`
		SELECT auth_index, auth_id, COALESCE(email,''), COALESCE(name,''), COALESCE(label,''),
		       unavailable, disabled, next_retry_after,
		       quota_backoff_lvl, COALESCE(status,'unknown'), COALESCE(status_message,''),
		       updated_at, last_keepalive_sent_at
		FROM account_states`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*AccountStateRow)
	for rows.Next() {
		r := &AccountStateRow{}
		var nra sql.NullString
		var updStr string
		var lksa sql.NullString
		if err := rows.Scan(
			&r.AuthIndex, &r.AuthID, &r.Email, &r.Name, &r.Label,
			&r.Unavailable, &r.Disabled, &nra,
			&r.QuotaBackoffLvl, &r.Status, &r.StatusMessage,
			&updStr, &lksa,
		); err != nil {
			return nil, err
		}
		if nra.Valid && nra.String != "" {
			if t, err := time.Parse(time.RFC3339, nra.String); err == nil {
				r.NextRetryAfter = &t
			}
		}
		if lksa.Valid && lksa.String != "" {
			if t, err := time.Parse(time.RFC3339, lksa.String); err == nil {
				r.LastKeepaliveSentAt = &t
			}
		}
		r.UpdatedAt, _ = time.Parse(time.RFC3339, updStr)
		result[r.AuthIndex] = r
	}
	return result, nil
}

// GetTargetsForKeepalive returns auth_index values where next_retry_after has passed
// (the account's quota is now available again) and the account was marked unavailable.
func GetTargetsForKeepalive(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT auth_index FROM account_states
		WHERE unavailable = 1 AND next_retry_after IS NOT NULL
		  AND next_retry_after <= datetime('now')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var idx string
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		result = append(result, idx)
	}
	return result, nil
}

// UpdateLastKeepaliveSentAt records the keepalive send time for the given auth_id.
func UpdateLastKeepaliveSentAt(db *sql.DB, authID string, sentAt time.Time) error {
	_, err := db.Exec(`
		UPDATE account_states
		SET last_keepalive_sent_at = ?
		WHERE auth_id = ?
	`, sentAt.UTC().Format(time.RFC3339), authID)
	return err
}
