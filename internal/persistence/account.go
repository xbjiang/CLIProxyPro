package persistence

import (
	"database/sql"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// AccountStateRow mirrors the account_states table.
type AccountStateRow struct {
	AuthIndex           string
	AuthID              string
	Email               string
	Name                string
	Label               string
	Unavailable         bool
	Disabled            bool
	NextRetryAfter      *time.Time // nil = no restriction
	QuotaBackoffLvl     int
	Status              string
	StatusMessage       string
	UpdatedAt           time.Time
	LastKeepaliveSentAt *time.Time // nil = never sent
	RateLimit           *coreauth.RateLimitInfo
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

	histStmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO account_reset_history (auth_index, rl_reset_requests, recorded_at, window_type)
		VALUES (?, ?, ?, 'primary')
	`)
	if err != nil {
		return err
	}
	defer histStmt.Close()

	// maxResetStmt fetches the latest recorded reset_time for a given auth_index,
	// used to suppress near-duplicate writes caused by per-request header drift.
	maxResetStmt, err := tx.Prepare(`
		SELECT MAX(rl_reset_requests) FROM account_reset_history WHERE auth_index = ? AND (window_type = 'primary' OR window_type IS NULL)
	`)
	if err != nil {
		return err
	}
	defer maxResetStmt.Close()

	stmt, err := tx.Prepare(`
		INSERT INTO account_states
			(auth_index, auth_id, email, name, label, unavailable, disabled,
			 next_retry_after, quota_backoff_lvl, status, status_message, updated_at,
			 rl_limit_requests, rl_remaining_requests, rl_reset_requests,
			 rl_limit_tokens, rl_remaining_tokens, rl_reset_tokens, rl_updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
				     AND datetime(account_states.next_retry_after) > datetime('now') THEN account_states.next_retry_after
				-- Preserve past value when no keepalive has been sent since the reset,
				-- so the frontend can still detect and surface the missed keepalive.
				WHEN account_states.next_retry_after IS NOT NULL
				     AND (account_states.last_keepalive_sent_at IS NULL
				          OR account_states.last_keepalive_sent_at < account_states.next_retry_after) THEN account_states.next_retry_after
				ELSE NULL
			END,
			quota_backoff_lvl    = excluded.quota_backoff_lvl,
			status               = excluded.status,
			status_message       = excluded.status_message,
			updated_at           = excluded.updated_at,
			rl_limit_requests = CASE
				WHEN excluded.rl_limit_requests > 0 OR excluded.rl_remaining_requests > 0 THEN excluded.rl_limit_requests
				WHEN account_states.rl_reset_requests IS NOT NULL
				     AND datetime(account_states.rl_reset_requests) > datetime('now') THEN account_states.rl_limit_requests
				ELSE excluded.rl_limit_requests
			END,
			rl_remaining_requests = CASE
				WHEN excluded.rl_limit_requests > 0 OR excluded.rl_remaining_requests > 0 THEN excluded.rl_remaining_requests
				WHEN account_states.rl_reset_requests IS NOT NULL
				     AND datetime(account_states.rl_reset_requests) > datetime('now') THEN account_states.rl_remaining_requests
				ELSE excluded.rl_remaining_requests
			END,
			rl_reset_requests = CASE
				WHEN (excluded.rl_limit_requests > 0 OR excluded.rl_remaining_requests > 0) AND excluded.rl_reset_requests IS NOT NULL AND datetime(excluded.rl_reset_requests) > datetime('now') THEN excluded.rl_reset_requests
				WHEN account_states.rl_reset_requests IS NOT NULL AND datetime(account_states.rl_reset_requests) > datetime('now') THEN account_states.rl_reset_requests
				WHEN (excluded.rl_limit_requests > 0 OR excluded.rl_remaining_requests > 0) AND excluded.rl_reset_requests IS NOT NULL THEN excluded.rl_reset_requests
				ELSE excluded.rl_reset_requests
			END,
			rl_limit_tokens = CASE
				WHEN excluded.rl_limit_tokens > 0 OR excluded.rl_remaining_tokens > 0 THEN excluded.rl_limit_tokens
				WHEN account_states.rl_reset_tokens IS NOT NULL
				     AND datetime(account_states.rl_reset_tokens) > datetime('now') THEN account_states.rl_limit_tokens
				ELSE excluded.rl_limit_tokens
			END,
			rl_remaining_tokens = CASE
				WHEN excluded.rl_limit_tokens > 0 OR excluded.rl_remaining_tokens > 0 THEN excluded.rl_remaining_tokens
				WHEN account_states.rl_reset_tokens IS NOT NULL
				     AND datetime(account_states.rl_reset_tokens) > datetime('now') THEN account_states.rl_remaining_tokens
				ELSE excluded.rl_remaining_tokens
			END,
			rl_reset_tokens = CASE
				WHEN (excluded.rl_limit_tokens > 0 OR excluded.rl_remaining_tokens > 0) AND excluded.rl_reset_tokens IS NOT NULL AND datetime(excluded.rl_reset_tokens) > datetime('now') THEN excluded.rl_reset_tokens
				WHEN account_states.rl_reset_tokens IS NOT NULL AND datetime(account_states.rl_reset_tokens) > datetime('now') THEN account_states.rl_reset_tokens
				WHEN (excluded.rl_limit_tokens > 0 OR excluded.rl_remaining_tokens > 0) AND excluded.rl_reset_tokens IS NOT NULL THEN excluded.rl_reset_tokens
				ELSE excluded.rl_reset_tokens
			END,
			rl_updated_at = CASE
				WHEN excluded.rl_updated_at IS NOT NULL THEN excluded.rl_updated_at
				WHEN account_states.rl_reset_requests IS NOT NULL
				     AND datetime(account_states.rl_reset_requests) > datetime('now') THEN account_states.rl_updated_at
				ELSE excluded.rl_updated_at
			END
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
		if !a.NextRetryAfter.IsZero() && a.Quota.Exceeded {
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
		var rlLimitReq, rlRemainReq, rlLimitTok, rlRemainTok int
		var rlResetReq, rlResetTok, rlUpdatedAt *string
		if a.RateLimit != nil {
			rlLimitReq = a.RateLimit.LimitRequests
			rlRemainReq = a.RateLimit.RemainingRequests
			rlLimitTok = a.RateLimit.LimitTokens
			rlRemainTok = a.RateLimit.RemainingTokens
			if !a.RateLimit.ResetRequests.IsZero() {
				s := a.RateLimit.ResetRequests.UTC().Format(time.RFC3339)
				rlResetReq = &s
			}
			if !a.RateLimit.ResetTokens.IsZero() {
				s := a.RateLimit.ResetTokens.UTC().Format(time.RFC3339)
				rlResetTok = &s
			}
			if !a.RateLimit.UpdatedAt.IsZero() {
				s := a.RateLimit.UpdatedAt.UTC().Format(time.RFC3339)
				rlUpdatedAt = &s
			}
		}
		if _, err := stmt.Exec(
			idx, a.ID, email, a.Label, a.Label,
			unav, dis, nra,
			a.Quota.BackoffLevel,
			string(a.Status), a.StatusMessage,
			now,
			rlLimitReq, rlRemainReq, rlResetReq,
			rlLimitTok, rlRemainTok, rlResetTok, rlUpdatedAt,
		); err != nil {
			return err
		}
		if rlResetReq != nil {
			// Only record a new cycle boundary if it is at least roughly a full cycle length 
			// after the latest already-recorded reset. We use 1 hour threshold to suppress
			// seconds-level drift and allow both 5-hour and 7-day cycles to register properly.
			var maxReset sql.NullString
			_ = maxResetStmt.QueryRow(idx).Scan(&maxReset)
			shouldRecord := true
			if maxReset.Valid && maxReset.String != "" {
				if last, err2 := time.Parse(time.RFC3339, maxReset.String); err2 == nil {
					if next, err2 := time.Parse(time.RFC3339, *rlResetReq); err2 == nil {
						if next.Sub(last) < 1*time.Hour {
							shouldRecord = false
						}
					}
				}
			}
			if shouldRecord {
				if _, err := histStmt.Exec(idx, *rlResetReq, now); err != nil {
					return err
				}
			}
		}
		// Record secondary window (weekly quota) reset for Plus accounts.
		// Plus accounts use percent mode (limit=100) and have a separate weekly
		// reset tracked in rl_reset_tokens. Apply the same 6-day dedup threshold
		// to avoid near-duplicate entries caused by header drift.
		isPctMode := rlLimitReq == 100 && rlLimitTok == 100
		if isPctMode && rlResetTok != nil {
			var maxSecondaryReset sql.NullString
			_ = tx.QueryRow(`
				SELECT MAX(rl_reset_requests) FROM account_reset_history
				WHERE auth_index = ? AND window_type = 'secondary'
			`, idx).Scan(&maxSecondaryReset)
			shouldRecordSecondary := true
			if maxSecondaryReset.Valid && maxSecondaryReset.String != "" {
				if last, err2 := time.Parse(time.RFC3339, maxSecondaryReset.String); err2 == nil {
					if next, err2 := time.Parse(time.RFC3339, *rlResetTok); err2 == nil {
						if next.Sub(last) < 6*24*time.Hour {
							shouldRecordSecondary = false
						}
					}
				}
			}
			if shouldRecordSecondary {
				if _, err := tx.Exec(`
					INSERT OR IGNORE INTO account_reset_history (auth_index, rl_reset_requests, recorded_at, window_type)
					VALUES (?, ?, ?, 'secondary')
				`, idx, *rlResetTok, now); err != nil {
					return err
				}
			}
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
		       updated_at, last_keepalive_sent_at,
		       COALESCE(rl_limit_requests,0), COALESCE(rl_remaining_requests,0), rl_reset_requests,
		       COALESCE(rl_limit_tokens,0), COALESCE(rl_remaining_tokens,0), rl_reset_tokens, rl_updated_at
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
		var rlLimitReq, rlRemainReq, rlLimitTok, rlRemainTok int
		var rlResetReq, rlResetTok, rlUpdatedAt sql.NullString
		if err := rows.Scan(
			&r.AuthIndex, &r.AuthID, &r.Email, &r.Name, &r.Label,
			&r.Unavailable, &r.Disabled, &nra,
			&r.QuotaBackoffLvl, &r.Status, &r.StatusMessage,
			&updStr, &lksa,
			&rlLimitReq, &rlRemainReq, &rlResetReq,
			&rlLimitTok, &rlRemainTok, &rlResetTok, &rlUpdatedAt,
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
		// Reconstruct RateLimitInfo if any value is non-zero/non-empty
		if rlLimitReq > 0 || rlRemainReq > 0 || rlLimitTok > 0 || rlRemainTok > 0 ||
			rlResetReq.Valid || rlResetTok.Valid || rlUpdatedAt.Valid {
			rl := &coreauth.RateLimitInfo{
				LimitRequests:     rlLimitReq,
				RemainingRequests: rlRemainReq,
				LimitTokens:       rlLimitTok,
				RemainingTokens:   rlRemainTok,
			}
			if rlResetReq.Valid && rlResetReq.String != "" {
				if t, err := time.Parse(time.RFC3339, rlResetReq.String); err == nil {
					rl.ResetRequests = t
				}
			}
			if rlResetTok.Valid && rlResetTok.String != "" {
				if t, err := time.Parse(time.RFC3339, rlResetTok.String); err == nil {
					rl.ResetTokens = t
				}
			}
			if rlUpdatedAt.Valid && rlUpdatedAt.String != "" {
				if t, err := time.Parse(time.RFC3339, rlUpdatedAt.String); err == nil {
					rl.UpdatedAt = t
				}
			}
			r.RateLimit = rl
		}
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
		  AND datetime(next_retry_after) <= datetime('now')
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
