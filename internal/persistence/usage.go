package persistence

import (
	"context"
	"database/sql"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// InsertUsageRecord writes a single usage record, ignoring duplicates (by dedup_hash).
func InsertUsageRecord(db *sql.DB, hash string, rec coreusage.Record, isKeepalive bool) error {
	kv := 0
	if isKeepalive {
		kv = 1
	}
	fv := 0
	if rec.Failed {
		fv = 1
	}
	_, err := db.Exec(`
		INSERT OR IGNORE INTO usage_records
			(dedup_hash, api_key, model, timestamp, source, auth_index, auth_id, provider,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, failed, is_keepalive)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		hash,
		rec.APIKey,
		rec.Model,
		rec.RequestedAt.UTC().Format(time.RFC3339Nano),
		rec.Source,
		rec.AuthIndex,
		rec.AuthID,
		rec.Provider,
		rec.Detail.InputTokens,
		rec.Detail.OutputTokens,
		rec.Detail.ReasoningTokens,
		rec.Detail.CachedTokens,
		rec.Detail.TotalTokens,
		fv,
		kv,
	)
	return err
}

// DedupHash computes the canonical dedup hash for a usage record.
// Format is identical to the Node.js generateDedupHash and the internal dedupKey function.
func DedupHash(rec coreusage.Record) string {
	ts := rec.RequestedAt.UTC().Format(time.RFC3339Nano)
	fStr := "false"
	if rec.Failed {
		fStr = "true"
	}
	raw := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%d|%d|%d|%d|%d",
		rec.APIKey,
		rec.Model,
		ts,
		rec.Source,
		rec.AuthIndex,
		fStr,
		rec.Detail.InputTokens,
		rec.Detail.OutputTokens,
		rec.Detail.ReasoningTokens,
		rec.Detail.CachedTokens,
		rec.Detail.TotalTokens,
	)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// AggregatedStats is returned by QueryAggregated.
type AggregatedStats struct {
	TotalRequests   int64            `json:"total_requests"`
	TotalTokens     int64            `json:"total_tokens"`
	InputTokens     int64            `json:"input_tokens"`
	OutputTokens    int64            `json:"output_tokens"`
	ReasoningTokens int64            `json:"reasoning_tokens"`
	CachedTokens    int64            `json:"cached_tokens"`
	FailedRequests  int64            `json:"failed_requests"`
	ByModel         []ModelStat      `json:"by_model"`
	ByAuthIndex     []AuthIndexStat  `json:"by_auth_index"`
}

type ModelStat struct {
	Model           string `json:"model"`
	Requests        int64  `json:"requests"`
	TotalTokens     int64  `json:"total_tokens"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
}

type AuthIndexStat struct {
	AuthIndex   string `json:"auth_index"`
	AuthID      string `json:"auth_id"`
	Requests    int64  `json:"requests"`
	TotalTokens int64  `json:"total_tokens"`
}

// QueryAggregated returns summary statistics from usage_records.
func QueryAggregated(ctx context.Context, db *sql.DB) (*AggregatedStats, error) {
	stats := &AggregatedStats{}

	row := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(total_tokens),0),
			COALESCE(SUM(input_tokens),0),
			COALESCE(SUM(output_tokens),0),
			COALESCE(SUM(reasoning_tokens),0),
			COALESCE(SUM(cached_tokens),0),
			COALESCE(SUM(CASE WHEN failed=1 THEN 1 ELSE 0 END),0)
		FROM usage_records`)
	if err := row.Scan(
		&stats.TotalRequests,
		&stats.TotalTokens,
		&stats.InputTokens,
		&stats.OutputTokens,
		&stats.ReasoningTokens,
		&stats.CachedTokens,
		&stats.FailedRequests,
	); err != nil {
		return nil, err
	}

	// By model
	rows, err := db.QueryContext(ctx, `
		SELECT model, COUNT(*), COALESCE(SUM(total_tokens),0), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0)
		FROM usage_records
		GROUP BY model ORDER BY COUNT(*) DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ms ModelStat
		if err := rows.Scan(&ms.Model, &ms.Requests, &ms.TotalTokens, &ms.InputTokens, &ms.OutputTokens); err != nil {
			return nil, err
		}
		stats.ByModel = append(stats.ByModel, ms)
	}

	// By auth index
	rows2, err := db.QueryContext(ctx, `
		SELECT COALESCE(auth_index,''), COALESCE(auth_id,''), COUNT(*), COALESCE(SUM(total_tokens),0)
		FROM usage_records
		GROUP BY auth_index ORDER BY COUNT(*) DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var as AuthIndexStat
		if err := rows2.Scan(&as.AuthIndex, &as.AuthID, &as.Requests, &as.TotalTokens); err != nil {
			return nil, err
		}
		stats.ByAuthIndex = append(stats.ByAuthIndex, as)
	}

	return stats, nil
}

// DailyStat is one row from QueryDaily.
type DailyStat struct {
	Date            string `json:"date"`
	Requests        int64  `json:"requests"`
	Success         int64  `json:"success"`
	Failures        int64  `json:"failures"`
	TotalTokens     int64  `json:"total_tokens"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
	CachedTokens    int64  `json:"cached_tokens"`
}

// QueryDaily returns per-day statistics for the last n days.
func QueryDaily(ctx context.Context, db *sql.DB, days int) ([]DailyStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			DATE(timestamp) as day,
			COUNT(*),
			SUM(CASE WHEN failed=0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN failed=1 THEN 1 ELSE 0 END),
			COALESCE(SUM(total_tokens),0),
			COALESCE(SUM(input_tokens),0),
			COALESCE(SUM(output_tokens),0),
			COALESCE(SUM(reasoning_tokens),0),
			COALESCE(SUM(cached_tokens),0)
		FROM usage_records
		WHERE timestamp >= datetime('now', ? || ' days')
		GROUP BY day ORDER BY day ASC`,
		fmt.Sprintf("-%d", days),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DailyStat
	for rows.Next() {
		var d DailyStat
		if err := rows.Scan(&d.Date, &d.Requests, &d.Success, &d.Failures,
			&d.TotalTokens, &d.InputTokens, &d.OutputTokens,
			&d.ReasoningTokens, &d.CachedTokens); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, nil
}

// DeleteOld removes usage records older than the given duration.
func DeleteOld(db *sql.DB, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339)
	res, err := db.Exec(`DELETE FROM usage_records WHERE timestamp < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// UsageRecord represents a single usage record from the database.
type UsageRecord struct {
	Timestamp       string `json:"timestamp"`
	Source          string `json:"source"`
	Model           string `json:"model"`
	Failed          bool   `json:"failed"`
	IsKeepalive     bool   `json:"is_keepalive"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
	CachedTokens    int64  `json:"cached_tokens"`
	TotalTokens     int64  `json:"total_tokens"`
}

// AccountCycleStat represents per-account usage within its current quota cycle.
type AccountCycleStat struct {
	AuthIndex       string `json:"auth_index"`
	Email           string `json:"email"`
	CycleStart      string `json:"cycle_start"`
	CycleEnd        string `json:"cycle_end"`
	SuccessRequests int64  `json:"success_requests"`
	FailedRequests  int64  `json:"failed_requests"`
	TotalTokens     int64  `json:"total_tokens"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
}

// QueryPerAccountCycles returns usage stats per account within each account's current quota cycle.
// Cycle end = COALESCE(next_retry_after, rl_reset_requests), cycle start = cycle_end - 7 days.
func QueryPerAccountCycles(ctx context.Context, db *sql.DB) ([]AccountCycleStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			a.auth_index,
			COALESCE(a.email, a.name) as email,
			datetime(COALESCE(a.next_retry_after, a.rl_reset_requests), '-7 days') as cycle_start,
			COALESCE(a.next_retry_after, a.rl_reset_requests) as cycle_end,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN 1 ELSE 0 END), 0) as success_requests,
			COALESCE(SUM(CASE WHEN u.failed=1 THEN 1 ELSE 0 END), 0) as failed_requests,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.total_tokens ELSE 0 END), 0) as total_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.input_tokens ELSE 0 END), 0) as input_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.output_tokens ELSE 0 END), 0) as output_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.reasoning_tokens ELSE 0 END), 0) as reasoning_tokens
		FROM account_states a
		LEFT JOIN usage_records u
			ON u.auth_index = a.auth_index
			AND u.is_keepalive = 0
			AND u.timestamp >= datetime(COALESCE(a.next_retry_after, a.rl_reset_requests), '-7 days')
			AND u.timestamp < COALESCE(a.next_retry_after, a.rl_reset_requests)
		WHERE COALESCE(a.next_retry_after, a.rl_reset_requests) IS NOT NULL
		GROUP BY a.auth_index
		ORDER BY total_tokens DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AccountCycleStat
	for rows.Next() {
		var s AccountCycleStat
		if err := rows.Scan(&s.AuthIndex, &s.Email, &s.CycleStart, &s.CycleEnd,
			&s.SuccessRequests, &s.FailedRequests, &s.TotalTokens,
			&s.InputTokens, &s.OutputTokens, &s.ReasoningTokens); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, nil
}

// QueryByDateRange returns aggregated stats and detailed logs for a date range.
func QueryByDateRange(ctx context.Context, db *sql.DB, startDate, endDate string) (*AggregatedStats, []UsageRecord, error) {
	stats := &AggregatedStats{}

	// Aggregated stats for the date range
	row := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(total_tokens),0),
			COALESCE(SUM(input_tokens),0),
			COALESCE(SUM(output_tokens),0),
			COALESCE(SUM(reasoning_tokens),0),
			COALESCE(SUM(cached_tokens),0),
			COALESCE(SUM(CASE WHEN failed=1 THEN 1 ELSE 0 END),0)
		FROM usage_records
		WHERE DATE(timestamp) >= ? AND DATE(timestamp) <= ?`,
		startDate, endDate)
	if err := row.Scan(
		&stats.TotalRequests,
		&stats.TotalTokens,
		&stats.InputTokens,
		&stats.OutputTokens,
		&stats.ReasoningTokens,
		&stats.CachedTokens,
		&stats.FailedRequests,
	); err != nil {
		return nil, nil, err
	}

	// By model
	rows, err := db.QueryContext(ctx, `
		SELECT model, COUNT(*), COALESCE(SUM(total_tokens),0), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0)
		FROM usage_records
		WHERE DATE(timestamp) >= ? AND DATE(timestamp) <= ?
		GROUP BY model ORDER BY COUNT(*) DESC LIMIT 50`,
		startDate, endDate)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ms ModelStat
		if err := rows.Scan(&ms.Model, &ms.Requests, &ms.TotalTokens, &ms.InputTokens, &ms.OutputTokens); err != nil {
			return nil, nil, err
		}
		stats.ByModel = append(stats.ByModel, ms)
	}

	// By auth index
	rows2, err := db.QueryContext(ctx, `
		SELECT COALESCE(auth_index,''), COALESCE(auth_id,''), COUNT(*), COALESCE(SUM(total_tokens),0)
		FROM usage_records
		WHERE DATE(timestamp) >= ? AND DATE(timestamp) <= ?
		GROUP BY auth_index ORDER BY COUNT(*) DESC LIMIT 100`,
		startDate, endDate)
	if err != nil {
		return nil, nil, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var as AuthIndexStat
		if err := rows2.Scan(&as.AuthIndex, &as.AuthID, &as.Requests, &as.TotalTokens); err != nil {
			return nil, nil, err
		}
		stats.ByAuthIndex = append(stats.ByAuthIndex, as)
	}

	// Detailed logs (ordered by timestamp desc, limit 1000)
	rows3, err := db.QueryContext(ctx, `
		SELECT timestamp, COALESCE(source,''), model, failed, is_keepalive,
			   input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM usage_records
		WHERE DATE(timestamp) >= ? AND DATE(timestamp) <= ?
		ORDER BY timestamp DESC LIMIT 1000`,
		startDate, endDate)
	if err != nil {
		return nil, nil, err
	}
	defer rows3.Close()

	var logs []UsageRecord
	for rows3.Next() {
		var rec UsageRecord
		var failedInt, keepaliveInt int
		if err := rows3.Scan(&rec.Timestamp, &rec.Source, &rec.Model, &failedInt, &keepaliveInt,
			&rec.InputTokens, &rec.OutputTokens, &rec.ReasoningTokens, &rec.CachedTokens, &rec.TotalTokens); err != nil {
			return nil, nil, err
		}
		rec.Failed = failedInt == 1
		rec.IsKeepalive = keepaliveInt == 1
		logs = append(logs, rec)
	}

	return stats, logs, nil
}
