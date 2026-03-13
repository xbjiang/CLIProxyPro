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
