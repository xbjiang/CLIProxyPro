package persistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
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
	TotalRequests   int64           `json:"total_requests"`
	TotalTokens     int64           `json:"total_tokens"`
	InputTokens     int64           `json:"input_tokens"`
	OutputTokens    int64           `json:"output_tokens"`
	ReasoningTokens int64           `json:"reasoning_tokens"`
	CachedTokens    int64           `json:"cached_tokens"`
	FailedRequests  int64           `json:"failed_requests"`
	ByModel         []ModelStat     `json:"by_model"`
	ByAuthIndex     []AuthIndexStat `json:"by_auth_index"`
}

type ModelStat struct {
	Model        string `json:"model"`
	Requests     int64  `json:"requests"`
	TotalTokens  int64  `json:"total_tokens"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
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
			DATE(timestamp, 'localtime') as day,
			COUNT(*),
			SUM(CASE WHEN failed=0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN failed=1 THEN 1 ELSE 0 END),
			COALESCE(SUM(total_tokens),0),
			COALESCE(SUM(input_tokens),0),
			COALESCE(SUM(output_tokens),0),
			COALESCE(SUM(reasoning_tokens),0),
			COALESCE(SUM(cached_tokens),0)
		FROM usage_records
		WHERE datetime(timestamp, 'localtime') >= datetime('now', 'localtime', ? || ' days')
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
	WindowType      string `json:"window_type"`
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
// Primary: use account_reset_history to find the previous reset time (precise cycle boundary).
// Fallback: if no history, use [cycle_end - 7 days, cycle_end) approximation.
// For Plus accounts (percent mode: rl_limit_requests=100 AND rl_limit_tokens=100), use the
// secondary window (rl_reset_tokens) as cycle boundary since the primary window is only ~5 hours.
// activeIndexes: if non-empty, only include accounts whose auth_index is in this list (filters ghost records).
func QueryPerAccountCycles(ctx context.Context, db *sql.DB, activeIndexes []string) ([]AccountCycleStat, error) {
	indexCond := ""
	args := make([]interface{}, 0, len(activeIndexes))
	if len(activeIndexes) > 0 {
		ph := make([]string, len(activeIndexes))
		for i, idx := range activeIndexes {
			ph[i] = "?"
			args = append(args, idx)
		}
		indexCond = " WHERE a.auth_index IN (" + strings.Join(ph, ",") + ")"
	}
	rows, err := db.QueryContext(ctx, `
		WITH anchors AS MATERIALIZED (
			SELECT
				a.auth_index,
				COALESCE(a.email, a.name) as email,
				COALESCE(NULLIF(a.rl_reset_requests, ''), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) as cycle_end,
				'primary' as wtype,
				(SELECT h.rl_reset_requests FROM account_reset_history h
				 WHERE h.auth_index = a.auth_index
				   AND COALESCE(h.window_type, 'primary') = 'primary'
				   AND h.rl_reset_requests < COALESCE(NULLIF(a.rl_reset_requests, ''), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
				   AND h.rl_reset_requests < strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
				 ORDER BY h.rl_reset_requests DESC LIMIT 1
				) as prev_reset,
				(a.rl_limit_requests = 100 AND a.rl_limit_tokens = 100 AND a.rl_reset_tokens IS NOT NULL AND a.rl_reset_tokens != '') as is_plus
			FROM account_states a`+indexCond+`
			UNION ALL
			SELECT
				a.auth_index,
				COALESCE(a.email, a.name) as email,
				a.rl_reset_tokens as cycle_end,
				'secondary' as wtype,
				(SELECT h.rl_reset_requests FROM account_reset_history h
				 WHERE h.auth_index = a.auth_index
				   AND h.window_type = 'secondary'
				   AND h.rl_reset_requests < a.rl_reset_tokens
				   AND h.rl_reset_requests < strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
				 ORDER BY h.rl_reset_requests DESC LIMIT 1
				) as prev_reset,
				1 as is_plus
			FROM account_states a
			WHERE a.rl_limit_requests = 100 AND a.rl_limit_tokens = 100 AND a.rl_reset_tokens IS NOT NULL AND a.rl_reset_tokens != ''
			`+strings.ReplaceAll(indexCond, "WHERE a.", "AND a.")+`
		),
		current_cycle AS MATERIALIZED (
			SELECT auth_index, email, wtype,
				COALESCE(prev_reset, strftime('%Y-%m-%dT%H:%M:%fZ', cycle_end, CASE WHEN wtype = 'primary' AND is_plus = 1 THEN '-5 hours' ELSE '-7 days' END)) as cycle_start,
				cycle_end
			FROM anchors
		)
		SELECT
			cc.auth_index,
			cc.email,
			cc.wtype,
			cc.cycle_start,
			cc.cycle_end,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN 1 ELSE 0 END), 0) as success_requests,
			COALESCE(SUM(CASE WHEN u.failed=1 THEN 1 ELSE 0 END), 0) as failed_requests,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.total_tokens ELSE 0 END), 0) as total_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.input_tokens ELSE 0 END), 0) as input_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.output_tokens ELSE 0 END), 0) as output_tokens,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.reasoning_tokens ELSE 0 END), 0) as reasoning_tokens
		FROM current_cycle cc
		LEFT JOIN usage_records u
			ON u.auth_index = cc.auth_index
			AND u.is_keepalive = 0
			AND u.timestamp >= cc.cycle_start
			AND u.timestamp < cc.cycle_end
		GROUP BY cc.auth_index, cc.wtype
		ORDER BY total_tokens DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AccountCycleStat
	for rows.Next() {
		var s AccountCycleStat
		if err := rows.Scan(&s.AuthIndex, &s.Email, &s.WindowType, &s.CycleStart, &s.CycleEnd,
			&s.SuccessRequests, &s.FailedRequests, &s.TotalTokens,
			&s.InputTokens, &s.OutputTokens, &s.ReasoningTokens); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, nil
}

// CycleHistoryItem represents one cycle's aggregated usage for an account.
type CycleHistoryItem struct {
	CycleNum        int    `json:"cycle_num"` // 0=current, 1=previous, ...
	CycleStart      string `json:"cycle_start"`
	CycleEnd        string `json:"cycle_end"`
	SuccessRequests int64  `json:"success_requests"`
	FailedRequests  int64  `json:"failed_requests"`
	TotalTokens     int64  `json:"total_tokens"`
}

// AccountCycleHistory represents an account with its multi-cycle usage history.
type AccountCycleHistory struct {
	AuthIndex  string             `json:"auth_index"`
	Email      string             `json:"email"`
	WindowType string             `json:"window_type"`
	CycleEnd   string             `json:"cycle_end"`
	Cycles     []CycleHistoryItem `json:"cycles"`
}

// QueryAccountCycleHistory returns multi-cycle usage history per account.
// Primary: use account_reset_history for precise cycle boundaries. Each recorded
// rl_reset_requests is a cycle_end; the previous recorded reset is the cycle_start.
// Fallback: for the oldest recorded cycle (no previous reset), use cycle_end - 7d.
// For Plus accounts (percent mode), use secondary window resets for cycle boundaries.
// activeIndexes: if non-empty, only include accounts whose auth_index is in this list (filters ghost records).
func QueryAccountCycleHistory(ctx context.Context, db *sql.DB, maxCycles int, activeIndexes []string) ([]AccountCycleHistory, error) {
	if maxCycles <= 0 {
		maxCycles = 10
	}
	indexCond := ""
	args := make([]interface{}, 0, len(activeIndexes)+1)
	if len(activeIndexes) > 0 {
		ph := make([]string, len(activeIndexes))
		for i, idx := range activeIndexes {
			ph[i] = "?"
			args = append(args, idx)
		}
		indexCond = " AND a.auth_index IN (" + strings.Join(ph, ",") + ")"
	}
	args = append(args, maxCycles)
	rows, err := db.QueryContext(ctx, `
		WITH resets AS MATERIALIZED (
			SELECT
				h.auth_index,
				COALESCE(a.email, a.name) as email,
				h.rl_reset_requests as cycle_end,
				LAG(h.rl_reset_requests) OVER (PARTITION BY h.auth_index, COALESCE(h.window_type, 'primary') ORDER BY h.rl_reset_requests) as prev_reset,
				ROW_NUMBER() OVER (PARTITION BY h.auth_index, COALESCE(h.window_type, 'primary') ORDER BY h.rl_reset_requests DESC) - 1 as cycle_num,
				COALESCE(h.window_type, 'primary') as wtype,
				(a.rl_limit_requests = 100 AND a.rl_limit_tokens = 100 AND a.rl_reset_tokens IS NOT NULL AND a.rl_reset_tokens != '') as is_plus
			FROM account_reset_history h
			JOIN account_states a ON a.auth_index = h.auth_index `+indexCond+`
		),
		cycles AS MATERIALIZED (
			SELECT
				r.auth_index, r.email, r.cycle_num, r.wtype,
				COALESCE(r.prev_reset, strftime('%Y-%m-%dT%H:%M:%fZ', r.cycle_end, CASE WHEN r.is_plus = 1 AND r.wtype = 'primary' THEN '-5 hours' ELSE '-7 days' END)) as cycle_start,
				r.cycle_end
			FROM resets r
			WHERE r.cycle_num < ?
		)
		SELECT
			c.auth_index, c.email, c.wtype, c.cycle_num, c.cycle_start, c.cycle_end,
			COALESCE(SUM(CASE WHEN u.failed=0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN u.failed=1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN u.failed=0 THEN u.total_tokens ELSE 0 END), 0)
		FROM cycles c
		LEFT JOIN usage_records u
			ON u.auth_index = c.auth_index
			AND u.is_keepalive = 0
			AND u.timestamp >= c.cycle_start
			AND u.timestamp < c.cycle_end
		GROUP BY c.auth_index, c.wtype, c.cycle_num
		ORDER BY c.auth_index, c.wtype, c.cycle_num ASC`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type flatRow struct {
		authIndex       string
		email           string
		wtype           string
		cycleNum        int
		cycleStart      string
		cycleEnd        string
		successRequests int64
		failedRequests  int64
		totalTokens     int64
	}
	var flat []flatRow
	for rows.Next() {
		var r flatRow
		if err := rows.Scan(&r.authIndex, &r.email, &r.wtype, &r.cycleNum, &r.cycleStart, &r.cycleEnd,
			&r.successRequests, &r.failedRequests, &r.totalTokens); err != nil {
			return nil, err
		}
		flat = append(flat, r)
	}

	indexMap := make(map[string]*AccountCycleHistory)
	var order []string
	for _, r := range flat {
		key := r.authIndex + "|" + r.wtype
		h, ok := indexMap[key]
		if !ok {
			h = &AccountCycleHistory{
				AuthIndex:  r.authIndex,
				Email:      r.email,
				WindowType: r.wtype,
			}
			indexMap[key] = h
			order = append(order, key)
		}
		h.Cycles = append(h.Cycles, CycleHistoryItem{
			CycleNum:        r.cycleNum,
			CycleStart:      r.cycleStart,
			CycleEnd:        r.cycleEnd,
			SuccessRequests: r.successRequests,
			FailedRequests:  r.failedRequests,
			TotalTokens:     r.totalTokens,
		})
	}

	var result []AccountCycleHistory
	for _, key := range order {
		result = append(result, *indexMap[key])
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
		WHERE DATE(timestamp, 'localtime') >= ? AND DATE(timestamp, 'localtime') <= ?`,
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
		WHERE DATE(timestamp, 'localtime') >= ? AND DATE(timestamp, 'localtime') <= ?
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
		WHERE DATE(timestamp, 'localtime') >= ? AND DATE(timestamp, 'localtime') <= ?
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
		WHERE DATE(timestamp, 'localtime') >= ? AND DATE(timestamp, 'localtime') <= ?
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
