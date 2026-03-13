package persistence

import (
	"database/sql"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	log "github.com/sirupsen/logrus"
)

// SeedUsageStats reads all usage records from SQLite and merges them into the
// in-memory RequestStatistics store so that /v0/management/usage reflects
// historical data immediately after restart.
func SeedUsageStats(db *sql.DB, stats *usage.RequestStatistics) error {
	if db == nil || stats == nil {
		return nil
	}

	rows, err := db.Query(`
		SELECT
			COALESCE(api_key,''),
			COALESCE(model,'unknown'),
			timestamp,
			COALESCE(source,''),
			COALESCE(auth_index,''),
			input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens,
			failed
		FROM usage_records
		ORDER BY timestamp ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Build StatisticsSnapshot from SQLite rows
	snapshot := usage.StatisticsSnapshot{
		APIs:           make(map[string]usage.APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}

	for rows.Next() {
		var (
			apiKey, model, tsStr, source, authIndex string
			in, out, reason, cache, total           int64
			failed                                  int
		)
		if err := rows.Scan(&apiKey, &model, &tsStr, &source, &authIndex,
			&in, &out, &reason, &cache, &total, &failed); err != nil {
			continue
		}

		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, tsStr)
			if err != nil {
				ts = time.Now()
			}
		}

		if apiKey == "" {
			apiKey = "unknown"
		}

		detail := usage.RequestDetail{
			Timestamp: ts,
			Source:    source,
			AuthIndex: authIndex,
			Failed:    failed != 0,
			Tokens: usage.TokenStats{
				InputTokens:     in,
				OutputTokens:    out,
				ReasoningTokens: reason,
				CachedTokens:    cache,
				TotalTokens:     total,
			},
		}

		apiSnap, ok := snapshot.APIs[apiKey]
		if !ok {
			apiSnap = usage.APISnapshot{Models: make(map[string]usage.ModelSnapshot)}
		}
		modelSnap := apiSnap.Models[model]
		modelSnap.Details = append(modelSnap.Details, detail)
		apiSnap.Models[model] = modelSnap
		snapshot.APIs[apiKey] = apiSnap
	}
	if err := rows.Err(); err != nil {
		return err
	}

	result := stats.MergeSnapshot(snapshot)
	log.Infof("persistence: seeded %d usage records into memory (skipped %d duplicates)",
		result.Added, result.Skipped)
	return nil
}
