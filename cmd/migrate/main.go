// Command migrate is a one-time tool that copies usage_records and account_states
// from an old Node.js SQLite database (codex-farm .data/stats/usage.db) into the
// new CLIProxyPro SQLite database (~/.cli-proxy-api/stats.db).
//
// Usage:
//
//	./migrate --src=/path/to/old/usage.db --dst=~/.cli-proxy-api/stats.db
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
	_ "modernc.org/sqlite"
)

func main() {
	srcPath := flag.String("src", "", "path to old Node.js usage.db")
	dstPath := flag.String("dst", "", "path to new CLIProxyPro stats.db (will be created if absent)")
	flag.Parse()

	if *srcPath == "" || *dstPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	src, err := sql.Open("sqlite", *srcPath+"?_journal_mode=WAL")
	if err != nil {
		log.Fatalf("open src: %v", err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite", expandHome(*dstPath)+"?_journal_mode=WAL")
	if err != nil {
		log.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	// Ensure schema exists in destination (idempotent)
	if err := persistence.InitSchema(dst); err != nil {
		log.Fatalf("init dst schema: %v", err)
	}

	// Migrate usage_records
	urOK, urSkip, urFail := migrateUsageRecords(src, dst)
	fmt.Printf("usage_records: imported=%d  skipped=%d  failed=%d\n", urOK, urSkip, urFail)

	// Migrate account_states
	asOK, asSkip, asFail := migrateAccountStates(src, dst)
	fmt.Printf("account_states: imported=%d  skipped=%d  failed=%d\n", asOK, asSkip, asFail)

	// Migrate settings
	sOK, sSkip, sFail := migrateSettings(src, dst)
	fmt.Printf("settings: imported=%d  skipped=%d  failed=%d\n", sOK, sSkip, sFail)
}

func migrateUsageRecords(src, dst *sql.DB) (ok, skip, fail int) {
	rows, err := src.Query(`
		SELECT dedup_hash, api_key, model, timestamp, source, auth_index,
		       input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, failed
		FROM usage_records`)
	if err != nil {
		log.Printf("query src usage_records: %v", err)
		return
	}
	defer rows.Close()

	stmt, err := dst.Prepare(`
		INSERT OR IGNORE INTO usage_records
			(dedup_hash, api_key, model, timestamp, source, auth_index,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, failed, is_keepalive)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)`)
	if err != nil {
		log.Printf("prepare dst insert: %v", err)
		return
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			hash, apiKey, model, ts, source, authIndex sql.NullString
			in, out, reason, cache, total, failed       int64
		)
		if err := rows.Scan(&hash, &apiKey, &model, &ts, &source, &authIndex,
			&in, &out, &reason, &cache, &total, &failed); err != nil {
			fail++
			continue
		}
		// Normalise timestamp to UTC RFC3339Nano
		normTS := normaliseTimestamp(ts.String)

		res, err := stmt.Exec(hash.String, apiKey.String, model.String, normTS,
			source.String, authIndex.String, in, out, reason, cache, total, failed)
		if err != nil {
			fail++
			continue
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			ok++
		} else {
			skip++
		}
	}
	return
}

func migrateAccountStates(src, dst *sql.DB) (ok, skip, fail int) {
	// Check which columns exist in the source table
	rows, err := src.Query(`
		SELECT auth_index, COALESCE(auth_id,''), COALESCE(email,''), COALESCE(name,''),
		       unavailable, disabled, next_retry_after,
		       COALESCE(status,'unknown'), COALESCE(status_message,''),
		       updated_at
		FROM account_states`)
	if err != nil {
		log.Printf("query src account_states: %v", err)
		return
	}
	defer rows.Close()

	stmt, err := dst.Prepare(`
		INSERT INTO account_states
			(auth_index, auth_id, email, name, label, unavailable, disabled,
			 next_retry_after, quota_backoff_lvl, status, status_message, updated_at)
		VALUES (?,?,?,?,?,?,?,?,0,?,?,?)
		ON CONFLICT(auth_index) DO UPDATE SET
			auth_id=excluded.auth_id, email=excluded.email, name=excluded.name,
			label=excluded.label, unavailable=excluded.unavailable, disabled=excluded.disabled,
			next_retry_after=excluded.next_retry_after,
			status=excluded.status, status_message=excluded.status_message,
			updated_at=excluded.updated_at`)
	if err != nil {
		log.Printf("prepare dst account_states: %v", err)
		return
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			authIndex, authID, email, name sql.NullString
			unav, dis                      int
			nra, status, statusMsg, updAt  sql.NullString
		)
		if err := rows.Scan(&authIndex, &authID, &email, &name, &unav, &dis, &nra,
			&status, &statusMsg, &updAt); err != nil {
			fail++
			continue
		}
		// Use name as label fallback
		label := name.String
		_, err := stmt.Exec(authIndex.String, authID.String, email.String, name.String, label,
			unav, dis, nra, status.String, statusMsg.String, updAt.String)
		if err != nil {
			fail++
		} else {
			ok++
		}
	}
	_ = rows.Err()
	return
}

func migrateSettings(src, dst *sql.DB) (ok, skip, fail int) {
	rows, err := src.Query(`SELECT key, value FROM settings`)
	if err != nil {
		// Table may not exist in old DB
		return
	}
	defer rows.Close()

	for rows.Next() {
		var k, v sql.NullString
		if err := rows.Scan(&k, &v); err != nil {
			fail++
			continue
		}
		res, err := dst.Exec(
			`INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING`,
			k.String, v.String)
		if err != nil {
			fail++
			continue
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			ok++
		} else {
			skip++
		}
	}
	return
}

func normaliseTimestamp(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format(time.RFC3339Nano)
		}
	}
	return s
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return home + p[1:]
		}
	}
	return p
}
