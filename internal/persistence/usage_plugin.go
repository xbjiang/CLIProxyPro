package persistence

import (
	"context"
	"database/sql"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/kacontext"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// SQLitePersistencePlugin implements coreusage.Plugin, persisting every usage
// record to SQLite with deduplication via dedup_hash.
type SQLitePersistencePlugin struct {
	db *sql.DB
}

// NewSQLitePersistencePlugin creates a new plugin backed by the given db.
func NewSQLitePersistencePlugin(db *sql.DB) *SQLitePersistencePlugin {
	return &SQLitePersistencePlugin{db: db}
}

// HandleUsage implements coreusage.Plugin.
func (p *SQLitePersistencePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || p.db == nil {
		return
	}
	hash := DedupHash(record)
	isKA := kacontext.IsKeepaliveContext(ctx)
	if err := InsertUsageRecord(p.db, hash, record, isKA); err != nil {
		log.Debugf("persistence: insert usage record: %v", err)
	}
}
