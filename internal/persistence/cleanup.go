package persistence

import (
	"database/sql"
	"time"

	log "github.com/sirupsen/logrus"
)

// StartCleanupTask launches a goroutine that runs daily cleanup.
// It deletes records older than retentionPeriod from usage_records.
func StartCleanupTask(db *sql.DB, retentionPeriod time.Duration) {
	go func() {
		for {
			// Sleep until next 2am local time
			now := time.Now()
			next := time.Date(now.Year(), now.Month(), now.Day()+1, 2, 0, 0, 0, now.Location())
			time.Sleep(time.Until(next))

			n, err := DeleteOld(db, retentionPeriod)
			if err != nil {
				log.Errorf("persistence: cleanup failed: %v", err)
			} else {
				log.Infof("persistence: cleanup deleted %d old usage records (retention=%s)", n, retentionPeriod)
			}
		}
	}()
}
