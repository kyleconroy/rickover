// Logic for interacting with the "archived_jobs" table.
package archived_jobs

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/kevinburke/go-dberror"
	"github.com/kevinburke/go-types"
	"github.com/kevinburke/rickover/models/queued_jobs"
	models "github.com/kevinburke/rickover/newmodels"
)

const Prefix = "job_"

// ErrNotFound indicates that the archived job was not found.
var ErrNotFound = errors.New("archived_jobs: job not found")

// Create an archived job with the given id, status, and attempts. Assumes that
// the job already exists in the queued_jobs table; the `data` field is copied
// from there. If the job does not exist, queued_jobs.ErrNotFound is returned.
func Create(id types.PrefixUUID, name string, status models.ArchivedJobStatus, attempt uint8) (*models.ArchivedJob, error) {
	aj, err := models.DB.CreateArchivedJob(context.Background(), id, name, status, attempt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, queued_jobs.ErrNotFound
		}
		err = dberror.GetError(err)
		return nil, err
	}
	aj.ID.Prefix = Prefix
	return &aj, nil
}

// Get returns the archived job with the given id, or sql.ErrNoRows if it's
// not present.
func Get(id types.PrefixUUID) (*models.ArchivedJob, error) {
	aj, err := models.DB.GetArchivedJob(context.Background(), id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, dberror.GetError(err)
	}
	aj.ID.Prefix = Prefix
	return &aj, nil
}

// GetRetry attempts to retrieve the job attempts times before giving up.
func GetRetry(id types.PrefixUUID, attempts uint8) (job *models.ArchivedJob, err error) {
	for i := uint8(0); i < attempts; i++ {
		job, err = Get(id)
		if err == nil || err == ErrNotFound {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return
}
