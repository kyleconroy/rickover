// Logic for interacting with the "queued_jobs" table.
package queued_jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kevinburke/go-dberror"
	"github.com/kevinburke/go-types"
	models "github.com/kevinburke/rickover/newmodels"
)

const Prefix = "job_"

// ErrNotFound indicates that the job was not found.
var ErrNotFound = errors.New("queued_jobs: job not found")

// UnknownOrArchivedError is raised when the job type is unknown or the job has
// already been archived. It's unfortunate we can't distinguish these, but more
// important to minimize the total number of queries to the database.
type UnknownOrArchivedError struct {
	Err string
}

func (e *UnknownOrArchivedError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Err
}

// StuckJobLimit is the maximum number of stuck jobs to fetch in one database
// query.
var StuckJobLimit = 100

// Enqueue creates a new queued job with the given ID and fields. A
// dberror.Error will be returned if Postgres returns a constraint failure -
// job exists, job name unknown, &c. A sql.ErrNoRows will be returned if the
// `name` does not exist in the jobs table. Otherwise the QueuedJob will be
// returned.
func Enqueue(id types.PrefixUUID, name string, runAfter time.Time, expiresAt types.NullTime, data json.RawMessage) (*models.QueuedJob, error) {
	qj, err := models.DB.EnqueueJob(context.Background(), id, name, runAfter, expiresAt, []byte(data))
	if err != nil {
		if err == sql.ErrNoRows {
			e := &UnknownOrArchivedError{
				Err: fmt.Sprintf("Job type %s does not exist or the job with that id has already been archived", name),
			}
			return nil, e
		}
		return nil, dberror.GetError(err)
	}
	qj.ID.Prefix = Prefix
	return &qj, err
}

// Get the queued job with the given id. Returns the job, or an error. If no
// record could be found, the error will be `queued_jobs.ErrNotFound`.
func Get(id types.PrefixUUID) (*models.QueuedJob, error) {
	qj, err := models.DB.GetQueuedJob(context.Background(), id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, dberror.GetError(err)
	}
	qj.ID.Prefix = Prefix
	return &qj, nil
}

// GetRetry attempts to retrieve the job attempts times before giving up.
func GetRetry(id types.PrefixUUID, attempts uint8) (job *models.QueuedJob, err error) {
	for i := uint8(0); i < attempts; i++ {
		job, err = Get(id)
		if err == nil || err == ErrNotFound {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return
}

// Delete deletes the given queued job. Returns nil if the job was deleted
// successfully. If no job exists to be deleted, sql.ErrNoRows is returned.
func Delete(id types.PrefixUUID) error {
	rows, err := models.DB.DeleteQueuedJob(context.Background(), id)
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	} else if rows == 1 {
		return nil
	} else {
		// This should not be possible because of database constraints
		return fmt.Errorf("queued_jobs: multiple rows (%d) deleted for job %s, please investigate", rows, id)
	}
}

// DeleteRetry attempts to Delete the item `attempts` times.
func DeleteRetry(id types.PrefixUUID, attempts uint8) error {
	for i := uint8(0); i < attempts; i++ {
		err := Delete(id)
		if err == nil || err == ErrNotFound {
			return err
		}
	}
	return nil
}

// Acquire a queued job with the given name that's able to run now. Returns
// the queued job and a boolean indicating whether the SELECT query found
// a row, or a generic error/sql.ErrNoRows if no jobs are available.
func Acquire(name string) (*models.QueuedJob, error) {
	qjs, err := models.DB.AcquireJobs(context.Background(), name)
	if err != nil {
		err = dberror.GetError(err)
		return nil, err
	}
	if len(qjs) == 0 {
		return nil, sql.ErrNoRows
	}
	if len(qjs) > 1 {
		fmt.Println(time.Now().UTC())
		panic(fmt.Sprintf("Too many rows affected by Acquire for '%s': %d", name, len(qjs)))
	}
	qjs[0].ID.Prefix = Prefix
	return &qjs[0], nil
}

// Decrement decrements the attempts counter for an existing job, and sets
// its status back to 'queued'. If the queued job does not exist, or the
// attempts counter in the database does not match the passed in attempts
// value, sql.ErrNoRows will be returned.
//
// attempts: The current value of the `attempts` column, the returned attempts
// value will be this number minus 1.
func Decrement(id types.PrefixUUID, attempts uint8, runAfter time.Time) (*models.QueuedJob, error) {
	qj, err := models.DB.DecrementJobAttempts(context.Background(), id, attempts, runAfter)
	if err != nil {
		err = dberror.GetError(err)
		return nil, err
	}
	qj.ID.Prefix = Prefix
	return &qj, nil
}

// GetOldInProgressJobs finds queued in-progress jobs with an updated_at
// timestamp older than olderThan. A maximum of StuckJobLimit jobs will be
// returned.
func GetOldInProgressJobs(olderThan time.Time) ([]*models.QueuedJob, error) {
	rows, err := models.DB.GetOldInProgressJobs(context.Background(), olderThan)
	var jobs []*models.QueuedJob
	if err != nil {
		return jobs, err
	}
	for _, qj := range rows {
		j := qj
		j.ID.Prefix = Prefix
		jobs = append(jobs, &j)
	}
	return jobs, err
}

// CountReadyAndAll returns the total number of queued and ready jobs in the
// table.
func CountReadyAndAll() (int, int, error) {
	row, err := models.DB.CountReadyAndAll(context.Background())
	return row.AllCountCount, row.ReadyCountCount, err
}

// GetCountsByStatus returns a map with each job type as the key, followed by
// the number of <status> jobs it has. For example:
//
// "echo": 5,
// "remind-assigned-driver": 7,
func GetCountsByStatus(status models.JobStatus) (map[string]int64, error) {
	rows, err := models.DB.GetCountsByStatus(context.Background(), status)
	m := make(map[string]int64)
	if err != nil {
		return m, err
	}
	for _, row := range rows {
		m[row.Name] = int64(row.Count)
	}
	return m, nil
}
