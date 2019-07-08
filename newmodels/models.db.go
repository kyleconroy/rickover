package newmodels

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/kevinburke/go-types"
)

type DeliveryStrategy string

const (
	DeliveryStrategyAtMostOnce  DeliveryStrategy = "at_most_once"
	DeliveryStrategyAtLeastOnce                  = "at_least_once"
)

type JobStatus string

const (
	JobStatusQueued     JobStatus = "queued"
	JobStatusInProgress           = "in-progress"
)

type ArchivedJobStatus string

const (
	ArchivedJobStatusSucceeded ArchivedJobStatus = "succeeded"
	ArchivedJobStatusFailed                      = "failed"
	ArchivedJobStatusExpired                     = "expired"
)

type Job struct {
	Name             string           `json:"name"`
	DeliveryStrategy DeliveryStrategy `json:"delivery_strategy"`
	Attempts         uint8            `json:"attempts"`
	Concurrency      uint8            `json:"concurrency"`
	CreatedAt        time.Time        `json:"created_at"`
}

type QueuedJob struct {
	ID        types.PrefixUUID `json:"id"`
	Name      string           `json:"name"`
	Attempts  uint8            `json:"attempts"`
	RunAfter  time.Time        `json:"run_after"`
	ExpiresAt types.NullTime   `json:"expires_at"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Status    JobStatus        `json:"status"`
	Data      json.RawMessage  `json:"data"`
}

type ArchivedJob struct {
	ID        types.PrefixUUID  `json:"id"`
	Name      string            `json:"name"`
	Attempts  uint8             `json:"attempts"`
	Status    ArchivedJobStatus `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
	Data      json.RawMessage   `json:"data"`
	ExpiresAt types.NullTime    `json:"expires_at"`
}

type dbtx interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func New(db dbtx) *Queries {
	return &Queries{db: db}
}

func Prepare(ctx context.Context, db dbtx) (*Queries, error) {
	q := Queries{db: db}
	var err error
	if q.acquireJob, err = db.PrepareContext(ctx, acquireJob); err != nil {
		return nil, err
	}
	if q.countReadyAndAll, err = db.PrepareContext(ctx, countReadyAndAll); err != nil {
		return nil, err
	}
	if q.createArchivedJob, err = db.PrepareContext(ctx, createArchivedJob); err != nil {
		return nil, err
	}
	if q.createJob, err = db.PrepareContext(ctx, createJob); err != nil {
		return nil, err
	}
	if q.decrementJobAttempts, err = db.PrepareContext(ctx, decrementJobAttempts); err != nil {
		return nil, err
	}
	if q.deleteQueuedJob, err = db.PrepareContext(ctx, deleteQueuedJob); err != nil {
		return nil, err
	}
	if q.enqueueJob, err = db.PrepareContext(ctx, enqueueJob); err != nil {
		return nil, err
	}
	if q.getAllJobs, err = db.PrepareContext(ctx, getAllJobs); err != nil {
		return nil, err
	}
	if q.getArchivedJob, err = db.PrepareContext(ctx, getArchivedJob); err != nil {
		return nil, err
	}
	if q.getCountsByStatus, err = db.PrepareContext(ctx, getCountsByStatus); err != nil {
		return nil, err
	}
	if q.getJob, err = db.PrepareContext(ctx, getJob); err != nil {
		return nil, err
	}
	if q.getOldInProgressJobs, err = db.PrepareContext(ctx, getOldInProgressJobs); err != nil {
		return nil, err
	}
	if q.getQueuedJob, err = db.PrepareContext(ctx, getQueuedJob); err != nil {
		return nil, err
	}
	return &q, nil
}

type Queries struct {
	db                   dbtx
	tx                   *sql.Tx
	acquireJob           *sql.Stmt
	countReadyAndAll     *sql.Stmt
	createArchivedJob    *sql.Stmt
	createJob            *sql.Stmt
	decrementJobAttempts *sql.Stmt
	deleteQueuedJob      *sql.Stmt
	enqueueJob           *sql.Stmt
	getAllJobs           *sql.Stmt
	getArchivedJob       *sql.Stmt
	getCountsByStatus    *sql.Stmt
	getJob               *sql.Stmt
	getOldInProgressJobs *sql.Stmt
	getQueuedJob         *sql.Stmt
}

func (q *Queries) WithTx(tx *sql.Tx) *Queries {
	return &Queries{
		db:                   tx,
		tx:                   tx,
		acquireJob:           q.acquireJob,
		countReadyAndAll:     q.countReadyAndAll,
		createArchivedJob:    q.createArchivedJob,
		createJob:            q.createJob,
		decrementJobAttempts: q.decrementJobAttempts,
		deleteQueuedJob:      q.deleteQueuedJob,
		enqueueJob:           q.enqueueJob,
		getAllJobs:           q.getAllJobs,
		getArchivedJob:       q.getArchivedJob,
		getCountsByStatus:    q.getCountsByStatus,
		getJob:               q.getJob,
		getOldInProgressJobs: q.getOldInProgressJobs,
		getQueuedJob:         q.getQueuedJob,
	}
}

const acquireJob = `-- name: AcquireJob :one
WITH queued_job as (
	SELECT id AS inner_id
	FROM queued_jobs
	WHERE status='queued'
		AND name = $1
		AND run_after <= now()
	ORDER BY created_at ASC
	LIMIT 1
	FOR UPDATE
) UPDATE queued_jobs
SET status='in-progress',
	updated_at=now()
FROM queued_job
WHERE queued_jobs.id = queued_job.inner_id 
	AND status='queued'
RETURNING id, name, attempts, run_after, expires_at, created_at, updated_at, status, data
`

func (q *Queries) AcquireJob(ctx context.Context) (QueuedJob, error) {
	var row *sql.Row
	switch {
	case q.acquireJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.acquireJob).QueryRowContext(ctx)
	case q.acquireJob != nil:
		row = q.acquireJob.QueryRowContext(ctx)
	default:
		row = q.db.QueryRowContext(ctx, acquireJob)
	}
	var i QueuedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.RunAfter, &i.ExpiresAt, &i.CreatedAt, &i.UpdatedAt, &i.Status, &i.Data)
	return i, err
}

const countReadyAndAll = `-- name: CountReadyAndAll :one
WITH all_count AS (
	SELECT count(*) FROM queued_jobs
), ready_count AS (
	SELECT count(*) FROM queued_jobs WHERE run_after <= now()
) 
SELECT all_count.count, ready_count.count
`

type CountReadyAndAllRow struct {
}

func (q *Queries) CountReadyAndAll(ctx context.Context) (CountReadyAndAllRow, error) {
	var row *sql.Row
	switch {
	case q.countReadyAndAll != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.countReadyAndAll).QueryRowContext(ctx)
	case q.countReadyAndAll != nil:
		row = q.countReadyAndAll.QueryRowContext(ctx)
	default:
		row = q.db.QueryRowContext(ctx, countReadyAndAll)
	}
	var i CountReadyAndAllRow
	err := row.Scan()
	return i, err
}

const createArchivedJob = `-- name: CreateArchivedJob :one
INSERT INTO archived_jobs (
  id,
  name,
  attempts,
  status,
  data,
  expires_at
) 
SELECT id, $2, $4, $3, data, expires_at
FROM queued_jobs 
WHERE id = $1
AND name = $2
RETURNING id, name, attempts, status, created_at, data, expires_at
`

func (q *Queries) CreateArchivedJob(ctx context.Context) (ArchivedJob, error) {
	var row *sql.Row
	switch {
	case q.createArchivedJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.createArchivedJob).QueryRowContext(ctx)
	case q.createArchivedJob != nil:
		row = q.createArchivedJob.QueryRowContext(ctx)
	default:
		row = q.db.QueryRowContext(ctx, createArchivedJob)
	}
	var i ArchivedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.Status, &i.CreatedAt, &i.Data, &i.ExpiresAt)
	return i, err
}

const createJob = `-- name: CreateJob :one
INSERT INTO jobs (
  name,
  delivery_strategy,
  attempts,
  concurrency
) VALUES (
  $1,
  $2,
  $3,
  $4
) RETURNING name, delivery_strategy, attempts, concurrency, created_at
`

func (q *Queries) CreateJob(ctx context.Context, name string, deliveryStrategy DeliveryStrategy, attempts uint8, concurrency uint8) (Job, error) {
	var row *sql.Row
	switch {
	case q.createJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.createJob).QueryRowContext(ctx, name, deliveryStrategy, attempts, concurrency)
	case q.createJob != nil:
		row = q.createJob.QueryRowContext(ctx, name, deliveryStrategy, attempts, concurrency)
	default:
		row = q.db.QueryRowContext(ctx, createJob, name, deliveryStrategy, attempts, concurrency)
	}
	var i Job
	err := row.Scan(&i.Name, &i.DeliveryStrategy, &i.Attempts, &i.Concurrency, &i.CreatedAt)
	return i, err
}

const decrementJobAttempts = `-- name: DecrementJobAttempts :one
UPDATE queued_jobs
SET status = 'queued',
	updated_at = now(),
	attempts = attempts - 1,
	run_after = $3
WHERE id = $1
	AND attempts=$2
RETURNING id, name, attempts, run_after, expires_at, created_at, updated_at, status, data
`

func (q *Queries) DecrementJobAttempts(ctx context.Context, id types.PrefixUUID, attempts uint8, runAfter time.Time) (QueuedJob, error) {
	var row *sql.Row
	switch {
	case q.decrementJobAttempts != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.decrementJobAttempts).QueryRowContext(ctx, id, attempts, runAfter)
	case q.decrementJobAttempts != nil:
		row = q.decrementJobAttempts.QueryRowContext(ctx, id, attempts, runAfter)
	default:
		row = q.db.QueryRowContext(ctx, decrementJobAttempts, id, attempts, runAfter)
	}
	var i QueuedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.RunAfter, &i.ExpiresAt, &i.CreatedAt, &i.UpdatedAt, &i.Status, &i.Data)
	return i, err
}

const deleteQueuedJob = `-- name: DeleteQueuedJob :exec
DELETE FROM queued_jobs WHERE id = $1
`

func (q *Queries) DeleteQueuedJob(ctx context.Context, id types.PrefixUUID) error {
	var err error
	switch {
	case q.deleteQueuedJob != nil && q.tx != nil:
		_, err = q.tx.StmtContext(ctx, q.deleteQueuedJob).ExecContext(ctx, id)
	case q.deleteQueuedJob != nil:
		_, err = q.deleteQueuedJob.ExecContext(ctx, id)
	default:
		_, err = q.db.ExecContext(ctx, deleteQueuedJob, id)
	}
	return err
}

const enqueueJob = `-- name: EnqueueJob :one
INSERT INTO queued_jobs (
	id,
	name,
	attempts,
	run_after,
	expires_at,
	status,
	data
) 
SELECT $1, name, attempts, $3, $4, 'queued', $5
FROM jobs 
WHERE name = $2
AND NOT EXISTS (
	SELECT id FROM archived_jobs WHERE id = $1
)
RETURNING id, name, attempts, run_after, expires_at, created_at, updated_at, status, data
`

func (q *Queries) EnqueueJob(ctx context.Context) (QueuedJob, error) {
	var row *sql.Row
	switch {
	case q.enqueueJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.enqueueJob).QueryRowContext(ctx)
	case q.enqueueJob != nil:
		row = q.enqueueJob.QueryRowContext(ctx)
	default:
		row = q.db.QueryRowContext(ctx, enqueueJob)
	}
	var i QueuedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.RunAfter, &i.ExpiresAt, &i.CreatedAt, &i.UpdatedAt, &i.Status, &i.Data)
	return i, err
}

const getAllJobs = `-- name: GetAllJobs :many
SELECT name, delivery_strategy, attempts, concurrency, created_at
FROM jobs
`

func (q *Queries) GetAllJobs(ctx context.Context) ([]Job, error) {
	var rows *sql.Rows
	var err error
	switch {
	case q.getAllJobs != nil && q.tx != nil:
		rows, err = q.tx.StmtContext(ctx, q.getAllJobs).QueryContext(ctx)
	case q.getAllJobs != nil:
		rows, err = q.getAllJobs.QueryContext(ctx)
	default:
		rows, err = q.db.QueryContext(ctx, getAllJobs)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Job{}
	for rows.Next() {
		var i Job
		if err := rows.Scan(&i.Name, &i.DeliveryStrategy, &i.Attempts, &i.Concurrency, &i.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getArchivedJob = `-- name: GetArchivedJob :one
SELECT id, name, attempts, status, created_at, data, expires_at
FROM archived_jobs
WHERE id = $1
`

func (q *Queries) GetArchivedJob(ctx context.Context, id types.PrefixUUID) (ArchivedJob, error) {
	var row *sql.Row
	switch {
	case q.getArchivedJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.getArchivedJob).QueryRowContext(ctx, id)
	case q.getArchivedJob != nil:
		row = q.getArchivedJob.QueryRowContext(ctx, id)
	default:
		row = q.db.QueryRowContext(ctx, getArchivedJob, id)
	}
	var i ArchivedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.Status, &i.CreatedAt, &i.Data, &i.ExpiresAt)
	return i, err
}

const getCountsByStatus = `-- name: GetCountsByStatus :many
SELECT name, count(*)
FROM queued_jobs
WHERE status=$1
GROUP BY name
`

type GetCountsByStatusRow struct {
	Name  string
	Count int
}

func (q *Queries) GetCountsByStatus(ctx context.Context, status JobStatus) ([]GetCountsByStatusRow, error) {
	var rows *sql.Rows
	var err error
	switch {
	case q.getCountsByStatus != nil && q.tx != nil:
		rows, err = q.tx.StmtContext(ctx, q.getCountsByStatus).QueryContext(ctx, status)
	case q.getCountsByStatus != nil:
		rows, err = q.getCountsByStatus.QueryContext(ctx, status)
	default:
		rows, err = q.db.QueryContext(ctx, getCountsByStatus, status)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []GetCountsByStatusRow{}
	for rows.Next() {
		var i GetCountsByStatusRow
		if err := rows.Scan(&i.Name, &i.Count); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getJob = `-- name: GetJob :one
SELECT name, delivery_strategy, attempts, concurrency, created_at
FROM jobs
WHERE name = $1
`

func (q *Queries) GetJob(ctx context.Context, name string) (Job, error) {
	var row *sql.Row
	switch {
	case q.getJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.getJob).QueryRowContext(ctx, name)
	case q.getJob != nil:
		row = q.getJob.QueryRowContext(ctx, name)
	default:
		row = q.db.QueryRowContext(ctx, getJob, name)
	}
	var i Job
	err := row.Scan(&i.Name, &i.DeliveryStrategy, &i.Attempts, &i.Concurrency, &i.CreatedAt)
	return i, err
}

const getOldInProgressJobs = `-- name: GetOldInProgressJobs :many
SELECT id, name, attempts, run_after, expires_at, created_at, updated_at, status, data
FROM queued_jobs
WHERE status='in-progress' AND updated_at < $1
LIMIT 100
`

func (q *Queries) GetOldInProgressJobs(ctx context.Context, updatedAt time.Time) ([]QueuedJob, error) {
	var rows *sql.Rows
	var err error
	switch {
	case q.getOldInProgressJobs != nil && q.tx != nil:
		rows, err = q.tx.StmtContext(ctx, q.getOldInProgressJobs).QueryContext(ctx, updatedAt)
	case q.getOldInProgressJobs != nil:
		rows, err = q.getOldInProgressJobs.QueryContext(ctx, updatedAt)
	default:
		rows, err = q.db.QueryContext(ctx, getOldInProgressJobs, updatedAt)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []QueuedJob{}
	for rows.Next() {
		var i QueuedJob
		if err := rows.Scan(&i.ID, &i.Name, &i.Attempts, &i.RunAfter, &i.ExpiresAt, &i.CreatedAt, &i.UpdatedAt, &i.Status, &i.Data); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getQueuedJob = `-- name: GetQueuedJob :one
SELECT id, name, attempts, run_after, expires_at, created_at, updated_at, status, data
FROM queued_jobs
WHERE id = $1
`

func (q *Queries) GetQueuedJob(ctx context.Context, id types.PrefixUUID) (QueuedJob, error) {
	var row *sql.Row
	switch {
	case q.getQueuedJob != nil && q.tx != nil:
		row = q.tx.StmtContext(ctx, q.getQueuedJob).QueryRowContext(ctx, id)
	case q.getQueuedJob != nil:
		row = q.getQueuedJob.QueryRowContext(ctx, id)
	default:
		row = q.db.QueryRowContext(ctx, getQueuedJob, id)
	}
	var i QueuedJob
	err := row.Scan(&i.ID, &i.Name, &i.Attempts, &i.RunAfter, &i.ExpiresAt, &i.CreatedAt, &i.UpdatedAt, &i.Status, &i.Data)
	return i, err
}
