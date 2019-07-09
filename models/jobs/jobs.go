// Logic for interacting with the "jobs" table.
package jobs

import (
	"context"
	"database/sql"
	"time"

	dberror "github.com/kevinburke/go-dberror"
	models "github.com/kevinburke/rickover/newmodels"
	"github.com/lib/pq"
)

func init() {
	dberror.RegisterConstraint(concurrencyConstraint)
	dberror.RegisterConstraint(attemptsConstraint)
}

func Create(job models.Job) (*models.Job, error) {
	dbJob, err := models.DB.CreateJob(context.Background(), job.Name, job.DeliveryStrategy, job.Attempts, job.Concurrency)
	if err != nil {
		err = dberror.GetError(err)
	}
	return &dbJob, err
}

// Get a job by its name.
func Get(name string) (*models.Job, error) {
	job, err := models.DB.GetJob(context.Background(), name)
	if err != nil {
		err = dberror.GetError(err)
	}
	return &job, err
}

func GetAll() ([]*models.Job, error) {
	jobs := []*models.Job{}
	result, err := models.DB.GetAllJobs(context.Background())
	for _, j := range result {
		jj := j
		jobs = append(jobs, &jj)
	}
	return jobs, err
}

// GetRetry attempts to get the job `attempts` times before giving up.
func GetRetry(name string, attempts uint8) (job *models.Job, err error) {
	for i := uint8(0); i < attempts; i++ {
		job, err = Get(name)
		if err == nil || err == sql.ErrNoRows {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return
}

var concurrencyConstraint = &dberror.Constraint{
	Name: "jobs_concurrency_check",
	GetError: func(e *pq.Error) *dberror.Error {
		return &dberror.Error{
			Message:    "Concurrency must be a positive number",
			Constraint: e.Constraint,
			Table:      e.Table,
			Severity:   e.Severity,
			Detail:     e.Detail,
		}
	},
}

var attemptsConstraint = &dberror.Constraint{
	Name: "jobs_attempts_check",
	GetError: func(e *pq.Error) *dberror.Error {
		return &dberror.Error{
			Message:    "Please set a greater-than-zero number of attempts",
			Constraint: e.Constraint,
			Table:      e.Table,
			Severity:   e.Severity,
			Detail:     e.Detail,
		}
	},
}
