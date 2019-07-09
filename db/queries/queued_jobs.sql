-- name: EnqueueJob :one
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
RETURNING *;

-- name: GetQueuedJob :one
SELECT *
FROM queued_jobs
WHERE id = $1;

-- name: DeleteQueuedJob :execrows
DELETE FROM queued_jobs WHERE id = $1;

-- name: AcquireJobs :many
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
RETURNING *;

-- name: DecrementJobAttempts :one
UPDATE queued_jobs
SET status = 'queued',
	updated_at = now(),
	attempts = attempts - 1,
	run_after = $3
WHERE id = $1
	AND attempts=$2
RETURNING *;

-- name: CountReadyAndAll :one
WITH all_count AS (
	SELECT count(*) FROM queued_jobs
), ready_count AS (
	SELECT count(*) FROM queued_jobs WHERE run_after <= now()
) 
SELECT all_count.count, ready_count.count
FROM all_count, ready_count;

-- name: GetCountsByStatus :many
SELECT name, count(*)
FROM queued_jobs
WHERE status=$1
GROUP BY name;

-- name: GetOldInProgressJobs :many
SELECT *
FROM queued_jobs
WHERE status='in-progress' AND updated_at < $1
LIMIT 100;
