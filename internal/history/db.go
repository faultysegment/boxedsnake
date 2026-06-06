package history

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

func NewDB(connStr string) (*DB, error) {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	db := &DB{conn: conn}
	if err := db.initSchema(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) initSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS task_results (
		id UUID PRIMARY KEY,
		task_id UUID NOT NULL,
		status VARCHAR(50) NOT NULL,
		stdout TEXT,
		stderr TEXT,
		result_data JSONB,
		executed_at TIMESTAMP WITH TIME ZONE NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_task_results_task_id ON task_results(task_id);
	`
	_, err := db.conn.Exec(query)
	return err
}

func (db *DB) Close() error {
	return db.conn.Close()
}

type TaskResult struct {
	ID         string
	TaskID     string
	Status     string
	Stdout     string
	Stderr     string
	ResultData string
	ExecutedAt time.Time
}

type TaskSummary struct {
	TaskID         string
	Status         string
	LastExecutedAt time.Time
}

func (db *DB) InsertTaskResult(res *TaskResult) error {
	query := `
	INSERT INTO task_results (id, task_id, status, stdout, stderr, result_data, executed_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	var resData interface{}
	if res.ResultData == "" {
		resData = nil
	} else {
		resData = res.ResultData
	}

	_, err := db.conn.Exec(query, res.ID, res.TaskID, res.Status, res.Stdout, res.Stderr, resData, res.ExecutedAt)
	return err
}

func (db *DB) GetTaskResults(taskID string) ([]TaskResult, error) {
	query := `
	SELECT id, task_id, status, stdout, stderr, result_data, executed_at
	FROM task_results
	WHERE task_id = $1
	ORDER BY executed_at DESC
	`
	rows, err := db.conn.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TaskResult
	for rows.Next() {
		var r TaskResult
		var stdout, stderr sql.NullString
		var resultData []byte
		
		err := rows.Scan(&r.ID, &r.TaskID, &r.Status, &stdout, &stderr, &resultData, &r.ExecutedAt)
		if err != nil {
			log.Printf("Error scanning task result: %v", err)
			continue
		}

		if stdout.Valid {
			r.Stdout = stdout.String
		}
		if stderr.Valid {
			r.Stderr = stderr.String
		}
		if len(resultData) > 0 {
			r.ResultData = string(resultData)
		}

		results = append(results, r)
	}

	return results, nil
}

func (db *DB) ListTasks() ([]TaskSummary, error) {
	query := `
	SELECT DISTINCT ON (task_id) task_id, status, executed_at
	FROM task_results
	ORDER BY task_id, executed_at DESC
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []TaskSummary
	for rows.Next() {
		var s TaskSummary
		err := rows.Scan(&s.TaskID, &s.Status, &s.LastExecutedAt)
		if err != nil {
			log.Printf("Error scanning task summary: %v", err)
			continue
		}
		summaries = append(summaries, s)
	}

	return summaries, nil
}
