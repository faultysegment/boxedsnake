package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type ScheduledTask struct {
	ID             string
	TaskType       string
	ScriptContent  string
	EnvVars        map[string]string
	TimeoutSeconds int
	ExecuteAt      time.Time
	CronExpression string
	Status         string
}

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
	CREATE TABLE IF NOT EXISTS scheduled_tasks (
		id UUID PRIMARY KEY,
		task_type VARCHAR(50) NOT NULL,
		script_content TEXT NOT NULL,
		env_vars JSONB,
		timeout_seconds INT,
		execute_at TIMESTAMP WITH TIME ZONE,
		cron_expression VARCHAR(100),
		status VARCHAR(50) NOT NULL
	);
	`
	_, err := db.conn.Exec(query)
	return err
}

func (db *DB) InsertTask(task *ScheduledTask) error {
	envVarsJSON, err := json.Marshal(task.EnvVars)
	if err != nil {
		return err
	}

	query := `
	INSERT INTO scheduled_tasks 
	(id, task_type, script_content, env_vars, timeout_seconds, execute_at, cron_expression, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = db.conn.Exec(query,
		task.ID, task.TaskType, task.ScriptContent, string(envVarsJSON),
		task.TimeoutSeconds, task.ExecuteAt, task.CronExpression, task.Status)
	return err
}

func (db *DB) GetDueTasks() ([]ScheduledTask, error) {
	query := `
	SELECT id, task_type, script_content, env_vars, timeout_seconds, execute_at, cron_expression, status
	FROM scheduled_tasks
	WHERE execute_at <= NOW() AND status = 'PENDING'
	FOR UPDATE SKIP LOCKED
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []ScheduledTask
	for rows.Next() {
		var t ScheduledTask
		var envVarsJSON []byte
		var execAt sql.NullTime
		var cronExpr sql.NullString
		
		err := rows.Scan(
			&t.ID, &t.TaskType, &t.ScriptContent, &envVarsJSON, 
			&t.TimeoutSeconds, &execAt, &cronExpr, &t.Status)
		if err != nil {
			log.Printf("Error scanning due task: %v", err)
			continue
		}

		if len(envVarsJSON) > 0 {
			json.Unmarshal(envVarsJSON, &t.EnvVars)
		} else {
			t.EnvVars = make(map[string]string)
		}
		
		if execAt.Valid {
			t.ExecuteAt = execAt.Time
		}
		if cronExpr.Valid {
			t.CronExpression = cronExpr.String
		}

		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (db *DB) MarkTaskCompleted(taskID string) error {
	_, err := db.conn.Exec(`UPDATE scheduled_tasks SET status = 'COMPLETED' WHERE id = $1`, taskID)
	return err
}

func (db *DB) UpdateNextRun(taskID string, nextRun time.Time) error {
	_, err := db.conn.Exec(`UPDATE scheduled_tasks SET execute_at = $1 WHERE id = $2`, nextRun, taskID)
	return err
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) GetPendingTasks() ([]ScheduledTask, error) {
	query := `
	SELECT id, status, execute_at
	FROM scheduled_tasks
	WHERE status = 'PENDING'
	ORDER BY execute_at ASC
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []ScheduledTask
	for rows.Next() {
		var t ScheduledTask
		var execAt sql.NullTime

		err := rows.Scan(&t.ID, &t.Status, &execAt)
		if err != nil {
			log.Printf("Error scanning pending task: %v", err)
			continue
		}

		if execAt.Valid {
			t.ExecuteAt = execAt.Time
		}

		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (db *DB) CancelTask(taskID string) error {
	res, err := db.conn.Exec(`UPDATE scheduled_tasks SET status = 'CANCELLED' WHERE id = $1 AND status = 'PENDING'`, taskID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("task not found or not in PENDING state")
	}
	return nil
}
