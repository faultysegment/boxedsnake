package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/internal/db"
	"github.com/faultysegment/boxedsnake/internal/kafka"
)

type Worker struct {
	db       *db.DB
	producer *kafka.Producer
}

func NewWorker(database *db.DB, producer *kafka.Producer) *Worker {
	return &Worker{
		db:       database,
		producer: producer,
	}
}

func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	cronParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processDueTasks(ctx, cronParser)
		}
	}
}

func (w *Worker) processDueTasks(ctx context.Context, parser cron.Parser) {
	tasks, err := w.db.GetDueTasks()
	if err != nil {
		log.Printf("Error getting due tasks: %v", err)
		return
	}

	for _, t := range tasks {
		payload := kafka.TaskPayload{
			TaskID:         t.ID,
			ScriptContent:  t.ScriptContent,
			EnvVars:        t.EnvVars,
			TimeoutSeconds: int32(t.TimeoutSeconds),
		}

		err := w.producer.ProduceTask(ctx, payload)
		if err != nil {
			log.Printf("Failed to produce task %s to Kafka: %v", t.ID, err)
			continue
		}
		
		log.Printf("Successfully produced scheduled task %s", t.ID)

		if t.TaskType == tasksv1.TaskType_TASK_TYPE_POSTPONED.String() {
			if err := w.db.MarkTaskCompleted(t.ID); err != nil {
				log.Printf("Failed to mark task %s as completed: %v", t.ID, err)
			}
		} else if t.TaskType == tasksv1.TaskType_TASK_TYPE_SCHEDULED.String() {
			schedule, err := parser.Parse(t.CronExpression)
			if err != nil {
				log.Printf("Failed to parse cron expression '%s' for task %s: %v", t.CronExpression, t.ID, err)
				w.db.MarkTaskCompleted(t.ID)
				continue
			}
			
			nextRun := schedule.Next(time.Now())
			if err := w.db.UpdateNextRun(t.ID, nextRun); err != nil {
				log.Printf("Failed to update next run for task %s: %v", t.ID, err)
			}
		}
	}
}
