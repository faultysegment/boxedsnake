package scheduler

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	schedulerv1 "github.com/faultysegment/boxedsnake/api/gen/scheduler/v1"
	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/internal/db"
)

type SchedulerServer struct {
	db *db.DB
}

func NewSchedulerServer(database *db.DB) *SchedulerServer {
	return &SchedulerServer{db: database}
}

func (s *SchedulerServer) ScheduleTask(
	ctx context.Context,
	req *connect.Request[schedulerv1.ScheduleTaskRequest],
) (*connect.Response[schedulerv1.ScheduleTaskResponse], error) {
	
	taskID := uuid.NewString()
	
	task := &db.ScheduledTask{
		ID:             taskID,
		TaskType:       req.Msg.TaskType.String(),
		ScriptContent:  req.Msg.ScriptContent,
		EnvVars:        req.Msg.EnvVars,
		TimeoutSeconds: int(req.Msg.TimeoutSeconds),
		Status:         "PENDING",
	}

	if req.Msg.TaskType == tasksv1.TaskType_TASK_TYPE_POSTPONED {
		task.ExecuteAt = time.Unix(req.Msg.ExecuteAt, 0)
	} else if req.Msg.TaskType == tasksv1.TaskType_TASK_TYPE_SCHEDULED {
		task.CronExpression = req.Msg.CronExpression
		// Worker will calculate the next run time
		task.ExecuteAt = time.Now() 
	}

	if err := s.db.InsertTask(task); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	res := connect.NewResponse(&schedulerv1.ScheduleTaskResponse{
		TaskId: taskID,
		Status: "Scheduled",
	})
	return res, nil
}

func (s *SchedulerServer) ListScheduledTasks(
	ctx context.Context,
	req *connect.Request[schedulerv1.ListScheduledTasksRequest],
) (*connect.Response[schedulerv1.ListScheduledTasksResponse], error) {
	
	pendingTasks, err := s.db.GetPendingTasks()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var summaries []*tasksv1.TaskSummary
	for _, pt := range pendingTasks {
		summaries = append(summaries, &tasksv1.TaskSummary{
			TaskId:         pt.ID,
			Status:         pt.Status,
			LastExecutedAt: pt.ExecuteAt.Unix(),
		})
	}

	return connect.NewResponse(&schedulerv1.ListScheduledTasksResponse{
		Tasks: summaries,
	}), nil
}

func (s *SchedulerServer) CancelScheduledTask(
	ctx context.Context,
	req *connect.Request[schedulerv1.CancelScheduledTaskRequest],
) (*connect.Response[schedulerv1.CancelScheduledTaskResponse], error) {
	
	err := s.db.CancelTask(req.Msg.TaskId)
	if err != nil {
		return connect.NewResponse(&schedulerv1.CancelScheduledTaskResponse{
			Success: false,
			Message: err.Error(),
		}), nil
	}

	return connect.NewResponse(&schedulerv1.CancelScheduledTaskResponse{
		Success: true,
		Message: "Task cancelled successfully",
	}), nil
}
