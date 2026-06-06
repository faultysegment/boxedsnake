package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	schedulerv1 "github.com/faultysegment/boxedsnake/api/gen/scheduler/v1"
	schedulerv1connect "github.com/faultysegment/boxedsnake/api/gen/scheduler/v1/schedulerv1connect"
	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/api/gen/history/v1/historyv1connect"
	"github.com/faultysegment/boxedsnake/internal/kafka"
)

type TaskServer struct {
	producer        *kafka.Producer
	consumer        *kafka.Consumer
	schedulerClient schedulerv1connect.SchedulerServiceClient
	historyClient   historyv1connect.HistoryServiceClient
}

func NewTaskServer(producer *kafka.Producer, consumer *kafka.Consumer, schedulerURL string, historyURL string) *TaskServer {
	return &TaskServer{
		producer: producer,
		consumer: consumer,
		schedulerClient: schedulerv1connect.NewSchedulerServiceClient(
			http.DefaultClient,
			schedulerURL,
		),
		historyClient: historyv1connect.NewHistoryServiceClient(
			http.DefaultClient,
			historyURL,
		),
	}
}

func (s *TaskServer) ExecuteTask(ctx context.Context, req *connect.Request[tasksv1.SubmitTaskRequest]) (*connect.Response[tasksv1.SubmitTaskResponse], error) {
	// 1. One Shot tasks directly to Kafka
	if req.Msg.TaskType == tasksv1.TaskType_TASK_TYPE_ONE_SHOT || req.Msg.TaskType == tasksv1.TaskType_TASK_TYPE_UNSPECIFIED {
		taskID := uuid.New().String()

		payload := kafka.TaskPayload{
			TaskID:         taskID,
			ScriptContent:  req.Msg.ScriptContent,
			EnvVars:        req.Msg.EnvVars,
			TimeoutSeconds: req.Msg.TimeoutSeconds,
		}

		ch := s.consumer.Subscribe(taskID)
		defer s.consumer.Unsubscribe(taskID)

		if err := s.producer.ProduceTask(ctx, payload); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to produce task: %w", err))
		}

		timeout := time.Duration(req.Msg.TimeoutSeconds+10) * time.Second
		if timeout == 0 {
			timeout = 310 * time.Second 
		}

		select {
		case res := <-ch:
			resultDataStr := ""
			if res.ResultData != nil {
				b, _ := json.Marshal(res.ResultData)
				resultDataStr = string(b)
			}

			return connect.NewResponse(&tasksv1.SubmitTaskResponse{
				TaskId:     res.TaskID,
				Status:     res.Status,
				Stdout:     res.Stdout,
				Stderr:     res.Stderr,
				ResultData: resultDataStr,
			}), nil
		case <-time.After(timeout):
			return nil, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("timed out waiting for worker result"))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 2. Scheduled or Postponed tasks to SchedulerService
	schedReq := connect.NewRequest(&schedulerv1.ScheduleTaskRequest{
		ScriptContent:  req.Msg.ScriptContent,
		EnvVars:        req.Msg.EnvVars,
		TimeoutSeconds: req.Msg.TimeoutSeconds,
		TaskType:       req.Msg.TaskType,
		ExecuteAt:      req.Msg.ExecuteAt,
		CronExpression: req.Msg.CronExpression,
	})

	schedRes, err := s.schedulerClient.ScheduleTask(ctx, schedReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to schedule task: %w", err))
	}

	return connect.NewResponse(&tasksv1.SubmitTaskResponse{
		TaskId: schedRes.Msg.TaskId,
		Status: schedRes.Msg.Status,
	}), nil
}

func (s *TaskServer) GetTaskResults(
	ctx context.Context,
	req *connect.Request[tasksv1.GetTaskResultsRequest],
) (*connect.Response[tasksv1.GetTaskResultsResponse], error) {

	histReq := connect.NewRequest(&tasksv1.GetTaskResultsRequest{
		TaskId: req.Msg.TaskId,
	})

	histRes, err := s.historyClient.GetTaskResults(ctx, histReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to fetch task results from history service: %w", err))
	}

	return connect.NewResponse(&tasksv1.GetTaskResultsResponse{
		Results: histRes.Msg.Results,
	}), nil
}

func (s *TaskServer) ListTasks(
	ctx context.Context,
	req *connect.Request[tasksv1.ListTasksRequest],
) (*connect.Response[tasksv1.ListTasksResponse], error) {

	histReq := connect.NewRequest(&tasksv1.ListTasksRequest{})
	histRes, err := s.historyClient.ListTasks(ctx, histReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to fetch task list from history service: %w", err))
	}

	schedReq := connect.NewRequest(&schedulerv1.ListScheduledTasksRequest{})
	schedRes, err := s.schedulerClient.ListScheduledTasks(ctx, schedReq)
	if err != nil {
		// Log the error but don't fail the entire request, so history tasks still show up
		fmt.Printf("failed to fetch task list from scheduler service: %v\n", err)
	}

	mergedTasks := make(map[string]*tasksv1.TaskSummary)

	// Add history tasks first
	if histRes != nil && histRes.Msg != nil {
		for _, task := range histRes.Msg.Tasks {
			mergedTasks[task.TaskId] = task
		}
	}

	// Add or override with scheduled tasks (PENDING)
	if schedRes != nil && schedRes.Msg != nil {
		for _, task := range schedRes.Msg.Tasks {
			mergedTasks[task.TaskId] = task
		}
	}

	var finalTasks []*tasksv1.TaskSummary
	for _, task := range mergedTasks {
		finalTasks = append(finalTasks, task)
	}

	return connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks: finalTasks,
	}), nil
}

func (s *TaskServer) CancelTask(
	ctx context.Context,
	req *connect.Request[tasksv1.CancelTaskRequest],
) (*connect.Response[tasksv1.CancelTaskResponse], error) {

	schedReq := connect.NewRequest(&schedulerv1.CancelScheduledTaskRequest{
		TaskId: req.Msg.TaskId,
	})

	schedRes, err := s.schedulerClient.CancelScheduledTask(ctx, schedReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to cancel task in scheduler: %w", err))
	}

	return connect.NewResponse(&tasksv1.CancelTaskResponse{
		Success: schedRes.Msg.Success,
		Message: schedRes.Msg.Message,
	}), nil
}
