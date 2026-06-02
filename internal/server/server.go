package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/internal/kafka"
)

type TaskServer struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func NewTaskServer(producer *kafka.Producer, consumer *kafka.Consumer) *TaskServer {
	return &TaskServer{
		producer: producer,
		consumer: consumer,
	}
}

func (s *TaskServer) ExecuteTask(ctx context.Context, req *connect.Request[tasksv1.SubmitTaskRequest]) (*connect.Response[tasksv1.SubmitTaskResponse], error) {
	taskID := uuid.New().String()

	payload := kafka.TaskPayload{
		TaskID:         taskID,
		ScriptContent:  req.Msg.ScriptContent,
		EnvVars:        req.Msg.EnvVars,
		TimeoutSeconds: req.Msg.TimeoutSeconds,
	}

	// Subscribe to results BEFORE producing
	ch := s.consumer.Subscribe(taskID)
	defer s.consumer.Unsubscribe(taskID)

	// Produce task
	if err := s.producer.ProduceTask(ctx, payload); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to produce task: %w", err))
	}

	// Wait for result
	timeout := time.Duration(req.Msg.TimeoutSeconds+10) * time.Second
	if timeout == 0 {
		timeout = 310 * time.Second // Default worker timeout + buffer
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
