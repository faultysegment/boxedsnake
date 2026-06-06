package history

import (
	"context"

	"connectrpc.com/connect"

	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
)

type HistoryServer struct {
	db *DB
}

func NewHistoryServer(database *DB) *HistoryServer {
	return &HistoryServer{db: database}
}

func (s *HistoryServer) GetTaskResults(
	ctx context.Context,
	req *connect.Request[tasksv1.GetTaskResultsRequest],
) (*connect.Response[tasksv1.GetTaskResultsResponse], error) {
	
	results, err := s.db.GetTaskResults(req.Msg.TaskId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var protoResults []*tasksv1.TaskResult
	for _, r := range results {
		protoResults = append(protoResults, &tasksv1.TaskResult{
			TaskId:     r.TaskID,
			Status:     r.Status,
			Stdout:     r.Stdout,
			Stderr:     r.Stderr,
			ResultData: r.ResultData,
			ExecutedAt: r.ExecutedAt.Unix(),
		})
	}

	res := connect.NewResponse(&tasksv1.GetTaskResultsResponse{
		Results: protoResults,
	})
	return res, nil
}

func (s *HistoryServer) ListTasks(
	ctx context.Context,
	req *connect.Request[tasksv1.ListTasksRequest],
) (*connect.Response[tasksv1.ListTasksResponse], error) {
	
	summaries, err := s.db.ListTasks()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var protoSummaries []*tasksv1.TaskSummary
	for _, s := range summaries {
		protoSummaries = append(protoSummaries, &tasksv1.TaskSummary{
			TaskId:         s.TaskID,
			Status:         s.Status,
			LastExecutedAt: s.LastExecutedAt.Unix(),
		})
	}

	res := connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks: protoSummaries,
	})
	return res, nil
}
