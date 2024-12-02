package main

import (
	"context"
	"log"
	"mp4/pkg/api"
	"net"

	"google.golang.org/grpc"
)

// implements api.LeaderServiceServer
type server struct {
	api.UnimplementedLeaderServiceServer
}

// LeaderService.RegisterWorker
func (s *server) RegisterWorker(ctx context.Context, workerInfo *api.WorkerInfo) (*api.RegisterResponse, error) {
	log.Printf("Worker Registered: ID=%s, Address=%s", workerInfo.WorkerId, workerInfo.Address)
	return &api.RegisterResponse{
		Success: true,
		Message: "Worker registered successfully.",
	}, nil
}

func (s *server) AssignTask(ctx context.Context, taskAssignment *api.TaskAssignment) (*api.TaskResponse, error) {
	log.Printf("Assigning Task: TaskID=%s, Operator=%s, Executable=%s, NumTasks=%d",
		taskAssignment.TaskId, taskAssignment.Operator, taskAssignment.Executable, taskAssignment.NumTasks)
	// TODO: Implement task assignment logic
	return &api.TaskResponse{
		Success: true,
		Message: "Task assigned successfully.",
	}, nil
}

func (s *server) ReportTaskStatus(ctx context.Context, taskStatus *api.TaskStatus) (*api.StatusResponse, error) {
	log.Printf("Task Status Reported: TaskID=%s, Status=%s", taskStatus.TaskId, taskStatus.Status)
	// TODO: Implement task status handling
	return &api.StatusResponse{
		Success: true,
		Message: "Task status received.",
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	api.RegisterLeaderServiceServer(s, &server{})
	log.Println("LeaderService Server running on port 50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
