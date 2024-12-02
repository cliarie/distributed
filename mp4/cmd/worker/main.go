package main

import (
	"context"
	"log"
	"mp4/pkg/api"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workerServer struct {
	api.UnimplementedWorkerServiceServer
}

func (s *workerServer) ExecuteTask(ctx context.Context, taskData *api.TaskData) (*api.ExecutionResponse, error) {
	log.Printf("received task: taskID=%s, num tuples=%d", taskData.TaskId, len(taskData.Tuples))

	log.Printf("completed task: taskID=%s", taskData.TaskId)
	return &api.ExecutionResponse{
		Success: true,
		Message: "task executed successfully.",
	}, nil
}

func (s *workerServer) AckTask(ctx context.Context, ackInfo *api.AckInfo) (*api.AckResponse, error) {
	log.Printf("ack received: taskID=%s, ack=%v", ackInfo.TaskId, ackInfo.Ack)
	return &api.AckResponse{
		Success: true,
		Message: "ack received.",
	}, nil
}

func main() {
	go func() {
		lis, err := net.Listen("tcp", ":50052")
		if err != nil {
			log.Fatalf("Failed to listen on port 50052: %v", err)
		}
		s := grpc.NewServer()
		api.RegisterWorkerServiceServer(s, &workerServer{})
		log.Println("worker service server running on port 50052")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve worker service: %v", err)
		}
	}()

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := api.NewLeaderServiceClient(conn)

	registerResp, err := client.RegisterWorker(context.Background(), &api.WorkerInfo{
		WorkerId: "worker1",
		Address:  "localhost:50052",
	})
	if err != nil {
		log.Fatalf("Register worker failed: %v", err)
	}
	log.Printf("Register worker success: sucess=%v, msg=%v", registerResp.Success, registerResp.Message)

	taskStatusResp, err := client.ReportTaskStatus(context.Background(), &api.TaskStatus{
		TaskId: "task1",
		Status: "completed",
	})
	if err != nil {
		log.Fatalf("Report task status failed: %v", err)
	}
	log.Printf("Report task status success: sucess=%v, msg=%v", taskStatusResp.Success, taskStatusResp.Message)
}
