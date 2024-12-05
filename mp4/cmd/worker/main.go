// registers with the leader, send periodic heartbeats to leader
// waits for task assignments from leader
package main

import (
	"context"
	"fmt"
	"log"
	"mp4/pkg/api"
	"mp4/pkg/hydfs/client"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workerServer struct {
	api.UnimplementedWorkerServiceServer
	workerID    string
	hydfsClient *client.Client
}

func processWithExecutable(executable string, lines []string) []string {
	input := strings.Join(lines, "\n")
	cmd := exec.Command(executable)
	cmd.Stdin = strings.NewReader(input)

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Error running %s: %v", executable, err)
		return nil
	}

	return strings.Split(strings.TrimSpace(string(output)), "\n")
}

func (s *workerServer) ExecuteTask(ctx context.Context, taskData *api.TaskData) (*api.ExecutionResponse, error) {
	resp, _ := s.hydfsClient.SendRequest(client.Request{
		Operation: client.GET,
		HyDFSFile: taskData.SrcFile,
	})
	if resp.Status == "error" {
		return nil, fmt.Errorf("Failed to fetch partition: %s", resp.Message)
	}

	lines := strings.Split(resp.Message, "\n")
	transformedLines := processWithExecutable(taskData.Executable, lines)

	resp, _ = s.hydfsClient.SendRequest(client.Request{
		Operation: client.APPEND,
		HyDFSFile: taskData.DestFile,
		Content:   strings.Join(transformedLines, "\n"),
	})
	if resp.Status == "error" {
		return nil, fmt.Errorf("Failed to append results")
	}

	log.Printf("Task %s processed and results appended to %s", taskData.TaskId, taskData.DestFile)

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

func parseWorkerIndex(workerIndex string) int {
	index, err := strconv.Atoi(workerIndex)
	if err != nil || index < 0 {
		log.Fatalf("Invalid WORKER_INDEX: must be a positive integer, got %s", workerIndex)
	}
	return index
}

func main() {
	hydfsClient := client.NewClient("fa24-cs425-0701.cs.illinois.edu:23120")
	defer hydfsClient.Close()
	// Get hostname to use as part of the WorkerID
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	// Read worker index from environment variable or default to 1
	workerIndex := os.Getenv("WORKER_INDEX")
	if workerIndex == "" {
		workerIndex = "1"
	}

	workerID := fmt.Sprintf("%s-worker-%s", hostname, workerIndex)
	port := 50050 + parseWorkerIndex(workerIndex)

	go func() {
		lis, err := net.Listen("tcp", ":50052")
		if err != nil {
			log.Fatalf("Failed to listen on port 50052: %v", err)
		}
		s := grpc.NewServer()
		api.RegisterWorkerServiceServer(s, &workerServer{
			workerID:    workerID,
			hydfsClient: hydfsClient,
		})
		log.Printf("WorkerService Server %s running on port %d", workerID, port)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve worker service: %v", err)
		}
	}()

	leaderAddress := os.Getenv("LEADER_ADDRESS")
	if leaderAddress == "" {
		leaderAddress = "fa24-cs425-0701.cs.illinois.edu:50051" // Default leader address
	}

	conn, err := grpc.Dial(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := api.NewLeaderServiceClient(conn)
	_, err = client.RegisterWorker(context.Background(), &api.WorkerInfo{
		WorkerId: workerID,
		Address:  fmt.Sprintf("%s:%d", hostname, port),
	})
	if err != nil {
		log.Fatalf("Worker registration failed: %v", err)
	}
	log.Printf("Worker %s registered with leader at %s", workerID, leaderAddress)

	taskStatusResp, err := client.ReportTaskStatus(context.Background(), &api.TaskStatus{
		TaskId: "task1",
		Status: "completed",
	})
	if err != nil {
		log.Fatalf("Report task status failed: %v", err)
	}
	log.Printf("Report task status success: sucess=%v, msg=%v", taskStatusResp.Success, taskStatusResp.Message)
}
