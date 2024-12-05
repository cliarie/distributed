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

// Process lines with a given executable
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

// Handle task execution from leader
func (s *workerServer) ExecuteTask(ctx context.Context, taskData *api.TaskData) (*api.ExecutionResponse, error) {
	// Read partition from HyDFS
	resp, _ := s.hydfsClient.SendRequest(client.Request{
		Operation: client.GET,
		HyDFSFile: taskData.SrcFile,
	})
	if resp.Status != "success" {
		return nil, fmt.Errorf("Failed to fetch partition: %s", resp.Message)
	}

	lines := strings.Split(resp.Message, "\n")
	transformedLines := processWithExecutable(taskData.Executable, lines)

	// Append results to HyDFS
	resp, _ = s.hydfsClient.SendRequest(client.Request{
		Operation: client.APPEND,
		HyDFSFile: taskData.DestFile,
		Content:   strings.Join(transformedLines, "\n"),
	})
	if resp.Status != "success" {
		return nil, fmt.Errorf("Failed to append results")
	}

	log.Printf("Task %s processed and results appended to %s", taskData.TaskId, taskData.DestFile)
	return &api.ExecutionResponse{
		Success: true,
		Message: "task executed successfully.",
	}, nil
}

// Acknowledge task
func (s *workerServer) AckTask(ctx context.Context, ackInfo *api.AckInfo) (*api.AckResponse, error) {
	log.Printf("ack received: taskID=%s, ack=%v", ackInfo.TaskId, ackInfo.Ack)
	return &api.AckResponse{
		Success: true,
		Message: "ack received.",
	}, nil
}

// Parse worker index
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

	workerIndex := os.Getenv("WORKER_INDEX")
	if workerIndex == "" {
		workerIndex = "1"
	}

	workerID := fmt.Sprintf("%s-worker-%s", hostname, workerIndex)
	port := 50050 + parseWorkerIndex(workerIndex)

	workerAddress := fmt.Sprintf("%s:%d", hostname, port)
	if envAddress := os.Getenv("WORKER_ADDRESS"); envAddress != "" {
		workerAddress = envAddress
	}

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Fatalf("Failed to listen on port %d: %v", port, err)
		}
		s := grpc.NewServer()
		api.RegisterWorkerServiceServer(s, &workerServer{
			workerID:    workerID,
			hydfsClient: hydfsClient,
		})
		log.Printf("WorkerService Server %s running on %s", workerID, workerAddress)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve worker service: %v", err)
		}
	}()

	// Connect to leader
	leaderAddress := os.Getenv("LEADER_ADDRESS")
	if leaderAddress == "" {
		leaderAddress = "fa24-cs425-0701.cs.illinois.edu:50051"
	}

	conn, err := grpc.Dial(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := api.NewLeaderServiceClient(conn)
	_, err = client.RegisterWorker(context.Background(), &api.WorkerInfo{
		WorkerId: workerID,
		Address:  workerAddress,
	})
	if err != nil {
		log.Fatalf("Worker registration failed: %v", err)
	}
	log.Printf("Worker %s registered with leader at %s", workerID, leaderAddress)

	// Send periodic heartbeats
	// go func() {
	// 	for {
	// 		_, err := client.ReportTaskStatus(context.Background(), &api.TaskStatus{
	// 			TaskId:    "heartbeat",
	// 			Status:    "alive",
	// 			WorkerId:  workerID,
	// 			Timestamp: time.Now().Unix(),
	// 		})
	// 		if err != nil {
	// 			log.Printf("Heartbeat error: %v", err)
	// 		}
	// 		time.Sleep(5 * time.Second)
	// 	}
	// }()

	// Block forever
	select {}
}
