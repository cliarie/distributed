// initializes the leader server, sets up grpc server, starts listening for worker registrations and task assignments
// monitors heartbeats from workers
package main

import (
	"context"
	"fmt"
	"log"
	"mp4/pkg/api"
	"net"
	"os"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// implements api.LeaderServiceServer
type leaderServer struct {
	api.UnimplementedLeaderServiceServer
	workers  map[string]string // workerid -> address
	tasks    map[string]string // taskid -> workerid
	taskLock sync.Mutex
	mu       sync.Mutex
	nodes    []string // list of nodes, see nodes.config
}

// LeaderService.RegisterWorker
func (s *leaderServer) RegisterWorker(ctx context.Context, workerInfo *api.WorkerInfo) (*api.RegisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[workerInfo.WorkerId] = workerInfo.Address

	log.Printf("Worker Registered: ID=%s, Address=%s", workerInfo.WorkerId, workerInfo.Address)
	return &api.RegisterResponse{
		Success: true,
		Message: "Worker registered successfully.",
	}, nil
}

func (s *leaderServer) AssignTask(ctx context.Context, taskAssignment *api.TaskAssignment) (*api.TaskResponse, error) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	log.Printf("Assigning Task: TaskID=%s, Operator=%s, Executable=%s, NumTasks=%d, SrcFile=%s, DestFile=%s",
		taskAssignment.TaskId, taskAssignment.Operator, taskAssignment.Executable, taskAssignment.NumTasks, taskAssignment.SrcFile, taskAssignment.DestFile)

	// TODO: partition input into chunks
	partitions := partitionInput(taskAssignment.NumTasks, taskAssignment.SrcFile)
	workerCount := len(s.workers)
	if workerCount == 0 {
		return &api.TaskResponse{Success: false, Message: "No workers"}, nil
	}

	for i, partition := range partitions {
		// TODO: select a worker (rr)
		workerID := selectWorker(s.workers, i)
		workerAddress := s.workers[workerID]

		log.Printf("Assignin partition %d to worker %s (%s)", i, workerID, workerAddress)
		go func(workerAddr string, taskID, partition, destFile string) {
			// TODO: connect to worker
			client, conn, err := connectToWorker(workerAddr)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v", workerAddr)
			}
			defer conn.Close()
			// TODO: client execute task
			_, err = client.ExecuteTask(context.Background(), &api.TaskData{
				TaskId:   taskID,
				SrcFile:  partition,
				DestFile: destFile,
			})
			if err != nil {
				log.Printf("Worker %s failed to execute taskk %s; error: %v", workerAddr, taskID, err)
			}
		}(workerAddress, fmt.Sprintf("%s-part-%d", taskAssignment.TaskId, i), partition, taskAssignment.DestFile)
	}

	return &api.TaskResponse{
		Success: true,
		Message: "Task assigned successfully.",
	}, nil
}

// Reassign tasks to a different worker
func (s *leaderServer) reassignTask(taskID, srcFile, destFile string) {
	log.Printf("Reassigning task %s", taskID)

	s.mu.Lock()
	defer s.mu.Unlock()

	for workerID, address := range s.workers {
		if _, exists := s.tasks[taskID]; exists && s.tasks[taskID] == workerID {
			continue
		}
		log.Printf("Reassigning task %s to worker %s", taskID, workerID)
		client, conn, err := connectToWorker(address)
		if err != nil {
			log.Printf("Failed to connect to worker %s: %v", workerID, err)
			continue
		}
		defer conn.Close()

		_, err = client.ExecuteTask(context.Background(), &api.TaskData{
			TaskId:   taskID,
			SrcFile:  srcFile,
			DestFile: destFile,
		})
		if err == nil {
			s.tasks[taskID] = workerID
			return
		}
		log.Printf("Worker %s failed: %v", workerID, err)
	}
}

func partitionInput(numTasks int32, srcFile string) []string {
	partitions := make([]string, numTasks)
	for i := int32(0); i < numTasks; i++ {
		partitions[i] = fmt.Sprintf("Partition-%s-%d", srcFile, i)
	}
	return partitions
}

func selectWorker(workers map[string]string, index int) string {
	i := 0
	for workerID := range workers {
		if i == index%len(workers) {
			return workerID
		}
		i++
	}
	return ""
}

func connectToWorker(address string) (api.WorkerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to connect to worker: %v", err)
	}
	client := api.NewWorkerServiceClient(conn)
	return client, conn, nil
}
func (s *leaderServer) ReportTaskStatus(ctx context.Context, taskStatus *api.TaskStatus) (*api.StatusResponse, error) {
	log.Printf("Task Status Reported: TaskID=%s, Status=%s", taskStatus.TaskId, taskStatus.Status)
	// TODO: Implement task status handling
	return &api.StatusResponse{
		Success: true,
		Message: "Task status received.",
	}, nil
}

func loadNodes(configFile string) []string {
	file, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read nodes config: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(file)), "\n")
}

func main() {
	nodes := loadNodes("nodes.config")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	leader := &leaderServer{
		workers: make(map[string]string),
		tasks:   make(map[string]string),
		nodes:   nodes,
	}
	api.RegisterLeaderServiceServer(s, leader)
	log.Println("LeaderService Server running on port 50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
