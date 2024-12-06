// initializes the leader server, sets up grpc server, starts listening for worker registrations and task assignments
// monitors heartbeats from workers
package main

import (
	"context"
	"fmt"
	"log"
	"mp4/pkg/api"
	"mp4/pkg/hydfs/client"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// implements api.LeaderServiceServer
type leaderServer struct {
	api.UnimplementedLeaderServiceServer
	workers     map[string]string // workerid -> address
	tasks       map[string]string // taskid -> workerid
	taskLock    sync.Mutex
	mu          sync.Mutex
	hydfsClient *client.Client
	// nodes    []string // list of nodes, see nodes.config
}

func (s *leaderServer) AckTask(ctx context.Context, ackInfo *api.AckInfo) (*api.AckResponse, error) {
	log.Printf("Ack received for task %s", ackInfo.TaskId)
	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	// Mark task as completed
	delete(s.tasks, ackInfo.TaskId)
	return &api.AckResponse{Success: true, Message: "Acknowledgment received."}, nil
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
	log.Printf("Assigning Task: TaskID=%s, Executable=%s, NumTasks=%d, SrcFile=%s, DestFile=%s",
		taskAssignment.TaskId, taskAssignment.Executable, taskAssignment.NumTasks, taskAssignment.SrcFile, taskAssignment.DestFile)

	partitions, err := partitionInput(taskAssignment.NumTasks, taskAssignment.SrcFile, s.hydfsClient)
	if err != nil {
		return nil, fmt.Errorf("Failed to partition input file: %v", err)
	}

	workerIDs := make([]string, 0, len(s.workers))
	for workerID := range s.workers {
		workerIDs = append(workerIDs, workerID)
	}

	// Assign partitions to workers
	for i, partition := range partitions {
		workerID := workerIDs[i%len(workerIDs)] // Round-robin assignment
		workerAddress := s.workers[workerID]

		go func(workerAddr string, taskID, executable, partition, destFile string) {
			client, conn, err := connectToWorker(workerAddr)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v", workerAddr, err)
				s.reassignTask(taskID, partition, destFile)
				return
			}
			defer conn.Close()

			_, err = client.ExecuteTask(context.Background(), &api.TaskData{
				TaskId:     taskID,
				SrcFile:    partition,
				DestFile:   destFile,
				Executable: executable,
			})
			if err != nil {
				log.Printf("Worker %s failed to execute task %s; error: %v", workerAddr, taskID, err)
				s.reassignTask(taskID, partition, destFile)
			}
		}(workerAddress, fmt.Sprintf("%s-part-%d", taskAssignment.TaskId, i), taskAssignment.Executable, partition, taskAssignment.DestFile)
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

func partitionInput(numTasks int32, srcFile string, hydfsClient *client.Client) ([]string, error) {
	partitions := make([]string, numTasks)
	lines := []string{}

	// Read the input file from HyDFS
	resp, _ := hydfsClient.SendRequest(client.Request{
		Operation: client.GET,
		HyDFSFile: srcFile,
	})
	if resp.Status != "success" {
		return nil, fmt.Errorf("Failed to fetch source file: %s", resp.Message)
	}

	lines = strings.Split(resp.Message, "\n")
	linesPerPartition := len(lines) / int(numTasks)

	for i := int32(0); i < numTasks; i++ {
		start := int(i) * linesPerPartition
		end := start + linesPerPartition
		if i == numTasks-1 { // Handle remainder
			end = len(lines)
		}

		partition := lines[start:end]
		partitionName := fmt.Sprintf("%s-part-%d", srcFile, i)

		// Write partition back to HyDFS
		_, _ = hydfsClient.SendRequest(client.Request{
			Operation: client.CREATE,
			HyDFSFile: partitionName,
			Content:   strings.Join(partition, "\n"),
		})
		partitions[i] = partitionName
	}

	return partitions, nil
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

func (s *leaderServer) monitorHeartbeats() {
	for {
		time.Sleep(10 * time.Second)
		// Logic to check heartbeats and detect failures
	}
}

func main() {
	// nodes := loadNodes("nodes.config")
	hydfsClient := client.NewClient("fa24-cs425-0701.cs.illinois.edu:8080")
	defer hydfsClient.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	leader := &leaderServer{
		workers:     make(map[string]string),
		tasks:       make(map[string]string),
		hydfsClient: hydfsClient,
		// nodes:   nodes,
	}

	// TODO:
	// go leader.monitorHeartbeats()

	api.RegisterLeaderServiceServer(s, leader)
	log.Println("LeaderService Server running on port 50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
