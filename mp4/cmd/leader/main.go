// initializes the leader server, sets up grpc server, starts listening for worker registrations and task assignments
// monitors heartbeats from workers
package main

import (
	"context"
	"fmt"
	"io"
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
	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	log.Printf("Assigning Task: TaskID=%s, Executable=%s, NumTasks=%d, SrcFile=%s, DestFile=%s",
		taskAssignment.TaskId, taskAssignment.Executable, taskAssignment.NumTasks, taskAssignment.SrcFile, taskAssignment.DestFile)
	// Add the filter value to the executable command
	filterValue := "STOP" // Replace "X" with the actual filter value, e.g., passed as a parameter to RainStorm
	executableWithParams := fmt.Sprintf("%s %s", taskAssignment.Executable, filterValue)

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
		log.Printf("Assigning partition %d to worker %s (%s)", i, workerID, workerAddress)

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
				Executable: executableWithParams,
			})
			if err != nil {
				log.Printf("Worker %s failed to execute task %s; error: %v", workerAddr, taskID, err)
				s.reassignTask(taskID, partition, destFile)
			}
		}(workerAddress, fmt.Sprintf("%s-part-%d", taskAssignment.TaskId, i), partition, taskAssignment.DestFile, executableWithParams)
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
	// Local file to store fetched data temporarily
	localFile := fmt.Sprintf("local_%s", srcFile)

	// Fetch file from HyDFS and stream to local file
	resp, reader := hydfsClient.SendRequest(client.Request{
		Operation: client.GET,
		HyDFSFile: srcFile,
	})
	if resp.Status != "success" {
		fmt.Println(resp.Message)
	}
	// Open local file for writing
	file, err := os.Create(localFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to create local file: %v", err)
	}
	defer file.Close()

	// Stream file data from server
	buffer := make([]byte, 4096)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return nil, fmt.Errorf("Failed to read from stream: %v", err)
		}

		// Write to local file
		_, err = file.Write(buffer[:n])
		if err != nil {
			return nil, fmt.Errorf("Failed to write to local file: %v", err)
		}
	}

	log.Println("File fetched successfully and stored locally.")

	// Read the file content into memory for partitioning
	fileContent, err := os.ReadFile(localFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to read local file content: %v", err)
	}

	lines := strings.Split(string(fileContent), "\n")
	totalLines := len(lines)

	// Partitioning logic
	partitions := make([]string, numTasks)
	linesPerPartition := totalLines / int(numTasks)
	remainder := totalLines % int(numTasks)

	start := 0
	for i := int32(0); i < numTasks; i++ {
		end := start + linesPerPartition
		if i == numTasks-1 { // Add the remainder to the last partition
			end += remainder
		}

		partition := lines[start:end]
		partitionName := fmt.Sprintf("%s-part-%d.csv", strings.TrimSuffix(srcFile, ".csv"), i)

		// Debug: Log the partition content
		log.Printf("Partition %d (%s): Start=%d, End=%d, Lines=%d", i, partitionName, start, end, len(partition))
		log.Println(strings.Join(partition, "\n"))

		// Write partition to a temporary local file
		tempLocalFile := fmt.Sprintf("temp_%s", partitionName)
		err := os.WriteFile(tempLocalFile, []byte(strings.Join(partition, "\n")), 0644)
		if err != nil {
			return nil, fmt.Errorf("Failed to write temporary file %s: %v", tempLocalFile, err)
		}

		// Upload local file to HyDFS
		resp, _ := hydfsClient.SendRequest(client.Request{
			Operation: client.CREATE,
			HyDFSFile: partitionName,
			LocalFile: tempLocalFile, // Specify the local file for upload
		})

		if resp.Status != "success" {
			log.Printf("Failed to create partition %s: %s", partitionName, resp.Message)
			return nil, fmt.Errorf("Failed to create partition %s: %s", partitionName, resp.Message)
		}

		// Clean up temporary local file
		os.Remove(tempLocalFile)

		partitions[i] = partitionName
		start = end
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
