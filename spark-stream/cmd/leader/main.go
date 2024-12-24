// initializes the leader server, sets up grpc server, starts listening for worker registrations and task assignments
// monitors heartbeats from workers

// on leader (vm1) run: go run main.go and also run hydfs/server/server.go
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
    "strconv"
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
	count       map[string]int
	// nodes    []string // list of nodes, see nodes.config
}
// extractPartNumber splits the file name and extracts the part number.
func extractPartNumber(fileName string) int {
    // Split the file name by '-'
    parts := strings.Split(fileName, "-")
    if len(parts) < 5 {
        log.Printf("Error extracting part number: file name %s does not have enough parts", fileName)
        return -1
    }

    // Extract the last part and remove the ".csv" suffix
    partStr := strings.TrimSuffix(parts[len(parts)-1], ".csv")

    // Convert the part string to an integer
    partNumber, err := strconv.Atoi(partStr)
    if err != nil {
        log.Printf("Error converting part number to integer: %v", err)
        return -1
    }

    return partNumber
}

func isAggregation(content string) bool {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return false
	}
	parts := strings.Split(lines[0], ",")
	if len(parts) == 2 {
		if _, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			return true
		}
	}
	return false
}

func (s *leaderServer) getCountAsString() string {
    var result strings.Builder
    for key, value := range s.count {
        line := fmt.Sprintf("%s,%d\n", key, value)
        result.WriteString(line)
    }
	result.WriteString("\n")
    return result.String()
}

// updateCountAndLocalFile updates the local count mapping and persists it to a local file.
func (s *leaderServer) updateCount(content string) {
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err == nil {
				s.count[key] += value
			}
		}
	}
}

func (s *leaderServer) AckTask(ctx context.Context, ackInfo *api.AckInfo) (*api.AckResponse, error) {
	log.Printf("Ack received for task %s", ackInfo.TaskId)
	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	// Mark task as completed
	delete(s.tasks, ackInfo.TaskId)
	return &api.AckResponse{Success: true, Message: "Acknowledgment received."}, nil
}

func (s *leaderServer) removeWorker(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the worker exists in the workers map
	address, exists := s.workers[workerID]
	if !exists {
		log.Printf("Worker %s does not exist and cannot be removed.", workerID)
		return
	}

	// Remove the worker from the workers map
	delete(s.workers, workerID)
	log.Printf("Removed worker %s with address %s.", workerID, address)
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
	s.count = make(map[string]int)

	log.Printf("Assigning Task: TaskID=%s, Executable1=%s, Executable2=%s, NumTasks=%d, SrcFile=%s, DestFile=%s",
		taskAssignment.TaskId, taskAssignment.Executable1, taskAssignment.Executable2, taskAssignment.NumTasks, taskAssignment.SrcFile, taskAssignment.DestFile)

	// Stage 1: Partition input file and run Executable1
	partitions, err := partitionInput(taskAssignment.NumTasks, taskAssignment.SrcFile, s.hydfsClient)
	if err != nil {
		return nil, fmt.Errorf("Failed to partition input file: %v", err)
	}
	time.Sleep(1 * time.Second)

	intermediateCh := make(chan string, len(partitions))
	finalCh := make(chan string, len(partitions))
	var wg sync.WaitGroup

	log.Println("Partitioning complete. Running Stage 1 and Stage 2 in parallel...")

	// Stage 1
	for i, partition := range partitions {
		wg.Add(1)
		go func(i int, partition string) {
			defer wg.Done()
			workerID := selectWorker(s.workers, i)
			workerAddress := s.workers[workerID]
			intermediateFile := fmt.Sprintf("%s-stage1-part-%d.csv", taskAssignment.TaskId, i)
			taskData := &api.TaskData{
				TaskId:     fmt.Sprintf("%s-part1-%d", taskAssignment.TaskId, i),
				SrcFile:    partition,
				DestFile:   intermediateFile,
				Executable: taskAssignment.Executable1,
			}

			client, conn, err := connectToWorker(workerAddress)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v", workerAddress, err)
				s.removeWorker(workerID)
				s.reassignTask(taskData)
				intermediateCh <- intermediateFile
				return
			}
			defer conn.Close()

			_, err = client.ExecuteTask(context.Background(), taskData)
			if err != nil {
				log.Printf("Worker %s failed to execute task %s; error: %v", workerAddress, taskAssignment.TaskId, err)
				s.removeWorker(workerID)
				s.reassignTask(taskData)
			}

			intermediateCh <- intermediateFile
		}(i, partition)
	}

	// Stage 2
	go func() {
		for intermediateFile := range intermediateCh {
			wg.Add(1)
			go func(intermediateFile string) {
				defer wg.Done()

				time.Sleep(400 * time.Millisecond)
				// Extract part number from intermediate file name
				partNumber := extractPartNumber(intermediateFile)
				workerID := selectWorker(s.workers, partNumber)
				workerAddress := s.workers[workerID]
				finalIntermediateFile := fmt.Sprintf("%s-final-stage-part-%d.csv", taskAssignment.TaskId, partNumber)
				taskData := &api.TaskData{
					TaskId:     fmt.Sprintf("%s-part2-%d", taskAssignment.TaskId, partNumber),
					SrcFile:    intermediateFile,
					DestFile:   finalIntermediateFile,
					Executable: taskAssignment.Executable2,
				}

				client, conn, err := connectToWorker(workerAddress)
				if err != nil {
					log.Printf("Failed to connect to worker %s: %v", workerAddress, err)
					s.reassignTask(taskData)
					finalCh <- finalIntermediateFile
					return
				}
				defer conn.Close()

				_, err = client.ExecuteTask(context.Background(), taskData)
				if err != nil {
					log.Printf("Worker %s failed to execute task %s; error: %v", workerAddress, taskAssignment.TaskId, err)
					s.reassignTask(taskData)
				}

				finalCh <- finalIntermediateFile
			}(intermediateFile)
		}
	}()
	
	s.mu.Lock()
	_ = s.hydfsClient.DeleteRequest(taskAssignment.DestFile)
	_ = s.hydfsClient.CreateRequest("blank", taskAssignment.DestFile)
	s.mu.Unlock()

	

	// Final stage to handle files from finalCh
	go func() {
		for finalFile := range finalCh {
			wg.Add(1)
			go func(finalFile string) {
				defer wg.Done()

				tempFile := fmt.Sprintf("temp_%s", finalFile)
				file, _ := os.OpenFile(tempFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				time.Sleep(400 * time.Millisecond)
				s.mu.Lock()
				resp := s.hydfsClient.GetRequest(finalFile, tempFile)
				file.WriteString("\n")

				if resp == "error" {
					log.Printf("Error fetching file %s\n", finalFile)
					os.Remove(tempFile)
					return
				}

				// Checks if this is aggregation vs simple line output
				contentSlice, _ := os.ReadFile(tempFile)
				content := string(contentSlice)
				if isAggregation(content) {
					s.updateCount(content)
					file, _ = os.OpenFile(tempFile, os.O_TRUNC|os.O_WRONLY, 0644)
					content = s.getCountAsString()
					file.WriteString(content)
				}

				// Output the file contents to the console
				fmt.Printf("Final file update:\n%s\n", content)
				_ = s.hydfsClient.AppendRequest(tempFile, taskAssignment.DestFile)
				s.mu.Unlock()


				// Remove the temp file after use
				err = os.Remove(tempFile)
			}(finalFile)
		}
	}()

	wg.Wait()
	close(intermediateCh)
	close(finalCh)

	log.Println("All tasks completed. Final output generated.")

	return &api.TaskResponse{
		Success: true,
		Message: "Task completed successfully.",
	}, nil
}

// Reassign tasks to a different worker
func (s *leaderServer) reassignTask(taskData *api.TaskData) {
	log.Printf("Reassigning task %s", taskData.TaskId)

	s.mu.Lock()
	defer s.mu.Unlock()

	for workerID, address := range s.workers {
		log.Printf("Reassigning task %s to worker %s", taskData.TaskId, workerID)
		client, conn, err := connectToWorker(address)
		if err != nil {
			log.Printf("Failed to connect to worker %s: %v", workerID, err)
			continue
		}
		defer conn.Close()

		_, err = client.ExecuteTask(context.Background(), taskData)
		if err == nil {
			return
		}
		log.Printf("Worker %s failed: %v", workerID, err)
	}
}

func partitionInput(numTasks int32, srcFile string, hydfsClient *client.Client) ([]string, error) {
	// Local file to store fetched data temporarily
	localFile := fmt.Sprintf("local_%s", srcFile)

	// Fetch file from HyDFS and stores as local file
	resp := hydfsClient.GetRequest(srcFile, localFile)
	if resp == "error" {
		log.Fatalf("Input file download failed")
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
		// log.Println(strings.Join(partition, "\n"))

		// Write partition to a temporary local file
		tempLocalFile := fmt.Sprintf("temp_%s", partitionName)
		err := os.WriteFile(tempLocalFile, []byte(strings.Join(partition, "\n")), 0644)
		if err != nil {
			return nil, fmt.Errorf("Failed to write temporary file %s: %v", tempLocalFile, err)
		}

		// Upload local file to HyDFS
		hydfsClient.DeleteRequest(partitionName)
		resp := hydfsClient.CreateRequest(tempLocalFile, partitionName)
		if resp == "error" {
			log.Printf("Failed to create partition.")
			return nil, fmt.Errorf("Failed to create partition %s", partitionName)
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
	// Open or create the log file
	logFile, _ := os.OpenFile("leader.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	defer logFile.Close()
	// Create a multi-writer to write to both stdout and the log file
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	// Set the output of the log package to the multi-writer
	log.SetOutput(multiWriter)


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
		count:       make(map[string]int),
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
