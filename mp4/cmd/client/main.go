package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"mp4/pkg/api"
	"mp4/pkg/hydfs/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 6 {
		log.Fatalf("Invalid input: Usage RainStorm <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
	}

	// Preserve quoted arguments
	args := parseArguments(os.Args[1:])

	if len(args) != 5 {
		log.Fatalf("Invalid input: Usage RainStorm <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
	}

	jobID := generateJobID()
	op1Exe := args[0]
	op2Exe := args[1]
	srcFile := args[2]
	destFile := args[3]
	numTasks := args[4]

	hydfsClient := client.NewClient("fa24-cs425-0701.cs.illinois.edu:8080")
	defer hydfsClient.Close()

	resp, _ := hydfsClient.SendRequest(client.Request{
		Operation: client.GET,
		HyDFSFile: srcFile,
	})
	if resp.Status == "error" {
		log.Fatalf("Input file validation failed: %s", resp.Message)
	}

	// Connect to the leader
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := api.NewLeaderServiceClient(conn)

	// Submit job
	taskAssignment := &api.TaskAssignment{
		TaskId:      jobID, // Generate unique ID for each job
		Executable1: op1Exe,
		Executable2: op2Exe,
		NumTasks:    parseNumTasks(numTasks),
		SrcFile:     srcFile,
		DestFile:    destFile,
	}
	_, err = client.AssignTask(context.Background(), taskAssignment)
	if err != nil {
		log.Fatalf("Job submission failed: %v", err)
	}
	fmt.Printf("Job submitted successfully, message=%v\n", resp.Message)
}

// parseArguments handles preserving quoted arguments
func parseArguments(args []string) []string {
	result := []string{}
	current := ""
	inQuotes := false

	for _, arg := range args {
		if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
			// Single quoted argument
			result = append(result, strings.Trim(arg, "\""))
		} else if strings.HasPrefix(arg, "\"") {
			// Start of a quoted argument
			inQuotes = true
			current = strings.TrimPrefix(arg, "\"")
		} else if strings.HasSuffix(arg, "\"") && inQuotes {
			// End of a quoted argument
			current += " " + strings.TrimSuffix(arg, "\"")
			result = append(result, current)
			current = ""
			inQuotes = false
		} else if inQuotes {
			// Inside a quoted argument
			current += " " + arg
		} else {
			// Regular argument
			result = append(result, arg)
		}
	}

	if inQuotes {
		log.Fatalf("Invalid input: unmatched quotes in arguments")
	}

	return result
}

func parseNumTasks(numTasks string) int32 {
	parsed, err := strconv.Atoi(numTasks)
	if err != nil || parsed < 1 {
		log.Fatalf("Invalid num_tasks: must be a positive integer")
	}
	return int32(parsed)
}

func generateJobID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("job-%d", rand.Intn(1000000))
}
