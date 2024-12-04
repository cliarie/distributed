package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"

	"mp4/pkg/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 6 {
		log.Fatalf("Invalid input: Usage RainStorm <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
	}

	command := strings.Join(os.Args[1:], " ")
	args := strings.Fields(command)

	if len(args) != 5 {
		log.Fatalf("Invalid input: Usage RainStorm <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
	}

	op1Exe := args[0]
	op2Exe := args[1]
	srcFile := args[2]
	destFile := args[3]
	numTasks := args[4]

	// connect to the leader
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := api.NewLeaderServiceClient(conn)

	// submit job
	taskAssignment := &api.TaskAssignment{
		TaskId:     "job-1", // generate unique ID for each job
		Operator:   op1Exe,
		Executable: op2Exe,
		NumTasks:   parseNumTasks(numTasks),
		SrcFile:    srcFile,
		DestFile:   destFile,
	}
	resp, err := client.AssignTask(context.Background(), taskAssignment)
	if err != nil {
		log.Fatalf("Job submission failed: %v", err)
	}
	log.Printf("Job submitted: success=%v, message=%v", resp.Success, resp.Message)
}

func parseNumTasks(numTasks string) int32 {
	parsed, err := strconv.Atoi(numTasks)
	if err != nil || parsed < 1 {
		log.Fatalf("Invalid num_tasks: must be a positive integer")
	}
	return int32(parsed)
}
