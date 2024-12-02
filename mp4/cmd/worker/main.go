package main

import (
	"context"
	"log"
	"mp4/pkg/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
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

}
