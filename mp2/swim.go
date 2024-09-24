// for now, just work on
// starting a server with GIVEN membership list
// only ping-ack, no piggybacking yet

// Ping is a little more complicated
// Ping j -> res -> complete
// Ping j -> no res -> ask k other nodes to ping j -> if no res, then its failed


// Ack is simple, just listen and ack


// 2 threads, one for ack, one for general pinging process

// Later on, we'll add piggybacking to every communication

package main

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

var membershipList = []string{
	"172.22.156.22:5000",
	// "fa24-cs425-0701.cs.illinois.edu:5000",
	// "fa24-cs425-0702.cs.illinois.edu:8080",
	// "fa24-cs425-0703.cs.illinois.edu:8080",
	// "fa24-cs425-0704.cs.illinois.edu:8080",
	// "fa24-cs425-0705.cs.illinois.edu:8080",
	// "fa24-cs425-0706.cs.illinois.edu:8080",
	// "fa24-cs425-0707.cs.illinois.edu:8080",
	// "fa24-cs425-0708.cs.illinois.edu:8080",
	// "fa24-cs425-0709.cs.illinois.edu:8080",
	// "fa24-cs425-0710.cs.illinois.edu:8080",
}


func ack(wg *sync.WaitGroup, addr *net.UDPAddr) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error setting up UDP listener: %v\n", err)
		return
	}
	defer conn.Close() // Close the connection when done

	fmt.Printf("Listening for ACKs on %s\n", addr.String())

	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			return
		}
		fmt.Printf("Received message: %s from %s\n", string(buf[:n]), remoteAddr.String())

		// Send ACK back to the sender
		_, err = conn.WriteToUDP([]byte("ACK"), remoteAddr)
		if err != nil {
			fmt.Printf("Error sending ACK to %s: %v\n", remoteAddr.String(), err)
		} else {
			fmt.Printf("Sent ACK to %s\n", remoteAddr.String())
		}
	}
}

// Function to ping a random address from the provided list every second
func pingRandomAddress(wg *sync.WaitGroup, addresses []string) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	for {
		// Choose a random address
		randomIndex := rand.Intn(len(addresses))
		address := addresses[randomIndex]

		// Attempt to send a ping
		fmt.Printf("Pinging %s...\n", address)

		// Create a UDP connection
		conn, err := net.Dial("udp", address)
		if err != nil {
			fmt.Printf("Error dialing %s: %v\n", address, err)
			time.Sleep(1 * time.Second) // Sleep before the next ping
			continue
		}
		defer conn.Close()

		// Send a PING message
		_, err = conn.Write([]byte("PING"))
		if err != nil {
			fmt.Printf("Error sending PING to %s: %v\n", address, err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Set a deadline for receiving a response
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		// Read the response
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("No response from %s: %v\n", address, err)
		} else {
			fmt.Printf("Received response from %s: %s\n", address, string(buf[:n]))
		}

		// Sleep for 1 second before the next ping
		time.Sleep(1 * time.Second)
	}
}

func main() {
	var wg sync.WaitGroup
	addr := &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 5000,
	}

	wg.Add(1)
	go ack(&wg, addr) // Start our ACK goroutine


	wg.Add(1)
	go pingRandomAddress(&wg, membershipList)

	select {}
}