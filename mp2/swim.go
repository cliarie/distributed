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
	// "math/rand"
	"net"
	"sync"
	"time"
)

type Member struct {
	Address string // ip:port of the member
	Status  string // Status of the member (ALIVE, FAILED)
}

var membershipList = map[string]Member{
	"172.22.158.22:5000": {Status: "ALIVE"},
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
		fmt.Printf("Received PING from %s\n", remoteAddr.String())

		// Send ACK back to the sender
		_, err = conn.WriteToUDP([]byte("ACK"), remoteAddr)
		if err != nil {
			fmt.Printf("Error sending ACK to %s: %v\n", remoteAddr.String(), err)
		} else {
			fmt.Printf("Sent ACK to %s\n", remoteAddr.String())
		}
	}
}

// Function to follow ping protocol for a specific address in a single cycle
// and implement sucess and marking the node as failed
// A separate function will create the random permutation list every n iterations
func pingAddress(wg *sync.WaitGroup) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	for {
		// Choose a random address
		address := "172.22.158.22:5000" // address will be given as an argument

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
			fmt.Printf("No ACK from %s. Attempting indirect ping.\n", address)
			// ping k random addresses that we have, asking them to ping for us
			// We'll have to handle this differently in the ack. maybe smth like
			// indirect request ip:port
			// and just parse that
			// then it tries to ping it, and if it works send back an ACK
			// indirect ACK
			// actually, i should have a single listening goroutine which sends the recieved messages to other
			// channels to get processesed!
			
			// ask 3 random addressess to ping it for us
			// "indirect ping ??"
		} else {
			fmt.Printf("Received ACK from %s\n", address)
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
	go pingAddress(&wg)

	select {}
}