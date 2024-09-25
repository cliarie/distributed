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

type Member struct {
	Address string // ip:port of the member
	Status  string // Status of the member (ALIVE, FAILED)
}

var membershipList = map[string]Member{
	"172.22.158.22:5000": {Status: "ALIVE"},
}

/*
respond to direct pings and handle requests to ping other nodes on behalf of requester
treat direct and indirect ping requests separately:

	direct PING format: "PING"
		respond with "ACK" to comfirm node is alive
	indirect PING format: "PING <target address>"
		extract target address, then initiate indirect ping with indirect ping handler
*/
func listener(wg *sync.WaitGroup, addr *net.UDPAddr) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error setting up UDP listener: %v\n", err)
		return
	}
	defer conn.Close() // Close the connection when done

	fmt.Printf("Listening on %s\n", addr.String())

	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			return
		}
		message := string(buf[:n])
		fmt.Printf("Received PING from %s, message = %s\n", remoteAddr.String(), message)

		// check if message is direct or indirect ping
		if message == "PING" {
			// direct message, send ACK back to sender
			_, err = conn.WriteToUDP([]byte("ACK"), remoteAddr)
			if err != nil {
				fmt.Printf("Error sending ACK to %s: %v\n", remoteAddr.String(), err)
			} else {
				fmt.Printf("Sending ACK to %s...\n", remoteAddr.String())
			}
		} else if len(message) > 5 && message[:4] == "PING" {
			// indirect PING, get target address and handle with indirectPingHandler
			targetAddress := message[5:]
			go indirectPingHandler(targetAddress, remoteAddr.String()) // initiate go routine
		}

	}
}

/*
sends ping to target address, reports back to requester if ACK is received
*/
func indirectPingHandler(targetAddress string, requester string) {
	conn, err := net.Dial("udp", targetAddress)
	if err != nil {
		fmt.Printf("Error dialing %s: %v\n", targetAddress, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte("PING"))
	if err != nil {
		fmt.Printf("Error sending PING to %s: %v", targetAddress, err)
	}

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(buf)

	if err != nil {
		fmt.Printf("Error reading ACK, no ACK from %s\n", targetAddress)
	} else {
		// ACK received, inform requester
		requesterconn, err := net.Dial("udp", requester)
		if err != nil {
			fmt.Printf("Error dialing requester %s: %v", requester, err)
			return
		}
		defer requesterconn.Close()

		_, err = requesterconn.Write([]byte("INDIRECT_ACK"))
		if err != nil {
			fmt.Printf("Error sending INDIRECT ACK to %s: %v", requester, err)
		} else {
			fmt.Printf("Send INDIRECT ACK to %s", requester)
		}
	}

}

/*
Function to follow ping protocol for a specific address in a single cycle
and implement sucess and marking the node as failed
A separate function will create the random permutation list every n iterations
*/
func processPingCycle(wg *sync.WaitGroup) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	for {
		// Choose a random address
		address := "172.22.158.22:5000" // address will be given as an argument
		res := pingSingleAddress(address)
		if res != 0 {
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
			nodesToPing := randomKNodes(3)
			var indirectWg sync.WaitGroup
			indirectACK := false
			for _, nodeAddress := range nodesToPing {
				indirectWg.Add(1)
				go func(node string) {
					defer indirectWg.Done()
					conn, err := net.Dial("udp", node)
					if err != nil {
						fmt.Printf("Error dialing %s: %v\n", node, err)
						return
					}
					defer conn.Close()
					// Send a request to the node to ping the targetAddress
					// TODO make listener handle for indirect ping requests
					// ie PING vs PING <address>
					request := fmt.Sprintf("PING %s", address)
					_, err = conn.Write([]byte(request))
					if err != nil {
						return
					}
					// Wait for a response (ACK)
					buf := make([]byte, 1024)
					conn.SetReadDeadline(time.Now().Add(2 * time.Second))
					_, err = conn.Read(buf)
					if err != nil {
						return
					}

					// TODO: add a mutex lock for this
					indirectACK = true
				}(nodeAddress)
			}
			indirectWg.Wait()
			if indirectACK {
				fmt.Printf("Received indirect ACK for %s\n", address)
			} else {
				fmt.Printf("No indirect ACK for %s. Marking node as failed.\n", address)
			}
		} else {
			fmt.Printf("Received ACK from %s\n", address)
		}

		// Sleep for 1 second before the next ping
		time.Sleep(1 * time.Second)
	}
}

// function to ping a given address, and then return 0 if recieved ACK, 1 if no ACK
func pingSingleAddress(address string) int {
	// Attempt to send a ping
	fmt.Printf("Sending PING to %s...\n", address)

	// Create a UDP connection
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("Error dialing %s: %v\n", address, err)
		return 1
	}
	defer conn.Close()

	// Send a PING message
	_, err = conn.Write([]byte("PING"))
	if err != nil {
		fmt.Printf("Error sending PING to %s: %v\n", address, err)
		return 1
	}

	// Set a deadline for receiving a response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Read the response
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		return 1
	} else {
		return 0
	}
}

func randomKNodes(n int) []string {
	keys := make([]string, 0, len(membershipList))
	for key := range membershipList {
		keys = append(keys, key)
	}

	// Set to track selected indices
	selectedIndices := make(map[int]struct{})
	var selectedNodes []string

	rand.Seed(time.Now().UnixNano())

	for len(selectedNodes) < n && len(selectedIndices) < len(keys) {
		// Generate a random index
		randomIndex := rand.Intn(len(keys))

		// Check if this index has already been selected
		if _, exists := selectedIndices[randomIndex]; !exists {
			// Mark this index as selected
			selectedIndices[randomIndex] = struct{}{}
			// Append the corresponding member to the selectedNodes slice
			selectedNodes = append(selectedNodes, keys[randomIndex])
		}
	}

	return selectedNodes
}

func main() {
	var wg sync.WaitGroup
	addr := &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 5000,
	}

	wg.Add(1)
	go listener(&wg, addr) // Start our ACK goroutine

	wg.Add(1)
	go processPingCycle(&wg)

	select {}
}
