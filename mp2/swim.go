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
	"os"
	"sync"
	"time"
)

var pingTimeout = 2 * time.Second

type Member struct {
	Status      string // Status of the member (ALIVE, FAILED)
	Incarnation int    // incarnation of member
}

var (
	membershipList = map[string]Member{
		"fa24-cs425-0701.cs.illinois.edu:8080": {Status: "ALIVE"},
		"fa24-cs425-0702.cs.illinois.edu:8080": {Status: "ALIVE"},
		"fa24-cs425-0703.cs.illinois.edu:8080": {Status: "ALIVE"},
	}
	membershipMutex sync.Mutex // concurrent access to membership list
)

// helper function to update membership list (safely)
// pass incarnation -1 when we detect a server down by ourselves
func updateMemberStatus(address string, status string, incarnation int) {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	if member, exists := membershipList[address]; exists {
		if incarnation == -1 {
			incarnation = member.Incarnation
		}
		if incarnation > member.Incarnation {
			member.Status = status
			member.Incarnation = incarnation
			fmt.Printf("Succcessfully updated %s to %s\n", address, status)
		} else if incarnation == member.Incarnation && (status == "SUSPECTED" && member.Status == "ALIVE") || (status == "FAILED" && (member.Status == "ALIVE" || member.Status == "SUSPECTED")) {
			member.Status = status
			fmt.Printf("Succcessfully updated %s to %s\n", address, status)
		} else {
			fmt.Printf("Stale update for %s with older inst %d\n", address, incarnation)
		}
	} else {
		membershipList[address] = Member{
			Status:      status,
			Incarnation: incarnation,
		}
		fmt.Printf("Added new member, %s, with incarnation %d\n", address, incarnation)
	}
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
	fmt.Printf("Handling indirect PING request for target %s from requester %s\n", targetAddress, requester)

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
	conn.SetReadDeadline(time.Now().Add(pingTimeout))
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
Function to follow ping protocol for a random target in a single cycle
and implement sucess and marking the node as failed
*/
func processPingCycle(wg *sync.WaitGroup, localAddress string) {
	defer wg.Done() // Signal that this goroutine is done when it exits

	for {
		// Choose a random address (unsure if we can ping already pinged address)
		address := getRandomAliveNode(localAddress) // address selected randomly from membershiplist, besides self
		res := pingSingleAddress(address)
		if res != 0 {
			fmt.Printf("No ACK from %s. Attempting indirect ping.\n", address)
			nodesToPing := randomKAliveNodes(localAddress, 2)
			fmt.Println(nodesToPing)
			var indirectWg sync.WaitGroup
			indirectACK := false
			var ackMutex sync.Mutex // protect indirect ACK bool
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
					// ie PING vs PING <address>
					request := fmt.Sprintf("PING %s", address)
					_, err = conn.Write([]byte(request))
					if err != nil {
						return
					}
					// Wait for a response (ACK)
					buf := make([]byte, 1024)
					conn.SetReadDeadline(time.Now().Add(pingTimeout))
					_, err = conn.Read(buf)
					if err != nil {
						return
					}

					ackMutex.Lock()
					indirectACK = true
					ackMutex.Unlock()
				}(nodeAddress)
			}
			indirectWg.Wait()

			ackMutex.Lock()
			if indirectACK {
				fmt.Printf("Received indirect ACK for %s\n", address)
				updateMemberStatus(address, "ALIVE", -1)
			} else {
				fmt.Printf("No indirect ACK for %s. Marking node as failed.\n", address)
				updateMemberStatus(address, "FAILED", -1)
			}
			ackMutex.Unlock()
		} else {
			fmt.Printf("Received ACK from %s\n", address)
			// TODO: fix with incarnation number!
			updateMemberStatus(address, "ALIVE", -1)
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
	conn.SetReadDeadline(time.Now().Add(pingTimeout))

	// Read the response
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		return 1
	} else {
		return 0
	}
}

func getRandomAliveNode(localAddress string) string {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	aliveNodes := []string{}
	for address, member := range membershipList {
		if member.Status == "ALIVE" && address != localAddress {
			aliveNodes = append(aliveNodes, address)
		}
	}
	if len(aliveNodes) == 0 {
		return ""
	}

	randomSource := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(randomSource)

	return aliveNodes[rng.Intn(len(aliveNodes))]
}

func randomKAliveNodes(localAddress string, n int) []string {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	aliveNodes := []string{}
	for address, member := range membershipList {
		if member.Status == "ALIVE" && address != localAddress {
			aliveNodes = append(aliveNodes, address)
		}
	}

	// Set to track selected indices
	selectedIndices := make(map[int]struct{})
	var selectedNodes []string

	randomSource := rand.NewSource(time.Now().UnixNano()) // rand.seed is deprecated
	rng := rand.New(randomSource)

	for len(selectedNodes) < n && len(selectedIndices) < len(aliveNodes) {
		// Generate a random index
		randomIndex := rng.Intn(len(aliveNodes))

		// Check if this index has already been selected
		if _, exists := selectedIndices[randomIndex]; !exists {
			// Mark this index as selected
			selectedIndices[randomIndex] = struct{}{}
			// Append the corresponding member to the selectedNodes slice
			selectedNodes = append(selectedNodes, aliveNodes[randomIndex])
		}
	}

	return selectedNodes
}

func main() {
	var wg sync.WaitGroup

	localAddress := os.Getenv("LOCAL_ADDRESS")
	if localAddress == "" {
		fmt.Println("set LOCAL_ADDRESS environtment variable with export LOCAL_ADDRESS=")
		os.Exit(1)
	}
	// delete(membershipList, localAddress)
	
	addr, _ := net.ResolveUDPAddr("udp", localAddress)

	wg.Add(1)
	go listener(&wg, addr) // Start our ACK goroutine

	wg.Add(1)
	go processPingCycle(&wg, localAddress)

	wg.Wait()
	select {}
}
