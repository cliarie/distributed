// NOTES: 
// - finished piggybacking
// - for every incomiing message, it'll be in the format
//    <message>|member1|status1|incarnation1|member2|status2|incarnation2|...
// - for outgoing messages, I tack the entire membership list on the back of it
// - I'm not sure if we need to make it so that we only piggyback a partial membership list

// TODO:
// 1) add leaves?
// 2) add suspicion
// 3) add/verify marshalling (Ensure that any platform-dependent fields (e.g., ints) are
//                             marshaled (converted) into a platform-independent format.)
// and thats it i think

package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
	"strings"
	"strconv"
	"log"
	"io"
)


var logger *log.Logger

var pingTimeout = 2 * time.Second
var introducerAddress = "fa24-cs425-0701.cs.illinois.edu:8080"

type Member struct {
	Status      string // Status of the member (ALIVE, FAILED)
	Incarnation int    // incarnation of member
}

var (
	membershipList = map[string]Member{ }
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
			membershipList[address] = Member{
				Status:      status,
				Incarnation: incarnation,
			}
			logger.Printf("UPDATE: Successfully updated %s to %s\n", address, status)
		} else if incarnation == member.Incarnation {
			if (status == "SUSPECTED" && member.Status == "ALIVE") || (status == "FAILED" && (member.Status == "ALIVE" || member.Status == "SUSPECTED")) {
				membershipList[address] = Member{
					Status:      status,
					Incarnation: incarnation,
				}
				logger.Printf("UPDATE: Successfully updated %s to %s\n", address, status)
			}
		} else {
			logger.Printf("Stale update for %s with older incarnation %d\n", address, incarnation)
		}
	} else {
		membershipList[address] = Member{
			Status:      status,
			Incarnation: incarnation,
		}
		logger.Printf("JOIN: Added new member, %s, with incarnation %d\n", address, incarnation)
	}
	// logger.Printf("membership list after updating: %v\n", membershipList)
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
		logger.Printf("Error setting up UDP listener: %v\n", err)
		return
	}
	defer conn.Close() // Close the connection when done

	logger.Printf("Listening on %s\n", addr.String())

	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			logger.Printf("Error reading from UDP: %v\n", err)
			return
		}
		payload := string(buf[:n])
		message := strings.Split(payload, "|")[0]
		logger.Printf("Received PING from %s, message = %s\n", remoteAddr.String(), message)

		// check if message is direct or indirect ping
		if message == "PING" {
			// direct message, send ACK back to sender
			_, err = conn.WriteToUDP([]byte(addPiggybackToMessage("ACK")), remoteAddr)
			if err != nil {
				logger.Printf("Error sending ACK to %s: %v\n", remoteAddr.String(), err)
			} else {
				logger.Printf("Sending ACK to %s...\n", remoteAddr.String())
			}
		} else if len(message) > 5 && message[:5] == "PING " {
			// indirect PING, get target address and handle with indirectPingHandler
			targetAddress := message[5:]
			go indirectPingHandler(targetAddress, remoteAddr.String()) // initiate go routine
		}
		
		if message == "JOIN" {
			// piggyback membership list to sender
			_, err = conn.WriteToUDP([]byte(addPiggybackToMessage("INFO")), remoteAddr)
			if err != nil {
				logger.Printf("Error sending INFO to %s: %v\n", remoteAddr.String(), err)
			} else {
				logger.Printf("Sending INFO to %s...\n", remoteAddr.String())
			}
		}
		processPiggyback(payload)
	}
}

/*
sends ping to target address, reports back to requester if ACK is received
*/
func indirectPingHandler(targetAddress string, requester string) {
	logger.Printf("Handling indirect PING request for target %s from requester %s\n", targetAddress, requester)

	conn, err := net.Dial("udp", targetAddress)
	if err != nil {
		logger.Printf("Error dialing %s: %v\n", targetAddress, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(addPiggybackToMessage("PING")))
	if err != nil {
		logger.Printf("Error sending PING to %s: %v", targetAddress, err)
	}

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(pingTimeout))
	n, err := conn.Read(buf)
	
	if err != nil {
		logger.Printf("Error reading ACK, no ACK from %s\n", targetAddress)
	} else {
		// ACK received, inform requester
		processPiggyback(string(buf[:n]))
		requesterconn, err := net.Dial("udp", requester)
		if err != nil {
			logger.Printf("Error dialing requester %s: %v", requester, err)
			return
		}
		defer requesterconn.Close()

		_, err = requesterconn.Write([]byte(addPiggybackToMessage("INDIRECT_ACK")))
		if err != nil {
			logger.Printf("Error sending INDIRECT ACK to %s: %v", requester, err)
		} else {
			logger.Printf("Send INDIRECT ACK to %s", requester)
		}
	}
}

/*
Function to follow ping protocol for a random target in a single cycle
and implement sucess and marking the node as failed
*/
func processPingCycle(wg *sync.WaitGroup, localAddress string) {
	defer wg.Done() // Signal that this goroutine is done when it exits
	time.Sleep(4 * time.Second)

	for {
		// Choose a random address (unsure if we can ping already pinged address)
		address := getRandomAliveNode(localAddress) // address selected randomly from membershiplist, besides self
		if address == "" {
			logger.Printf("No alive nodes.\n")
			printMembershipList()
			time.Sleep(1 * time.Second)
			continue
		}
		res := pingSingleAddress(address)
		if res != 0 {
			logger.Printf("No ACK from %s. Attempting indirect ping.\n", address)
			nodesToPing := randomKAliveNodes(localAddress, 2)
			var indirectWg sync.WaitGroup
			indirectACK := false
			var ackMutex sync.Mutex // protect indirect ACK bool
			for _, nodeAddress := range nodesToPing {
				indirectWg.Add(1)
				go func(node string) {
					defer indirectWg.Done()
					conn, err := net.Dial("udp", node)
					if err != nil {
						logger.Printf("Error dialing %s: %v\n", node, err)
						return
					}
					defer conn.Close()
					// Send a request to the node to ping the targetAddress
					// ie PING vs PING <address>
					request := fmt.Sprintf("PING %s", address)
					_, err = conn.Write([]byte(addPiggybackToMessage(request)))
					if err != nil {
						return
					}
					// Wait for a response (ACK)
					buf := make([]byte, 1024)
					conn.SetReadDeadline(time.Now().Add(pingTimeout))
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					processPiggyback(string(buf[:n]))
					

					ackMutex.Lock()
					indirectACK = true
					ackMutex.Unlock()
				}(nodeAddress)
			}
			indirectWg.Wait()

			ackMutex.Lock()
			if indirectACK {
				logger.Printf("Received indirect ACK for %s\n", address)
				updateMemberStatus(address, "ALIVE", -1)
			} else {
				logger.Printf("FAILURE DETECTED: No indirect ACK for %s. Marking node as failed.\n", address)
				updateMemberStatus(address, "FAILED", -1)
			}
			ackMutex.Unlock()
		} else {
			logger.Printf("Received ACK from %s\n", address)
			// TODO: fix with incarnation number!
			updateMemberStatus(address, "ALIVE", -1)
		}

		// Sleep for 1 second before the next ping
		printMembershipList()
		time.Sleep(1 * time.Second)
	}
}

// function to ping a given address, and then return 0 if recieved ACK, 1 if no ACK
func pingSingleAddress(address string) int {
	// Attempt to send a ping
	logger.Printf("Sending PING to %s...\n", address)

	// Create a UDP connection
	conn, err := net.Dial("udp", address)
	if err != nil {
		logger.Printf("Error dialing %s: %v\n", address, err)
		return 1
	}
	defer conn.Close()

	// Send a PING message
	_, err = conn.Write([]byte(addPiggybackToMessage("PING")))
	if err != nil {
		logger.Printf("Error sending PING to %s: %v\n", address, err)
		return 1
	}

	// Set a deadline for receiving a response
	conn.SetReadDeadline(time.Now().Add(pingTimeout))

	// Read the response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return 1
	} else {
		processPiggyback(string(buf[:n]))
		return 0
	}
}

// function to ping introducer for intial contact and membership list
func pingIntroducer() int {
	// Attempt to send a JOIN ping
	logger.Printf("Sending JOIN to introducer...\n")

	// Create a UDP connection
	conn, err := net.Dial("udp", introducerAddress)
	if err != nil {
		logger.Printf("Error dialing %s: %v\n", introducerAddress, err)
		return 1
	}
	defer conn.Close()

	// Send a JOIN message
	_, err = conn.Write([]byte(addPiggybackToMessage("JOIN")))
	if err != nil {
		logger.Printf("Error sending JOIN to %s: %v\n", introducerAddress, err)
		return 1
	}

	// Set a deadline for receiving a response
	conn.SetReadDeadline(time.Now().Add(pingTimeout))

	// Read the response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return 1
	} else {
		processPiggyback(string(buf[:n]))
		return 0
	}
}

func printMembershipList() {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	logger.Printf("Current Membership List:\n")
	for address, member := range membershipList {
		logger.Printf("%s, %s, %d\n", address, member.Status, member.Incarnation)
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

// Function to process incoming messages and extract piggyback information
func processPiggyback(message string) {
	// Assume message is formatted like "PING|member1|status1|incarnation1|member2|status2|incarnation2|..."
	parts := strings.Split(message, "|")
	if len(parts) < 4 {
		fmt.Println("Invalid message format for piggyback.")
		return
	}

	// Iterate over parts and update membership list
	for i := 1; i < len(parts); i += 3 {
		address := parts[i]
		status := parts[i+1]
		incarnationStr := parts[i+2]
		// logger.Printf("Member list info: %s, %s, %s\n", address, status, incarnationStr)

		// Parse incarnation number
		incarnation, err := strconv.Atoi(incarnationStr)
		if err != nil {
			logger.Printf("Error parsing incarnation for %s: %v\n", address, err)
			continue
		}

		// Update membership status
		updateMemberStatus(address, status, incarnation)
	}
}

// Function to add the piggyback message containing the membership list to the message
func addPiggybackToMessage(message string) string {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	var piggyback strings.Builder
	piggyback.WriteString(message)
	for address, member := range membershipList {
		piggyback.WriteString(fmt.Sprintf("|%s|%s|%d", address, member.Status, member.Incarnation))
	}

	return piggyback.String()
}

func main() {

	var wg sync.WaitGroup

	localAddress := os.Getenv("LOCAL_ADDRESS")
	if localAddress == "" {
		fmt.Println("set LOCAL_ADDRESS environtment variable with export LOCAL_ADDRESS=")
		os.Exit(1)
	}
	logFile := os.Getenv("LOGFILE")
	if logFile == "" {
		fmt.Println("set LOGFILE environtment variable with export LOGFILE=")
		os.Exit(1)
	}
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	multiWriter := io.MultiWriter(os.Stdout, file)
	logger = log.New(multiWriter, "", log.Ldate|log.Ltime)

	// No need to know whether it is introducer, because only the introducer will get 
	// new node join requests
	var isIntroducer = false
	if localAddress == introducerAddress {
		isIntroducer = true
	}
	
	addr, _ := net.ResolveUDPAddr("udp", localAddress)

	updateMemberStatus(localAddress, "ALIVE", 0) // Add self to membership list

	wg.Add(1)
	go listener(&wg, addr) // Start our ACK goroutine

	if !isIntroducer {
		for pingIntroducer() != 0 { // Ask introducer to introduce
			logger.Printf("Unable to contact introducer at %s, retrying...\n", introducerAddress)
			time.Sleep(1 * time.Second)
		}
	}

	wg.Add(1)
	go processPingCycle(&wg, localAddress)

	wg.Wait()
	select {}
}
