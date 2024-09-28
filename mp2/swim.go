// NOTES:
// - FINISHED PIGGYBACKING
// - for every incomiing message, it'll be in the format
//    <message>|member1|status1|incarnation1|member2|status2|incarnation2|...
// - for outgoing messages, I tack the entire membership list on the back of it
// - I'm not sure if we need to make it so that we only piggyback a partial membership list
// - FINISHED SUSPICION

// https://piazza.com/class/lzapkyodcvm4t2/post/292
// TODO: (probably like 10-30 minutes of work)
// 2) add/verify marshalling (Ensure that any platform-dependent fields (e.g., ints) are
//                             marshaled (converted) into a platform-independent format.)
// and then we should probably test it with all 10 vms following the piazza demo test format

package main

import (
	"io/ioutil"
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Member struct {
	Status      string // Status of the member (ALIVE, FAILED)
	Incarnation int    // incarnation of member
}

var (
	membershipList  = map[string]Member{} // key format "address"
	membershipMutex sync.Mutex            // concurrent access to membership list
)

var logger *log.Logger
var pingTimeout = 2 * time.Second
var introducerAddress = "fa24-cs425-0701.cs.illinois.edu:8080"

var (
	cycle            = 0
	timeoutCycles    = 5
	suspicionEnabled = false
	suspicionMap     = make(map[string]int) // maps suspected nodes to when they were suspected
	failedMap        = make(map[string]int) // maps failed nodes to when they were failed, so we can remove them from the list after a certain number of cycles
	suspicionMutex   sync.Mutex             // concurrent access to suspicionEnabled boolean, just in case since it's global
	messageDropRate  = 0                    // Message drop rate (percentage from 0 to 100)
	dropRateMutex    sync.Mutex             // Mutex to protect access to messageDropRate
)

var (
	localIdMutex     sync.Mutex
	localId          = ""
)

// helper function to update membership list (safely)
// pass incarnation -1 when we detect a server down by ourselves
func updateMemberStatus(id string, status string, incarnation int) {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()
	
	if id == getLocalId() && status != "ALIVE" {
		membershipList[id] = Member{
			Status:      "ALIVE",
			Incarnation: membershipList[id].Incarnation + 1,
		}
		logger.Printf("UPDATE: (self) Incarnation number incremented to %d \n", membershipList[id].Incarnation)
		return
	}

	if member, exists := membershipList[id]; exists {
		if incarnation == -1 {
			incarnation = member.Incarnation
		}
		if status == "LEAVE" {
			membershipList[id] = Member{
				Status:      status,
				Incarnation: incarnation,
			}
			logger.Printf("LEAVE: %s has left the group.\n", id)
			return
		}
		if member.Status == "LEAVE" {
			return
		}
		if incarnation > member.Incarnation {
			membershipList[id] = Member{
				Status:      status,
				Incarnation: incarnation,
			}
			logger.Printf("UPDATE: Successfully updated %s to %s\n", id, status)
		} else if incarnation == member.Incarnation {
			if (status == "SUSPECTED" && member.Status == "ALIVE") || (status == "FAILED" && (member.Status == "ALIVE" || member.Status == "SUSPECTED")) {
				membershipList[id] = Member{
					Status:      status,
					Incarnation: incarnation,
				}
				suspicionMutex.Lock()
				if status == "SUSPECTED" {
					suspicionMap[id] = cycle
				} else if status == "FAILED" {
					failedMap[id] = cycle
				}
				suspicionMutex.Unlock()
				logger.Printf("UPDATE: Successfully updated %s to %s\n", id, status)
			}
		} else {
			logger.Printf("Stale update for %s with older incarnation %d\n", id, incarnation)
		}
	} else if status == "ALIVE" {
		membershipList[id] = Member{
			Status:      status,
			Incarnation: incarnation,
		}
		logger.Printf("JOIN: Added new member, %s, with incarnation %d\n", id, incarnation)
	}
	// logger.Printf("membership list after updating: %v\n", membershipList)
}

func getAddressFromId(id string) string {
	return strings.Split(id, "`")[0]
}


/*
handle leave will
1. update node's status to LEAVE
2. broadcast the LEAVE status to all nodes
3. remove node from membership list after some time
*/
func handleLeave() {
	membershipMutex.Lock()
	if member, exists := membershipList[getLocalId()]; exists {
		member.Status = "LEAVE"
		membershipList[getLocalId()] = member
	}
	membershipMutex.Unlock()

	for id := range membershipList {
		if id != getLocalId() {
			address := getAddressFromId(id)
			conn, err := net.Dial("udp", address)
			if err != nil {
				logger.Printf("Error dialing %s: %v\n", address, err)
				continue
			}
			defer conn.Close()

			// send leave message
			_, err = conn.Write([]byte(addPiggybackToMessage("LEAVE")))
			if err != nil {
				logger.Printf("Error sending LEAVE to %s: %v\n", address, err)
			}
		}
	}

	time.Sleep(time.Duration(timeoutCycles) * time.Second)
	logger.Printf("LEAVE: Node %s has left the group. \n", getLocalId())
	os.Exit(0) // exit the program
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
		// logger.Printf("Received PING from %s, message = %s\n", remoteAddr.String(), message)

		// check if message is direct or indirect ping
		if message == "PING" {
			// direct message, send ACK back to sender
			if shouldDropMessage() {
				logger.Printf("Dropping message to %s (PING)\n", remoteAddr.String())
				continue
			}
			_, err = conn.WriteToUDP([]byte(addPiggybackToMessage("ACK")), remoteAddr)
			if err != nil {
				logger.Printf("Error sending ACK to %s: %v\n", remoteAddr.String(), err)
			} else {
				// logger.Printf("Sending ACK to %s...\n", remoteAddr.String())
			}
		} else if len(message) > 5 && message[:5] == "PING " {
			// indirect PING, get target address and handle with indirectPingHandler
			targetAddress := message[5:]
			go indirectPingHandler(targetAddress, remoteAddr.String()) // initiate go routine
		}

		if message == "JOIN" {
			// piggyback membership list to sender
			if shouldDropMessage() {
				logger.Printf("Dropping message to %s (JOIN)\n", remoteAddr.String())
				continue
			}
			_, err = conn.WriteToUDP([]byte(addPiggybackToMessage("INFO")), remoteAddr)
			if err != nil {
				logger.Printf("Error sending INFO to %s: %v\n", remoteAddr.String(), err)
			} else {
				// logger.Printf("Sending INFO to %s...\n", remoteAddr.String())
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
		suspicionMutex.Lock()
		cycle += 1
		suspicionMutex.Unlock()
		checkForExpiredNodes()

		id := getRandomAliveNode() // address selected randomly from membershiplist, besides self
		if id == "" {
			// logger.Printf("No alive nodes.\n")
			// printMembershipList()
			time.Sleep(1 * time.Second)
			continue
		}

		res := pingSingleAddress(id)
		if res != 0 {
			logger.Printf("No ACK from %s. Attempting indirect ping.\n", id)
			nodesToPing := randomKAliveNodes(2)
			var indirectWg sync.WaitGroup
			indirectACK := false
			var ackMutex sync.Mutex // protect indirect ACK bool
			for _, nodeAddress := range nodesToPing {
				indirectWg.Add(1)
				go func(node string) {
					address := getAddressFromId(node)
					defer indirectWg.Done()
					conn, err := net.Dial("udp", address)
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
				logger.Printf("Received indirect ACK for %s\n", id)
				updateMemberStatus(id, "ALIVE", -1)
			} else {
				suspicionMutex.Lock()
				if suspicionEnabled {
					suspicionMutex.Unlock()
					logger.Printf("SUSPICION DETECTED: No indirect ACK for %s. Marking node as SUSPECTED.\n", id)
					updateMemberStatus(id, "SUSPECTED", -1)
				} else {
					suspicionMutex.Unlock()
					logger.Printf("FAILURE DETECTED: No indirect ACK for %s. Marking node as FAILED.\n", id)
					updateMemberStatus(id, "FAILED", -1)
				}
			}
			ackMutex.Unlock()
		} else {
			// logger.Printf("Received ACK from %s\n", address)
			updateMemberStatus(id, "ALIVE", -1)
		}

		// Sleep for 1 second before the next ping
		// printMembershipList()
		time.Sleep(1 * time.Second)
	}
}

func checkForExpiredNodes() {
	suspicionMutex.Lock()

	for node, markedCycle := range suspicionMap {
		// logger.Printf("markedCycle: %d, timeoutCycles: %d, currentCycle: %d\n", markedCycle, timeoutCycles, cycle)
		if markedCycle+timeoutCycles <= cycle {
			suspicionMutex.Unlock()
			logger.Printf("Grace period for %s expired, marking node as FAILED\n", node)
			updateMemberStatus(node, "FAILED", -1)
			suspicionMutex.Lock()
			delete(suspicionMap, node)
		}
	}

	for node, markedCycle := range failedMap {
		if markedCycle+timeoutCycles <= cycle {
			membershipMutex.Lock()
			delete(membershipList, node)
			membershipMutex.Unlock()
			logger.Printf("Grace period for %s expired, evicting node from membership list\n", node)
			delete(failedMap, node)
		}
	}
	suspicionMutex.Unlock()
}

// function to ping a given address, and then return 0 if recieved ACK, 1 if no ACK
func pingSingleAddress(id string) int {
	// Attempt to send a ping
	// logger.Printf("Sending PING to %s...\n", address)
	address := getAddressFromId(id)

	// Create a UDP connection
	conn, err := net.Dial("udp", address)
	if err != nil {
		logger.Printf("Error dialing %s: %v\n", address, err)
		return 1
	}
	defer conn.Close()

	if shouldDropMessage() {
		logger.Printf("Dropping PING to %s\n", address)
		return 1
	}

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

	if shouldDropMessage() {
		logger.Printf("Dropping JOIN to %s\n", introducerAddress)
		return 1
	}

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

func printSuspectedNodes() {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	logger.Printf("Suspected Nodes:\n")
	count := 0
	for address, member := range membershipList {
		if member.Status == "SUSPECTED" {
			count += 1
			logger.Printf("%s, %s, %d\n", address, member.Status, member.Incarnation)
		}
	}
	if count == 0 {
		logger.Printf("None\n")
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

func printSelf() {
	logger.Printf("self: %s\n", getLocalId())
}

func getRandomAliveNode() string {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	aliveNodes := []string{}
	for id, member := range membershipList {
		if member.Status == "ALIVE" && id != getLocalId() {
			aliveNodes = append(aliveNodes, id)
		}
	}
	if len(aliveNodes) == 0 {
		return ""
	}

	randomSource := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(randomSource)

	return aliveNodes[rng.Intn(len(aliveNodes))]
}

func randomKAliveNodes(n int) []string {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	aliveNodes := []string{}
	for id, member := range membershipList {
		if member.Status == "ALIVE" && id != getLocalId() {
			aliveNodes = append(aliveNodes, id)
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

// Function to determine if a message should be dropped based on the drop rate
func shouldDropMessage() bool {
	dropRateMutex.Lock()
	defer dropRateMutex.Unlock()

	if messageDropRate == 0 {
		return false
	}
	return rand.Intn(100) < messageDropRate
}

// Function to process incoming messages and extract piggyback information
func processPiggyback(message string) {
	// Assume message is formatted like "PING|id|member1|status1|incarnation1|member2|status2|incarnation2|..."
	parts := strings.Split(message, "|")
	if len(parts) < 4 {
		fmt.Println("Invalid message format for piggyback.")
		return
	}

	// Iterate over parts and update membership list
	for i := 1; i < len(parts); i += 3 {
		id := parts[i]
		status := parts[i+1]
		incarnationStr := parts[i+2]
		// logger.Printf("Member list info: %s, %s, %s\n", address, status, incarnationStr)

		// Parse incarnation number
		incarnation, err := strconv.Atoi(incarnationStr)
		if err != nil {
			logger.Printf("Error parsing incarnation for %s: %v\n", id, err)
			continue
		}

		// Update membership status
		updateMemberStatus(id, status, incarnation)

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


func handleCLICommands(wg *sync.WaitGroup) {
	defer wg.Done()

	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// Process commands here
		switch input {
		case "list_mem":
			printMembershipList()
		case "list_self":
			printSelf()
		case "leave":
			handleLeave()
		case "enable_sus":
			suspicionMutex.Lock()
			suspicionEnabled = true
			logger.Printf("Suspicion Enabled\n")
			suspicionMutex.Unlock()
		case "disable_sus":
			suspicionMutex.Lock()
			suspicionEnabled = false
			logger.Printf("Suspicion Disabled\n")
			suspicionMutex.Unlock()
		case "status_sus":
			suspicionMutex.Lock()
			if suspicionEnabled {
				logger.Printf("Suspicion is enabled\n")
			} else {
				logger.Printf("Suspicion is disabled\n")
			}
			suspicionMutex.Unlock()
		case "???": // (Mandatory, not optional) Display a suspected node immediately to stdout at the VM terminal of the suspecting node.
			//
		case "list_suspected_nodes":
			printSuspectedNodes()
		case "drop_rate":
			dropRateMutex.Lock()
			logger.Printf("Current message drop rate: %d%%\n", messageDropRate)
			dropRateMutex.Unlock()
		case "set_drop_rate":
			fmt.Println("Enter new drop rate (0-100):")
			rateInput, _ := reader.ReadString('\n')
			rateInput = strings.TrimSpace(rateInput)
			newRate, err := strconv.Atoi(rateInput)
			if err != nil || newRate < 0 || newRate > 100 {
				fmt.Println("Invalid drop rate. Please enter a value between 0 and 100.")
			} else {
				dropRateMutex.Lock()
				messageDropRate = newRate
				dropRateMutex.Unlock()
				logger.Printf("Message drop rate set to: %d%%\n", newRate)
			}
		default:
			fmt.Printf("Unknown command: %s\n", input)
		}
	}
}

func getLocalId() string {
	localIdMutex.Lock()
	defer localIdMutex.Unlock()

	return localId
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


	const versionFile = "version.txt"
	versionNumber := 0
	data, _ := ioutil.ReadFile(versionFile)
	if data != nil {
		versionNumber, _ = strconv.Atoi(string(data))
		versionNumber++ // Increment the version number
	}
	ioutil.WriteFile(versionFile, []byte(strconv.Itoa(versionNumber)), 0644)

	// No need to know whether it is introducer, because only the introducer will get
	// new node join requests
	var isIntroducer = false
	if localAddress == introducerAddress {
		isIntroducer = true
	}

	addr, _ := net.ResolveUDPAddr("udp", localAddress)

	localIdMutex.Lock()
	localId = localAddress + "`" + strconv.Itoa(versionNumber)
	localIdMutex.Unlock()
	
	updateMemberStatus(getLocalId(), "ALIVE", 0) // Add self to membership list

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

	wg.Add(1)
	go handleCLICommands(&wg)

	wg.Wait()
	select {}
}
