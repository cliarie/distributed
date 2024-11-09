/*
HyDFS Server:
Manages file storage, replication, and handles client requests.
Each server instance maintains a portion of the consistent hash ring and is responsible for specific file replicas.
*/
/*
To-do (this is everything we need to get 100% on demo):
- integrate in mp2 for failure detection or add any sort of failure detection (super simple, we only need to detect failures, no rejoins)
- (30%) create
-        add logic for forwarding to primary server
-               either tell the client to send the create to the primary server
-               or download the file onto the server, then send it to the primary server
-        add logic for replicating
-            have primary server act as a client, and send create requests to the replica addresses
- (20%) replication after failure
-        ??? have servers periodically check if they need to replicate any files from the prev 2 on the ring
-            if so, just issue create requests to the prev server
- (24%) client append ordering (from same client to same file)
-        super super easy
- (26%) client concurrent append - append to same file 2/ 4 clients concurrenty. merge. then, show that 2 files on separate replicas are identical
-        merge all changes to primary?
-        then overwrite replicas from primary?
*/
package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Constants
const (
	SERVER_PORT        = 23120           // Not directly used, default port number
	BUFFER_SIZE        = 65535           // Size of udp buffer for reading incoming msgs
	FILES_DIR          = ".files"        // Where hydfs files are stored
	LOG_FILE           = "server.log"    // Where server logs are written
	REPLICATION_FACTOR = 3               // Num replicas each file should have
	HEARTBEAT_INTERVAL = 2 * time.Second // Freq of sending heartbeat msgs to other servers
	FAILURE_TIMEOUT    = 5 * time.Second // Duration after server is considered failed after no heartbeat
)

// Operation Types (what clients can req)
type Operation string

const (
	CREATE       Operation = "create"
	GET          Operation = "get"
	APPEND       Operation = "append"
	MERGE        Operation = "merge"
	LS           Operation = "ls"
	STORE        Operation = "store"
	GETFROM      Operation = "getfromreplica"
	LIST_MEM_IDS Operation = "list_mem_ids"
)

// Request defines the structure of client requests
type Request struct {
	Operation   Operation `json:"operation"`
	LocalFile   string    `json:"local_file,omitempty"`
	HyDFSFile   string    `json:"hydfs_file,omitempty"`
	Content     string    `json:"content,omitempty"`      // Used for append
	ReplicaAddr string    `json:"replica_addr,omitempty"` // Used for getfromreplica
}

// Response defines the structure of server responses to client
type Response struct {
	Status  string       `json:"status"`            // "success" or "error"
	Message string       `json:"message"`           // Detailed message or content
	Files   []string     `json:"files,omitempty"`   // Used for ls and store
	Servers []ServerInfo `json:"servers,omitempty"` // Used for ls and list_mem_ids
}

// ServerInfo holds server address and ring ID
type ServerInfo struct {
	Address string `json:"address"`
	RingID  uint32 `json:"ring_id"`
}

// MemberInfo: server metadata in the membership list
type MemberInfo struct {
	Address  string    `json:"address"`
	RingID   uint32    `json:"ring_id"`
	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"` // "active" or "failed"
}

// Server represents the HyDFS server
type Server struct {
	mutex          sync.Mutex
	logger         *log.Logger
	address        string
	allServers     []string
	hashRing       []uint32
	serverMap      map[uint32]string
	membershipList map[string]MemberInfo
	files          map[string][]string // HyDFSFile -> list of server addresses
	// For simplicity, files map HyDFSFile to list of replicas
}

// NewServer initializes the server
func NewServer(address string, allServers []string) *Server {
	// Ensure .files dir exists, if not create
	if err := os.MkdirAll(FILES_DIR, 0755); err != nil {
		log.Fatalf("Failed to create files directory: %v", err)
	}

	// opens/creates server.log for logging server activities, prefixed with HYDFS Server: {timestamp, file info}
	logFile, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	logger := log.New(logFile, "HYDFS Server: ", log.Ldate|log.Ltime|log.Lshortfile)

	// initializes server fields
	server := &Server{
		logger:         logger,
		address:        address,
		allServers:     allServers,
		serverMap:      make(map[uint32]string),
		membershipList: make(map[string]MemberInfo),
		files:          make(map[string][]string),
	}

	// set up consistent hashing and membershiplist
	server.initializeHashRing()
	server.initializeMembership()
	return server
}

// initializeHashRing sets up the consistent hashing ring
func (s *Server) initializeHashRing() {
	for _, server := range s.allServers { // each server hashed using SHA1, first 4 bytes converted into uint32 to serve as position on hash ring
		hash := hashKey(server)
		s.hashRing = append(s.hashRing, hash)
		s.serverMap[hash] = server // map hash to server address for quick retrieval
	}
	// sort hash ring for efficient lookup for file replica assignment
	sort.Slice(s.hashRing, func(i, j int) bool { return s.hashRing[i] < s.hashRing[j] })
	s.logger.Println("Hash ring initialized with servers:", s.allServers)
}

// hashKey generates a hash for a given key using SHA1 and returns first 4 bytes as uint32
func hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	bs := h.Sum(nil)
	return bytesToUint32(bs[:4])
}

// bytesToUint32 converts 4 bytes to uint32
func bytesToUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// initializeMembership initializes the membership list with all servers as active, to monitor server health and managing replicas
func (s *Server) initializeMembership() {
	for _, server := range s.allServers {
		s.membershipList[server] = MemberInfo{
			Address:  server,
			RingID:   hashKey(server),
			LastSeen: time.Now(),
			Status:   "active",
		}
	}
	s.logger.Println("Membership list initialized.")
}

// getReplicas returns the list of servers responsible for a given filename
func (s *Server) getReplicas(filename string) []string {
	hash := hashKey(filename)
	// Find the first server with hash >= file hash
	idx := sort.Search(len(s.hashRing), func(i int) bool { return s.hashRing[i] >= hash })
	if idx == len(s.hashRing) {
		idx = 0
	}
	replicas := []string{}
	for i := 0; i < REPLICATION_FACTOR; i++ {
		serverHash := s.hashRing[(idx+i)%len(s.hashRing)]
		replicas = append(replicas, s.serverMap[serverHash])
	}
	return replicas
}

// forwardRequest forwards a request to another server and waits for the response
func (s *Server) forwardRequest(target string, req Request) Response {
	conn, err := net.Dial("udp", target)
	if err != nil {
		s.logger.Printf("Error dialing target server %s: %v", target, err)
		return Response{
			Status:  "error",
			Message: "Failed to forward request to target server.",
		}
	}
	defer conn.Close()

	data, err := json.Marshal(req)
	if err != nil {
		s.logger.Printf("Error marshalling forwarded request: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to marshal forwarded request.",
		}
	}

	_, err = conn.Write(data)
	if err != nil {
		s.logger.Printf("Error sending forwarded request to %s: %v", target, err)
		return Response{
			Status:  "error",
			Message: "Failed to send forwarded request.",
		}
	}

	// Set a timeout for the response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Wait for the response
	buffer := make([]byte, BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		s.logger.Printf("Error reading forwarded response from %s: %v", target, err)
		return Response{
			Status:  "error",
			Message: "Failed to read forwarded response.",
		}
	}

	var resp Response
	err = json.Unmarshal(buffer[:n], &resp)
	if err != nil {
		s.logger.Printf("Error unmarshalling forwarded response: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to unmarshal forwarded response.",
		}
	}

	return resp
}

// HandleCreate processes create requests
func (s *Server) HandleCreate(req Request, conn net.Conn, reader *bufio.Reader) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	replicas := s.getReplicas(req.HyDFSFile) // which servers should store file

	// Check if the file already exists on the primary replica (first server is primary replica)
	primary := replicas[0]
	if primary != s.address {
		// if cur server is not primary, forward to primary
		return s.forwardRequest(primary, req)
	}

	// Primary replica handles the creation
	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)

	// Check if HyDFS file already exists (prevent dups) THIS HANGS FOR NOW, IT SHOULD SEND IT DIRECTLY AND RETURN
	if _, err := os.Stat(hydfsPath); err == nil {
		return Response{
			Status:  "error",
			Message: "HyDFS file already exists.",
		}
	}

	resp := Response{
		Status:  "success",
		Message: "File appended successfully.",
	}
	respData, _ := json.Marshal(resp)

	// Send JSON response + delimiter
	conn.Write(respData)
	conn.Write([]byte("\n\n")) // Custom delimiter

	// Read file data from client
	buffer := make([]byte, 4096)
	file, _ := os.Create(hydfsPath)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file data
			}
			fmt.Printf("Failed to read file data: %v\n", err)
			break
		}

		// Write data to local HyDFS file
		_, err = file.Write(buffer[:n])
		if err != nil {
			fmt.Printf("Failed to write data to local file: %v\n", err)
			break
		}
	}

	s.logger.Printf("Created HyDFS file %s from local file %s", req.HyDFSFile, req.LocalFile)

	// Replicate the file to other replicas (async with goroutines to avoid blocking)
	// NOTE: IF FILES R TOO LARGE, CANT STORE CONTENTS IN A STRING
	for _, replica := range replicas[1:] {
		go s.replicateFile(replica, req.HyDFSFile, []byte{})
	}

	// Update file map to maintain mapping of hydfs files to respective replicas
	s.files[req.HyDFSFile] = replicas
	return Response{
		Status:  "error",
		Message: "HyDFS file already exists.",
	}
}

// replicateFile sends create req to replica server with file content to replicate file
func (s *Server) replicateFile(replica string, hydfsFile string, content []byte) {
	conn, err := net.Dial("udp", replica)
	if err != nil {
		s.logger.Printf("Error dialing replica %s for replication: %v", replica, err)
		return
	}
	defer conn.Close()

	replicaReq := Request{
		Operation: CREATE,
		HyDFSFile: hydfsFile,
		Content:   string(content),
	}

	data, err := json.Marshal(replicaReq)
	if err != nil {
		s.logger.Printf("Error marshalling replication request: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		s.logger.Printf("Error sending replication request to %s: %v", replica, err)
		return
	}

	s.logger.Printf("Replicated HyDFS file %s to replica %s", hydfsFile, replica)
}

// Send JSON response, followed by a delimiter, then the file content
func (s *Server) HandleGet(req Request, conn net.Conn) Response {
	// Prepare the JSON response
	resp := Response{
		Status:  "success",
		Message: "File fetched successfully.",
	}
	respData, _ := json.Marshal(resp)

	// Send JSON response + delimiter
	conn.Write(respData)
	conn.Write([]byte("\n\n")) // Custom delimiter

	// Now send the file content
	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)
	content, err := os.ReadFile(hydfsPath)
	if err != nil {
		s.logger.Printf("Failed to read file: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to read file.",
		}
	}
	conn.Write(content) // Send file data
	fmt.Printf("sending file content: %s\n", content)
	return resp
}

// HandleAppend processes append requests
func (s *Server) HandleAppend(req Request, conn net.Conn, reader *bufio.Reader) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	replicas := s.getReplicas(req.HyDFSFile)

	// Check if the file exists on the primary replica, if not forward to primary
	primary := replicas[0]
	if primary != s.address {
		// Forward the append request to the primary replica
		return s.forwardRequest(primary, req)
	}

	// Primary replica handles the append
	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)
	
	// Check if HyDFS file exists
	if _, err := os.Stat(hydfsPath); os.IsNotExist(err) {
		return Response{
			Status:  "error",
			Message: "HyDFS file does not exist.",
		}
	}
	
	resp := Response{
		Status:  "success",
		Message: "File created successfully.",
	}
	respData, _ := json.Marshal(resp)
	conn.Write(respData)
	conn.Write([]byte("\n\n"))

	// Append content to HyDFS file
	file, _ := os.OpenFile(hydfsPath, os.O_APPEND|os.O_WRONLY, 0644)
	defer file.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file data
			}
			fmt.Printf("Failed to read file data: %v\n", err)
			break
		}

		// Write data to local HyDFS file
		_, err = file.Write(buffer[:n])
		if err != nil {
			fmt.Printf("Failed to write data to local file: %v\n", err)
			break
		}
	}
	

	s.logger.Printf("Appended to HyDFS file %s from local file %s", req.HyDFSFile, req.LocalFile)

	// Replicate the append to other replicas
	for _, replica := range replicas[1:] {
		go s.replicateAppend(replica, req.HyDFSFile, req.Content)
	}

	return Response{
		Status:  "success",
		Message: "File appended successfully.",
	}
}

// replicateAppend sends the append content to a replica
func (s *Server) replicateAppend(replica string, hydfsFile string, content string) {
	conn, err := net.Dial("udp", replica)
	if err != nil {
		s.logger.Printf("Error dialing replica %s for append replication: %v", replica, err)
		return
	}
	defer conn.Close()

	replicaReq := Request{
		Operation: APPEND,
		HyDFSFile: hydfsFile,
		Content:   content,
	}

	data, err := json.Marshal(replicaReq)
	if err != nil {
		s.logger.Printf("Error marshalling append replication request: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		s.logger.Printf("Error sending append replication request to %s: %v", replica, err)
		return
	}

	s.logger.Printf("Replicated append to HyDFS file %s on replica %s", hydfsFile, replica)
}

// HandleLS processes ls requests
func (s *Server) HandleLS(req Request) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	replicas, exists := s.files[req.HyDFSFile] // list all replicas of hydfs file
	if !exists {
		return Response{
			Status:  "error",
			Message: "HyDFS file does not exist.",
		}
	}

	serversInfo := []ServerInfo{}
	for _, replica := range replicas {
		ringID := hashKey(replica)
		serversInfo = append(serversInfo, ServerInfo{
			Address: replica,
			RingID:  ringID,
		})
	}

	return Response{
		Status:  "success",
		Message: "List of replicas.",
		Servers: serversInfo,
	}
}

// HandleStore processes store requests
func (s *Server) HandleStore(req Request) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// List all hydfs files stored on this server
	storedFiles := []string{}
	files, err := os.ReadDir(FILES_DIR) // reads .files directory to list all stored files
	if err != nil {
		s.logger.Printf("Error reading files directory: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to read files directory.",
		}
	}

	for _, file := range files {
		if !file.IsDir() {
			storedFiles = append(storedFiles, file.Name())
		}
	}

	// Get ring ID
	ringID := hashKey(s.address)

	serversInfo := []ServerInfo{
		{
			Address: s.address,
			RingID:  ringID,
		},
	}

	return Response{
		Status:  "success",
		Message: "Files stored on this server.",
		Files:   storedFiles,
		Servers: serversInfo,
	}
}

// HandleGetFromReplica processes getfromreplica requests, clients can fetch hydfs file from replica
func (s *Server) HandleGetFromReplica(req Request) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)

	// Check if HyDFS file exists
	content, err := os.ReadFile(hydfsPath) // read file from .files directory
	if err != nil {
		s.logger.Printf("Error reading HyDFS file %s: %v", req.HyDFSFile, err)
		return Response{
			Status:  "error",
			Message: "HyDFS file does not exist.",
		}
	}

	// Write to local file
	err = os.WriteFile(req.LocalFile, content, 0644)
	if err != nil {
		s.logger.Printf("Error writing to local file %s: %v", req.LocalFile, err)
		return Response{
			Status:  "error",
			Message: fmt.Sprintf("Failed to write to local file: %v", err),
		}
	}

	s.logger.Printf("Fetched HyDFS file %s from replica to local file %s", req.HyDFSFile, req.LocalFile)
	return Response{
		Status:  "success",
		Message: "File fetched from replica successfully.",
	}
}

// HandleListMemIDs processes list_mem_ids requests, list all servers in membership list with ring ids
func (s *Server) HandleListMemIDs(req Request) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	serversInfo := []ServerInfo{}
	for _, member := range s.membershipList {
		serversInfo = append(serversInfo, ServerInfo{
			Address: member.Address,
			RingID:  member.RingID,
		})
	}

	return Response{
		Status:  "success",
		Message: "Membership list with ring IDs.",
		Servers: serversInfo,
	}
}

// HandleMerge processes merge requests, ensure all replicas of a file are identical
func (s *Server) HandleMerge(req Request) Response {
	// Implementation of merge to ensure all replicas are identical (tbd check back on)
	// assumes no concurrent operations during merge...
	s.mutex.Lock()
	defer s.mutex.Unlock()

	replicas := s.getReplicas(req.HyDFSFile)

	// Read content from all replicas, concantenate in hash ring order
	contents := make(map[string]string)
	for _, replica := range replicas {
		if replica == s.address {
			hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)
			content, err := os.ReadFile(hydfsPath)
			if err != nil {
				s.logger.Printf("Error reading HyDFS file %s: %v", req.HyDFSFile, err)
				continue
			}
			contents[replica] = string(content)
		} else {
			// Forward a special merge request to the replica to get file content
			mergeReq := Request{
				Operation: MERGE,
				HyDFSFile: req.HyDFSFile,
			}
			resp := s.forwardRequest(replica, mergeReq)
			if resp.Status == "success" {
				contents[replica] = resp.Message
			}
		}
	}

	// Merge contents ensuring ring order
	mergedContent := ""
	for _, replica := range replicas {
		if content, exists := contents[replica]; exists {
			mergedContent += content
		}
	}

	// Write merged content back to all replicas
	for _, replica := range replicas {
		if replica == s.address {
			hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)
			err := os.WriteFile(hydfsPath, []byte(mergedContent), 0644)
			if err != nil {
				s.logger.Printf("Error writing merged content to HyDFS file %s: %v", req.HyDFSFile, err)
				continue
			}
		} else {
			// Send the merged content to the replica
			go s.sendMergedContent(replica, req.HyDFSFile, mergedContent)
		}
	}

	s.logger.Printf("Merged HyDFS file %s across all replicas.", req.HyDFSFile)
	return Response{
		Status:  "success",
		Message: "Merge completed successfully.",
	}
}

// sendMergedContent sends the merged content to a replica
func (s *Server) sendMergedContent(replica string, hydfsFile string, content string) {
	conn, err := net.Dial("udp", replica)
	if err != nil {
		s.logger.Printf("Error dialing replica %s for sending merged content: %v", replica, err)
		return
	}
	defer conn.Close()

	mergeReq := Request{
		Operation: MERGE,
		HyDFSFile: hydfsFile,
		Content:   content,
	}

	data, err := json.Marshal(mergeReq)
	if err != nil {
		s.logger.Printf("Error marshalling merge request: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		s.logger.Printf("Error sending merge request to %s: %v", replica, err)
		return
	}

	s.logger.Printf("Sent merged content to replica %s for HyDFS file %s", replica, hydfsFile)
}
// processRequest deserializes and routes the request
func (s *Server) processRequest(data []byte, conn net.Conn, reader *bufio.Reader) {
	var req Request
	err := json.Unmarshal(data, &req)
	if err != nil {
		s.logger.Printf("Invalid request format: %v", err)
		s.sendResponse(Response{
			Status:  "error",
			Message: "Invalid request format.",
		}, conn)
		return
	}

	var resp Response

	// Route the request to the appropriate handler based on the operation
	switch req.Operation {
	case CREATE:
		fmt.Printf("handling create\n")
		resp = s.HandleCreate(req, conn, reader)
		return
	case GET:
		resp = s.HandleGet(req, conn)
		conn.Close()
		return
	case APPEND:
		resp = s.HandleAppend(req, conn, reader)
		return
	case MERGE:
		resp = s.HandleMerge(req)
	case LS:
		resp = s.HandleLS(req)
	case STORE:
		resp = s.HandleStore(req)
	case GETFROM:
		resp = s.HandleGetFromReplica(req)
	case LIST_MEM_IDS:
		resp = s.HandleListMemIDs(req)
	default:
		resp = Response{
			Status:  "error",
			Message: "Unsupported operation.",
		}
	}

	// Send the response back to the client over the TCP connection
	s.sendResponse(resp, conn)
	conn.Close()
}
// AcceptIncomingConnections listens for new TCP connections from clients
func (s *Server) AcceptIncomingConnections(address string) {
	// Resolve the TCP address and start listening
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}
	defer listener.Close()
	s.logger.Printf("Server is listening on %s", address)

	for {
		// Accept a new client connection
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Printf("Error accepting connection: %v", err)
			continue
		}

		// Handle each client connection in a new goroutine
		go s.handleClient(conn)
	}
}

// HandleIncomingRequests processes all incoming requests
// func (s *Server) HandleIncomingRequests(conn *net.UDPConn) {
// 	buffer := make([]byte, BUFFER_SIZE)
// 	for {
// 		n, clientAddr, err := conn.ReadFromUDP(buffer)
// 		if err != nil {
// 			s.logger.Printf("Error reading from UDP: %v", err)
// 			continue
// 		}

// 		data := buffer[:n]
// 		go s.processRequest(data, clientAddr, conn)
// 	}
// }

// HandleClient processes requests from a single client
func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	// buffer := make([]byte, BUFFER_SIZE)
	// Read data from the client
	// for {
		reader := bufio.NewReader(conn)
		jsonData := make([]byte, 0, 4096) // Buffer to store the JSON response
		for {
			b, _ := reader.ReadByte()
			jsonData = append(jsonData, b)
			if len(jsonData) >= 2 && string(jsonData[len(jsonData)-2:]) == "\n\n" {
				break
			}
		}
		fmt.Printf("extra bytes: %d\n", reader.Buffered())

		fmt.Printf("read from client: %s\n", jsonData)
		s.processRequest(jsonData, conn, reader)
	// }
}

// sendResponse serializes and sends the response back to the client over TCP
func (s *Server) sendResponse(resp Response, conn net.Conn) {
	data, err := json.Marshal(resp)
	if err != nil {
		s.logger.Printf("Failed to marshal response: %v", err)
		return
	}

	_, err = conn.Write(data)
	fmt.Printf("response: %s\n", data)
	conn.Write([]byte("\n\n")) // Custom delimiter
	if err != nil {
		s.logger.Printf("Failed to send response: %v", err)
	}
}

// Start begins the server to listen for incoming requests and handle heartbeats
func (s *Server) Start() {
	addr, err := net.ResolveUDPAddr("udp", s.address)
	if err != nil {
		s.logger.Fatalf("Failed to resolve UDP address %s: %v", s.address, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		s.logger.Fatalf("Failed to listen on UDP port %s: %v", s.address, err)
	}
	defer conn.Close()

	s.logger.Printf("Server started and listening on %s", s.address)

	// Start handling incoming requests from client
	go s.AcceptIncomingConnections(s.address)
	// go s.HandleIncomingRequests(conn)

	// Start heartbeat mechanism (might replace with mp2, no heartbeating failure detection here)

	// Monitor membership for failures and in case rejoin

	// Prevent the main goroutine from exiting
	select {}
}

// main function to start the server
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run server.go <server_address> <all_server_addresses_comma_separated>")
		fmt.Println("Example: go run server.go localhost:23120 localhost:23120,localhost:23121,localhost:23122")
		return
	}

	serverAddress := os.Args[1]
	allServersArg := os.Args[2]
	allServers := strings.Split(allServersArg, ",")

	server := NewServer(serverAddress, allServers)
	server.Start()
}
