/*
HyDFS Server:
Manages file storage, replication, and handles client requests.
Each server instance maintains a portion of the consistent hash ring and is responsible for specific file replicas.
*/
/*
To-do (this is everything we need to get 100% on demo):
- integrate in mp2 for failure detection or add any sort of failure detection (super simple, we only need to detect failures, no rejoins)
- (30%) create
-        add logic for forwarding to primary server DONE!
-               either tell the client to send the create to the primary server 
-               or download the file onto the server, then send it to the primary server
-        add logic for replicating DONE!
-            have primary server act as a client, and send create requests to the replica addresses
- (20%) replication after failure + failure detection DONE!
-          have servers check if they need to replicate files to other replicas periodically 
-                 (send replicate req to server, if other server alr has the file, then don't do anything. if not, send file)
- (24%) client append ordering (from same client to same file) DONE!
-        super super easy DONE!
- (26%) client concurrent append - append to same file 2/4 clients concurrenty. merge. then, show that 2 files on separate replicas are identical
-        merge all changes to primary
-        then overwrite replicas from primary
*/
package main

import (
	"io/ioutil"
	"math/rand"
	"strconv"
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
	REPLI_INTERVAL     = 10 * time.Second // Replication period interval
	HEARTBEAT_INTERVAL = 2 * time.Second // Freq of senfding heartbeat msgs to other servers
	FAILURE_TIMEOUT    = 5 * time.Second // Duration after server is considered failed after no heartbeat
)

// Operation Types (what clients can req)
type Operation string

const (
	CREATE       Operation = "create"
	REPLICATE    Operation = "replicate"
	GET          Operation = "get"
	APPEND       Operation = "append"
	MERGE        Operation = "merge"
	LS           Operation = "ls"
	STORE        Operation = "store"
	GETFROM      Operation = "getfromreplica"
	LIST_MEM_IDS Operation = "list_mem_ids"
	SERVER_MERGE Operation = "server_merge"
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

var globalServer *Server

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
	s.hashRing = []uint32{}
	for _, server := range s.allServers { // each server hashed using SHA1, first 4 bytes converted into uint32 to serve as position on hash ring
		hash := hashKey(server)
		s.hashRing = append(s.hashRing, hash)
		s.serverMap[hash] = server // map hash to server address for quick retrieval
	}
	// sort hash ring for efficient lookup for file replica assignment
	sort.Slice(s.hashRing, func(i, j int) bool { return s.hashRing[i] < s.hashRing[j] })
	s.logger.Println("Hash ring (re)initialized with servers:", s.allServers)
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
	s.membershipList = map[string]MemberInfo{}
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

// forwardRequest another server and waits for the response
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

// Mutex locking issue, cant handle multiple requests at once...
// HandleCreate processes create requests
func (s *Server) HandleCreate(req Request, conn net.Conn, reader *bufio.Reader, replicate bool) Response {
	// s.mutex.Lock()
	// defer s.mutex.Unlock()

	replicas := s.getReplicas(req.HyDFSFile) // which servers should store file
	// Check if the file already exists on the primary replica (first server is primary replica)
	if !replicate {
		primary := replicas[0]
		if primary != s.address {
			// if cur server is not primary, forward to primary
			resp := Response{
				Status:  "redirect",
				Message: primary,
			}
			respData, _ := json.Marshal(resp)
			
			// Send JSON response + delimiter
			conn.Write(respData)
			conn.Write([]byte("\n\n")) // Custom delimiter
			fmt.Printf("Not primary address, forwarding to %s\n", primary)
			return s.forwardRequest(primary, req)
		}
	}

	// Primary replica handles the creation
	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)

	// Check if HyDFS file already exists (prevent dups) THIS HANGS FOR NOW, IT SHOULD SEND IT DIRECTLY AND RETURN
	if _, err := os.Stat(hydfsPath); err == nil {
		resp := Response{
			Status:  "exists",
			Message: "File already exists in HyDFS.",
		}
		respData, _ := json.Marshal(resp)
	
		// Send JSON response + delimiter
		conn.Write(respData)
		conn.Write([]byte("\n\n")) // Custom delimiter
		return Response{
			Status:  "exists",
			Message: "File already exists in HyDFS.",
		}
	}

	resp := Response{
		Status:  "success",
		Message: "File created successfully.",
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
	if !replicate {
		for _, replica := range replicas[1:] {
			go s.replicateFile(replica, req.HyDFSFile)
		}
	}

	// Update file map to maintain mapping of hydfs files to respective replicas
	s.files[req.HyDFSFile] = replicas
	return Response{
		Status:  "error",
		Message: "HyDFS file already exists.",
	}
}

// HandleServerMerge processes server merge requests
func (s *Server) HandleServerMerge(req Request, conn net.Conn, reader *bufio.Reader) Response {
	// s.mutex.Lock()
	// defer s.mutex.Unlock()
	
	hydfsPath := filepath.Join(FILES_DIR, req.HyDFSFile)

	resp := Response{
		Status:  "success",
		Message: "File merged successfully.",
	}
	respData, _ := json.Marshal(resp)

	// Send JSON response + delimiter
	conn.Write(respData)
	conn.Write([]byte("\n\n")) // Custom delimiter

	// Read file data from client
	buffer := make([]byte, 4096)
	file, _ := os.OpenFile(hydfsPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
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

	s.logger.Printf("Merged HyDFS file %s", req.HyDFSFile, req.LocalFile)

	// Update file map to maintain mapping of hydfs files to respective replicas
	// s.files[req.HyDFSFile] = replicas
	return Response{
		Status:  "error",
		Message: "HyDFS file already exists.",
	}
}

// replicateFile sends create req to replica server with file content to replicate file, basically the same as a client would send a create
func (s *Server) replicateFile(replica string, hydfsFile string) {
	req := Request{
		Operation: REPLICATE,
		HyDFSFile: hydfsFile,
	}
	// fmt.Printf("sending replicate req to %s\n", replica)
	resp, _, conn := SendRequest(req, replica)
	if resp.Status == "exists" {
		// fmt.Printf("aborted replication, file exists\n")
		return
	} else if resp.Status == "error" {
		// fmt.Printf("aborted replication, unable to connect to server\n")
		return
	}
	localFile := filepath.Join(FILES_DIR, hydfsFile)
	content, _ := os.ReadFile(localFile)
	conn.Write(content)
}

// mergeFile sends create req to replica server with file content to merge file
func (s *Server) mergeFile(replica string, hydfsFile string) {
	req := Request{
		Operation: SERVER_MERGE,
		HyDFSFile: hydfsFile,
	}
	_, _, conn := SendRequest(req, replica)
	localFile := filepath.Join(FILES_DIR, hydfsFile)
	content, _ := os.ReadFile(localFile)
	conn.Write(content)
}

func (s *Server) periodicReplicator() {
	ticker := time.NewTicker(REPLI_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.logger.Println("Starting periodic replication cycle...")

			// List all files in the ./.files/ directory
			files, _ := os.ReadDir("./.files/")

			// Iterate over each file
			for _, file := range files {
				hydfsFile := file.Name()

				// Determine the replicas for the file
				replicas := s.getReplicas(hydfsFile)

				// Send replication requests to each replica
				for _, replica := range replicas {
					// Skip replicating to self
					if replica == s.address {
						continue
					}

					go s.replicateFile(replica, hydfsFile)
				}
			}

			s.logger.Println("Completed periodic replication.")
		}
	}
}

// Helper function to check if an address is in the replicas list
func containsAddress(replicas []string, address string) bool {
	for _, replica := range replicas {
		if replica == address {
			return true
		}
	}
	return false
}

// Send JSON response, followed by a delimiter, then the file content
func (s *Server) HandleGet(req Request, conn net.Conn) Response {
	replicas := s.getReplicas(req.HyDFSFile)
	// forward req if this server isn't the primary
	// if !containsAddress(replicas, s.address) {
	if s.address != replicas[0] {
		primary := replicas[0]
		resp := Response{
			Status:  "redirect",
			Message: primary,
		}
		respData, _ := json.Marshal(resp)
		
		// Send JSON response + delimiter
		conn.Write(respData)
		conn.Write([]byte("\n\n")) // Custom delimiter
		fmt.Printf("Not primary address, forwarding to %s\n", primary)
		return s.forwardRequest(primary, req)
	}

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
	return resp
}

// HandleAppend processes append requests
func (s *Server) HandleAppend(req Request, conn net.Conn, reader *bufio.Reader) Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	replicas := s.getReplicas(req.HyDFSFile)
	// forward req if this server isn't the primary
	primary := replicas[0]
	if s.address != primary {
		resp := Response{
			Status:  "redirect",
			Message: primary,
		}
		respData, _ := json.Marshal(resp)
		
		// Send JSON response + delimiter
		conn.Write(respData)
		conn.Write([]byte("\n\n")) // Custom delimiter
		fmt.Printf("Not primary address, forwarding to %s\n", primary)
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
		Message: "File appended successfully.",
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

	// Use getReplicas to get the list of responsible servers for the file
	replicas := s.getReplicas(req.HyDFSFile)

	// Check if there are any replicas; return an error if none
	if len(replicas) == 0 {
		return Response{
			Status:  "error",
			Message: "HyDFS file does not exist.",
		}
	}

	// Build the serversInfo slice with server addresses and their RingIDs
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
// Send JSON response, followed by a delimiter, then the file content
func (s *Server) HandleGetFromReplica(req Request, conn net.Conn) Response {
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
func (s *Server) HandleMerge(req Request, conn net.Conn) Response {
	// Implementation of merge to ensure all replicas are identical (tbd check back on)
	// assumes no concurrent operations during merge...
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	replicas := s.getReplicas(req.HyDFSFile)
	if s.address != replicas[0] {
		primary := replicas[0]
		resp := Response{
			Status:  "redirect",
			Message: primary,
		}
		respData, _ := json.Marshal(resp)
		
		// Send JSON response + delimiter
		conn.Write(respData)
		conn.Write([]byte("\n\n")) // Custom delimiter
		fmt.Printf("Not primary address, forwarding to %s\n", primary)
		return s.forwardRequest(primary, req)
	}



	// send new version of file to replicas, lock to stop periodic replicating temporarily
	for _, replica := range replicas[1:] {
		go s.mergeFile(replica, req.HyDFSFile)
	}
	
	//wait for these

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
		fmt.Printf("[%s] Begin handling create\n", time.Now().Format("2006-01-02 15:04:05"))
		resp = s.HandleCreate(req, conn, reader, false)
		fmt.Printf("[%s] Create finished\n", time.Now().Format("2006-01-02 15:04:05"))
		return
	case REPLICATE:
		resp = s.HandleCreate(req, conn, reader, true)
		return
	case GET:
		fmt.Printf("[%s] Begin handling get\n", time.Now().Format("2006-01-02 15:04:05"))
		resp = s.HandleGet(req, conn)
		conn.Close()
		fmt.Printf("[%s] Get finished\n", time.Now().Format("2006-01-02 15:04:05"))
		return
	case GETFROM:
		resp = s.HandleGetFromReplica(req, conn)
		conn.Close()
		return
	case APPEND:
		fmt.Printf("[%s] Begin handling append\n", time.Now().Format("2006-01-02 15:04:05"))
		resp = s.HandleAppend(req, conn, reader)
		fmt.Printf("[%s] Append finished\n", time.Now().Format("2006-01-02 15:04:05"))
		return
	case MERGE:
		resp = s.HandleMerge(req, conn)
	case SERVER_MERGE:
		fmt.Printf("server merge initiated\n")
		resp = s.HandleServerMerge(req, conn, reader)
		return
	case LS:
		resp = s.HandleLS(req)
	case STORE:
		resp = s.HandleStore(req)
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
		// fmt.Printf("extra bytes: %d\n", reader.Buffered())

		fmt.Printf("read from client: %s\n", jsonData)
		s.processRequest(jsonData, conn, reader)
	// }
}


// SendRequest reads JSON response until delimiter
func SendRequest(req Request, addr string) (Response, *bufio.Reader, *net.TCPConn) {
	serverAddr, _ := net.ResolveTCPAddr("tcp", addr)
	conn, err := net.DialTCP("tcp", nil, serverAddr)
	if err != nil {
		log.Println("Failed to dial server at %s: %v", serverAddr, err)
		return Response{
			Status:  "error",
			Message: "Failed to connect to server.",
		}, nil, nil
	}

	data, err := json.Marshal(req)
	if err != nil {
		return Response{Status: "error", Message: "Failed to marshal request."}, nil, nil
	}
	conn.Write(data)
	conn.Write([]byte("\n\n")) // Custom delimiter

	// Read JSON response until the delimiter
	reader := bufio.NewReader(conn)
	jsonData := make([]byte, 0, 4096) // Buffer to store the JSON response
	for {
		// Read a single byte at a time to avoid buffering extra data
		b, err := reader.ReadByte()
		if err != nil {
			log.Printf("Failed to read byte: %v", err)
			return Response{Status: "error", Message: "Failed to read JSON response."}, nil, nil
		}

		// Append byte to jsonData
		jsonData = append(jsonData, b)

		// Check if we've reached the delimiter (e.g., "\n\n")
		if len(jsonData) >= 2 && string(jsonData[len(jsonData)-2:]) == "\n\n" {
			break
		}
	}

	var resp Response
	err = json.Unmarshal(jsonData, &resp)
	if err != nil {
		return Response{Status: "error", Message: "Failed to unmarshal JSON."}, nil, nil
	}

	return resp, reader, conn
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

// Start begins the server to listen for incoming requests and handle heartbeats multiappend a fa24-cs425-0702.cs.illinois.edu:8080,fa24-cs425-0701.cs.illinois.edu:8080 a,d
func (s *Server) Start() {
	// addr, err := net.ResolveUDPAddr("udp", s.address)
	// if err != nil {
	// 	s.logger.Fatalf("Failed to resolve UDP address %s: %v", s.address, err)
	// }

	// conn, err := net.ListenUDP("udp", addr)
	// if err != nil {
	// 	s.logger.Fatalf("Failed to listen on UDP port %s: %v", s.address, err)
	// }
	// defer conn.Close()

	// s.logger.Printf("Server started and listening on %s", s.address)

	// Start handling incoming requests from client
	go s.AcceptIncomingConnections(s.address)

	go s.periodicReplicator()
	// go s.HandleIncomingRequests(conn)

	// Start heartbeat mechanism (might replace with mp2, no heartbeating failure detection here)

	// Monitor membership for failures and in case rejoin

	// Prevent the main goroutine from exiting
	select {}
}

// main function to start the server
func main() {
	// if len(os.Args) < 3 {
	// 	fmt.Println("Usage: go run server.go <server_address> <all_server_addresses_comma_separated>")
	// 	fmt.Println("Example: go run server.go localhost:23120 localhost:23120,localhost:23121,localhost:23122")
	// 	return
	// }
	if len(os.Args) != 1 {
		fmt.Println("Usage: go run server.go")
		return
	}

	const localAddressFile = "./../../mp2/localaddr.txt"
	data, _ := ioutil.ReadFile(localAddressFile)
	localAddress := string(data)
	serverAddress := localAddress

	go swimMain()
	globalServer = NewServer(serverAddress, []string{})
	globalServer.Start()
}





















// failure detector logic


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
	suspicionEnabled = true
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

var (
	bandwidthMutex     sync.Mutex
	bandwidth          = 0
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
		addr := getAddressFromId(id)
		globalServer.allServers = append(globalServer.allServers, addr)
		globalServer.initializeHashRing() //add mutex locks for whenever we access the hashring
		globalServer.initializeMembership()
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
			updateBandwidth(len(addPiggybackToMessage("LEAVE")))
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
				updateBandwidth(len(addPiggybackToMessage("ACK")))
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
				updateBandwidth(len(addPiggybackToMessage("INFO")))
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
	updateBandwidth(len(addPiggybackToMessage("PING")))

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
			updateBandwidth(len(addPiggybackToMessage("INDIRECT_ACK")))
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
					updateBandwidth(len(addPiggybackToMessage(request)))
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

func removeAddress(allServers []string, addr string) []string {
	newServers := []string{}
	for _, serverAddr := range allServers {
		if serverAddr != addr {
			newServers = append(newServers, serverAddr)
		}
	}
	return newServers
}

func checkForExpiredNodes() {
	suspicionMutex.Lock()

	for node, markedCycle := range suspicionMap {
		// logger.Printf("markedCycle: %d, timeoutCycles: %d, currentCycle: %d\n", markedCycle, timeoutCycles, cycle)
		if markedCycle+timeoutCycles <= cycle {
			suspicionMutex.Unlock()
			logger.Printf("Grace period for %s expired, marking node as FAILED\n", node)
			// UPDATE THE HASHRING!!!!!
			addr := getAddressFromId(node)
			// remove addr from server.allServers[]
			globalServer.allServers = removeAddress(globalServer.allServers, addr)
			globalServer.initializeHashRing() //add mutex locks for whenever we access the hashring
			globalServer.initializeMembership()
			updateMemberStatus(node, "FAILED", -1)
			suspicionMutex.Lock()
			delete(suspicionMap, node)
		}
	}

	for node, markedCycle := range failedMap {
		if markedCycle+timeoutCycles <= cycle {
		// if markedCycle <= cycle {
			membershipMutex.Lock()
			delete(membershipList, node)
			membershipMutex.Unlock()
			// logger.Printf("Grace period for %s expired, evicting node from membership list\n", node)
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
	updateBandwidth(len(addPiggybackToMessage("PING")))

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
	updateBandwidth(len(addPiggybackToMessage("JOIN")))

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

func getBandwidth() int {
	bandwidthMutex.Lock()
	defer bandwidthMutex.Unlock()

	return bandwidth
}

func updateBandwidth(val int) {
	bandwidthMutex.Lock()
	defer bandwidthMutex.Unlock()

	bandwidth += val
}

func swimMain() {
	
	var wg sync.WaitGroup

	const localAddressFile = "./../../mp2/localaddr.txt"
	data, _ := ioutil.ReadFile(localAddressFile)
	localAddress := string(data)
	logFile := "./../../mp1/swim.log"
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	multiWriter := io.MultiWriter(os.Stdout, file)
	logger = log.New(multiWriter, "", log.Ldate|log.Ltime)


	const versionFile = "./../../mp2/version.txt"
	versionNumber := 0
	data, _ = ioutil.ReadFile(versionFile)
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
