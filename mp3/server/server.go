/*
HyDFS Server:
Manages file storage, replication, and handles client requests.
Each server instance maintains a portion of the consistent hash ring and is responsible for specific file replicas.
*/
package main

import (
	"crypto/sha1"
	"log"
	"os"
	"sort"
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
