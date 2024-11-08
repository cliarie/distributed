/*
HyDFS Client: Provides an interface for users to perform file operations (create, get, append, merge)
Manages client-side caching to optimize read performance.
*/
/*
NOTE: LRU Cache, parsing shell commands and sending req to Server
*/
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Constants
const (
	SERVER_PORT    = 23120 // not used directly in code
	BUFFER_SIZE    = 65535 // size of udp buffer for incoming msgs
	CACHE_MAX_SIZE = 100   // Maximum number of files in cache
)

// Operation Types
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

// Response defines the structure of server responses
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

// CacheEntry represents a cached file with content and time cached
type CacheEntry struct {
	Content string
	Time    time.Time
}

// Client represents the HyDFS client's state with udp conn, server address, and caching
type Client struct {
	conn       *net.UDPConn
	serverAddr *net.UDPAddr
	cache      map[string]CacheEntry
	cacheMutex sync.Mutex
	cacheOrder []string // To implement LRU
}

// NewClient initializes the client
func NewClient(serverAddress string) *Client {
	addr, err := net.ResolveUDPAddr("udp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to resolve server address %s: %v", serverAddress, err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Failed to dial server at %s: %v", serverAddress, err)
	}

	client := &Client{
		conn:       conn,
		serverAddr: addr,
		cache:      make(map[string]CacheEntry), // set up cache map for lru
		cacheOrder: []string{},                  // set up order for lru
	}

	return client
}

// SendRequest sends a request to the server and waits for a response
func (c *Client) SendRequest(req Request) Response {
	data, err := json.Marshal(req) // seralize req, send to server, wait for resp, deserialize resp
	if err != nil {
		log.Printf("Failed to marshal request: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to marshal request.",
		}
	}

	// Send the request
	_, err = c.conn.Write(data)
	if err != nil {
		log.Printf("Failed to send request: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to send request.",
		}
	}

	// Set a timeout for the response
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Wait for the response
	buffer := make([]byte, BUFFER_SIZE)
	n, _, err := c.conn.ReadFromUDP(buffer)
	if err != nil {
		log.Printf("Failed to read response: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to read response.",
		}
	}

	var resp Response
	err = json.Unmarshal(buffer[:n], &resp)
	if err != nil {
		log.Printf("Failed to unmarshal response: %v", err)
		return Response{
			Status:  "error",
			Message: "Failed to unmarshal response.",
		}
	}

	return resp
}

// Close closes the UDP connection
func (c *Client) Close() {
	c.conn.Close()
}

// Cache management functions

// AddToCache adds a file to the cache
func (c *Client) AddToCache(file string, content string) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	// If file already in cache, update its position, moved to end of cache order slice slice to mark recently used
	for i, f := range c.cacheOrder {
		if f == file {
			c.cacheOrder = append(c.cacheOrder[:i], c.cacheOrder[i+1:]...)
			break
		}
	}

	// Add to cache, append file to cache order
	c.cache[file] = CacheEntry{
		Content: content,
		Time:    time.Now(),
	}
	c.cacheOrder = append(c.cacheOrder, file)

	// Evict if cache exceeds max size (lru evicted)
	if len(c.cacheOrder) > CACHE_MAX_SIZE {
		evict := c.cacheOrder[0]
		delete(c.cache, evict)
		c.cacheOrder = c.cacheOrder[1:]
	}
}

// GetFromCache retrieves a file from the cache
func (c *Client) GetFromCache(file string) (string, bool) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	entry, exists := c.cache[file]
	if !exists {
		return "", false
	}

	// Update LRU order
	for i, f := range c.cacheOrder {
		if f == file {
			c.cacheOrder = append(c.cacheOrder[:i], c.cacheOrder[i+1:]...)
			break
		}
	}
	c.cacheOrder = append(c.cacheOrder, file)

	return entry.Content, true
}

// InvalidateCache removes a file in the cache (when file modified or appended)
func (c *Client) InvalidateCache(file string) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	delete(c.cache, file)
	for i, f := range c.cacheOrder {
		if f == file {
			c.cacheOrder = append(c.cacheOrder[:i], c.cacheOrder[i+1:]...)
			break
		}
	}
}

// Helper function to display usage
func displayUsage() {
	fmt.Println("Available Commands:")
	fmt.Println("  create <localfilename> <HyDFSfilename>       - Create a new HyDFS file")
	fmt.Println("  get <HyDFSfilename> <localfilename>          - Get a HyDFS file")
	fmt.Println("  append <localfilename> <HyDFSfilename>      - Append to a HyDFS file")
	fmt.Println("  merge <HyDFSfilename>                        - Merge replicas of a HyDFS file")
	fmt.Println("  ls <HyDFSfilename>                           - List replicas of a HyDFS file")
	fmt.Println("  store                                        - List all files stored on this server")
	fmt.Println("  getfromreplica <VMaddress> <HyDFSfilename> <localfilename> - Get a file from a specific replica")
	fmt.Println("  list_mem_ids                                 - List membership list with ring IDs")
	fmt.Println("  exit                                         - Exit the client")
}

// main function
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <server_address>")
		fmt.Println("Example: go run client.go localhost:23120")
		return
	}

	serverAddress := os.Args[1]
	client := NewClient(serverAddress)
	defer client.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("HyDFS Client Started.")
	displayUsage()

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]

		switch command {
		case "create":
			if len(parts) != 3 {
				fmt.Println("Usage: create <localfilename> <HyDFSfilename>")
				continue
			}
			localFile := parts[1]
			hydfsFile := parts[2]

			// Check if local file exists
			if _, err := os.Stat(localFile); os.IsNotExist(err) {
				fmt.Printf("Local file %s does not exist.\n", localFile)
				continue
			}

			req := Request{
				Operation: CREATE,
				LocalFile: localFile,
				HyDFSFile: hydfsFile,
			}

			resp := client.SendRequest(req)
			fmt.Println(resp.Message)

		case "get":
			if len(parts) != 3 {
				fmt.Println("Usage: get <HyDFSfilename> <localfilename>")
				continue
			}
			hydfsFile := parts[1]
			localFile := parts[2]

			// Check if file is in cache
			if content, exists := client.GetFromCache(hydfsFile); exists {
				// Write to local file
				err := os.WriteFile(localFile, []byte(content), 0644)
				if err != nil {
					fmt.Printf("Failed to write to local file %s: %v\n", localFile, err)
					continue
				}
				fmt.Println("File fetched from cache successfully.")
				continue
			}

			req := Request{
				Operation: GET,
				HyDFSFile: hydfsFile,
				LocalFile: localFile,
			}

			resp := client.SendRequest(req)
			if resp.Status == "success" {
				// Read the fetched file content to cache
				content, err := os.ReadFile(localFile)
				if err == nil {
					client.AddToCache(hydfsFile, string(content))
				}
			}
			fmt.Println(resp.Message)

		case "append":
			if len(parts) != 3 {
				fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
				continue
			}
			localFile := parts[1]
			hydfsFile := parts[2]

			// Check if local file exists
			if _, err := os.Stat(localFile); os.IsNotExist(err) {
				fmt.Printf("Local file %s does not exist.\n", localFile)
				continue
			}

			// Read local file content
			content, err := os.ReadFile(localFile)
			if err != nil {
				fmt.Printf("Failed to read local file %s: %v\n", localFile, err)
				continue
			}

			req := Request{
				Operation: APPEND,
				LocalFile: localFile,
				HyDFSFile: hydfsFile,
				Content:   string(content),
			}

			resp := client.SendRequest(req)
			if resp.Status == "success" {
				// Invalidate cache for this file
				client.InvalidateCache(hydfsFile)
			}
			fmt.Println(resp.Message)

		case "merge":
			if len(parts) != 2 {
				fmt.Println("Usage: merge <HyDFSfilename>")
				continue
			}
			hydfsFile := parts[1]

			req := Request{
				Operation: MERGE,
				HyDFSFile: hydfsFile,
			}

			resp := client.SendRequest(req)
			fmt.Println(resp.Message)

		case "ls":
			if len(parts) != 2 {
				fmt.Println("Usage: ls <HyDFSfilename>")
				continue
			}
			hydfsFile := parts[1]

			req := Request{
				Operation: LS,
				HyDFSFile: hydfsFile,
			}

			resp := client.SendRequest(req)
			if resp.Status == "success" && len(resp.Servers) > 0 {
				fmt.Printf("Replicas of %s:\n", hydfsFile)
				for _, server := range resp.Servers {
					fmt.Printf("Address: %s, RingID: %d\n", server.Address, server.RingID)
				}
			} else {
				fmt.Println(resp.Message)
			}

		case "store":
			req := Request{
				Operation: STORE,
			}

			resp := client.SendRequest(req)
			if resp.Status == "success" && len(resp.Files) > 0 {
				fmt.Println("Files stored on this server:")
				for _, file := range resp.Files {
					fmt.Println(file)
				}
			} else {
				fmt.Println(resp.Message)
			}

		case "getfromreplica":
			if len(parts) != 4 {
				fmt.Println("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>")
				continue
			}
			replicaAddr := parts[1]
			hydfsFile := parts[2]
			localFile := parts[3]

			req := Request{
				Operation:   GETFROM,
				ReplicaAddr: replicaAddr,
				HyDFSFile:   hydfsFile,
				LocalFile:   localFile,
			}

			resp := client.SendRequest(req)
			fmt.Println(resp.Message)

		case "list_mem_ids":
			req := Request{
				Operation: LIST_MEM_IDS,
			}

			resp := client.SendRequest(req)
			if resp.Status == "success" && len(resp.Servers) > 0 {
				fmt.Println("Membership List with Ring IDs:")
				for _, server := range resp.Servers {
					fmt.Printf("Address: %s, RingID: %d\n", server.Address, server.RingID)
				}
			} else {
				fmt.Println(resp.Message)
			}

		case "exit":
			fmt.Println("Exiting HyDFS client.")
			return

		default:
			fmt.Println("Unknown command.")
			displayUsage()
		}
	}
}
