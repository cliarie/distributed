/*
HyDFS Client: Provides an interface for users to perform file operations (create, get, append, merge)
Manages client-side caching to optimize read performance.
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
	"io"
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
	timeoutDuration = 1 * time.Second
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

// Client represents the HyDFS client's state with TCP connection, server address, and caching
type Client struct {
	conn       *net.TCPConn
	serverAddr *net.TCPAddr
	cache      map[string]CacheEntry
	cacheMutex sync.Mutex
	cacheOrder []string // To implement LRU
}

// NewClient initializes the client using TCP
func NewClient(serverAddress string) *Client {
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to resolve server address %s: %v", serverAddress, err)
	}

	client := &Client{
		serverAddr: addr,
		cache:      make(map[string]CacheEntry), // set up cache map for LRU
		cacheOrder: []string{},                  // set up order for LRU
	}

	return client
}

// SendRequest reads JSON response until delimiter
func (c *Client) SendRequest(req Request) (Response, *bufio.Reader) {
	if c.conn != nil {
		c.conn.Close()
	}

	// Send request as before
	conn, err := net.DialTCP("tcp", nil, c.serverAddr)
	if err != nil {
		log.Fatalf("Failed to dial server at %s: %v", c.serverAddr, err)
		return Response{
			Status:  "error",
			Message: "Failed to connect to server.",
		}, nil
	}
	c.conn = conn

	data, err := json.Marshal(req)
	if err != nil {
		return Response{Status: "error", Message: "Failed to marshal request."}, nil
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
			return Response{Status: "error", Message: "Failed to read JSON response."}, nil
		}

		// Append byte to jsonData
		jsonData = append(jsonData, b)

		// Check if we've reached the delimiter (e.g., "\n\n")
		if len(jsonData) >= 2 && string(jsonData[len(jsonData)-2:]) == "\n\n" {
			break
		}
	}

	fmt.Printf("json: %s\n", jsonData)
	var resp Response
	err = json.Unmarshal(jsonData, &resp)
	if err != nil {
		return Response{Status: "error", Message: "Failed to unmarshal JSON."}, nil
	}

	return resp, reader
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
			resp, _ := client.SendRequest(req)

			content, err := os.ReadFile(localFile)
			if err != nil {
				return
			}
			client.conn.Write(content)
			fmt.Println(resp.Message)
			continue

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
			}
		
			resp, reader := client.SendRequest(req)
			if resp.Status != "success" {
				fmt.Println(resp.Message)
				continue
			}
		
			// Open or create the local file for writing
			file, err := os.Create(localFile)
			if err != nil {
				fmt.Printf("Failed to create local file %s: %v\n", localFile, err)
				continue
			}
			defer file.Close()

			// Read file data from server
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
		
				// Write data to local file
				_, err = file.Write(buffer[:n])
				if err != nil {
					fmt.Printf("Failed to write data to local file: %v\n", err)
					break
				}
			}
		
			fmt.Println("File downloaded successfully.")

		case "append":
			if len(parts) != 3 {
				fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
				continue
			}
			localFile := parts[1]
			hydfsFile := parts[2]

			req := Request{
				Operation: APPEND,
				LocalFile: localFile,
				HyDFSFile: hydfsFile,
			}

			resp, _ := client.SendRequest(req)
			if resp.Status == "success" {
				// Invalidate cache for this file
				client.InvalidateCache(hydfsFile)
			}
			content, _ := os.ReadFile(localFile)
			client.conn.Write(content)
			fmt.Println(resp.Message)
			continue

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

			resp, _ := client.SendRequest(req)
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

			resp, _ := client.SendRequest(req)
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

			resp, _ := client.SendRequest(req)
			if resp.Status == "success" && len(resp.Files) > 0 {
				fmt.Println("Files stored on this server:")
				for _, file := range resp.Files {
					fmt.Println(file)
				}
			} else {
				fmt.Println(resp.Message)
			}
		
		// THIS IS JUST A WRAPPER AROUND SENDING A GET TO <VMaddress>
		case "getfromreplica":
			if len(parts) != 4 {
				fmt.Println("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>")
				continue
			}
			replicaAddr := parts[1]
			hydfsFile := parts[2]
			localFile := parts[3]

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
			}
			
			// connect client to replicaAddr
			//fa24-cs425-0701.cs.illinois.edu:8080
			oldAddr := client.serverAddr
			addr, _ := net.ResolveTCPAddr("tcp", replicaAddr)
			client.serverAddr = addr
			resp, reader := client.SendRequest(req)
			if resp.Status != "success" {
				fmt.Println(resp.Message)
				continue
			}
			// set client back to replicaAddr
			client.serverAddr = oldAddr
		
			// Open or create the local file for writing
			file, err := os.Create(localFile)
			if err != nil {
				fmt.Printf("Failed to create local file %s: %v\n", localFile, err)
				continue
			}
			defer file.Close()

			// Read file data from server
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
		
				// Write data to local file
				_, err = file.Write(buffer[:n])
				if err != nil {
					fmt.Printf("Failed to write data to local file: %v\n", err)
					break
				}
			}
		
			fmt.Println("File downloaded from replica successfully.")

		case "list_mem_ids":
			req := Request{
				Operation: LIST_MEM_IDS,
			}

			resp, _ := client.SendRequest(req)
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
