/*
HyDFS Client: Provides an interface for users to perform file operations (create, get, append, merge)
Manages client-side caching to optimize read performance.
*/
package main

import (
	"encoding/json"
	"log"
	"net"
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
