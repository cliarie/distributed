package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// queryMachine sends an HTTP request to a specific machine to perform a grep search with options.
func queryMachine(machineURL, grepCommand string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	url := fmt.Sprintf("%s/grep?command=%s", machineURL, strings.ReplaceAll(grepCommand, " ", "%20"))
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		results <- fmt.Sprintf("Error querying %s: %v", machineURL, err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		results <- fmt.Sprintf("Error reading response from %s: %v", machineURL, err)
		return
	}

	results <- fmt.Sprintf("Results from %s:\n%s", machineURL, string(body))
}

// localGrep performs a grep search on the local log file with options.
func localGrep(grepCommand string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	// Split the grep command into the command and arguments
	parts := strings.Fields(grepCommand)
	if len(parts) < 2 {
		results <- "Invalid grep command format"
		return
	}

	cmd := exec.Command("grep", parts...)
	output, err := cmd.Output()
	if err != nil {
		results <- fmt.Sprintf("Error performing local grep: %v", err)
		return
	}

	results <- fmt.Sprintf("Results from local machine:\n%s", string(output))
}

func main() {
	// Prompt the user for the grep command
	var grepCommand string
	fmt.Print("Enter the grep command (e.g., -c -i pattern): ")
	fmt.Scanln(&grepCommand)

	// List of remote machines (example)
	machines := []string{"http://vm1.example.com", "http://vm2.example.com"}

	results := make(chan string, len(machines)+1)
	var wg sync.WaitGroup

	// Perform local grep with options
	wg.Add(1)
	go localGrep(grepCommand, &wg, results)

	// Perform grep on each remote machine with options
	for _, machine := range machines {
		wg.Add(1)
		go queryMachine(machine, grepCommand, &wg, results)
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Print results as they arrive
	for result := range results {
		fmt.Println(result)
	}
}
