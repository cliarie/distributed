package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strings"
	"strings"
	"sync"
	"time"
)

// List of only the first two VMs for testing
var machines = []string{
	"http://fa24-cs425-0701.cs.illinois.edu:8080",
	"http://fa24-cs425-0702.cs.illinois.edu:8080",
}

// queryMachine sends an HTTP request to a specific machine to perform a grep search.
func queryMachine(machineURL, pattern string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	url := fmt.Sprintf("%s/grep?pattern=%s", machineURL, pattern)
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

// localGrep performs a grep search on the local log file.
func localGrep(pattern string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	cmd := exec.Command("grep", "-c", pattern, "machine.log") // Adjust "machine.log" as needed
	output, err := cmd.Output()
	if err != nil {
		results <- fmt.Sprintf("Error performing local grep: %v", err)
		return
	}

	results <- fmt.Sprintf("Results from local machine:\n%s", string(output))
}

func main() {
	// Prompt the user for the pattern to search
	var pattern string
	fmt.Print("Enter the pattern to search: ")
	fmt.Scanln(&pattern)

	results := make(chan string, len(machines)+1)
	var wg sync.WaitGroup

	// Perform local grep with options
	wg.Add(1)
	go localGrep(pattern, &wg, results)

	// Perform grep on each remote machine (for now, only the first two VMs)
	for _, machine := range machines {
		wg.Add(1)
		go queryMachine(machine, pattern, &wg, results)
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	totalMatches := 0

	// Print results and count total matches
	for result := range results {
		fmt.Println(result)

		// Count total number of matching lines by checking the output
		lines := strings.Split(result, "\n")
		for _, line := range lines {
			if line != "" && !strings.HasPrefix(line, "Error") && strings.Contains(line, ":") {
				totalMatches++
			}
		}
	}

	fmt.Printf("\nTotal matching lines: %d\n", totalMatches)
}
