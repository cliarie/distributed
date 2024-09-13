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

// List of VM addresses (server URLs) in the cluster
var machines = []string{
	"http://fa24-cs425-0701.cs.illinois.edu:8080",
	"http://fa24-cs425-0702.cs.illinois.edu:8080",
	// Add other VMs here
}

// queryMachine sends an HTTP request to a specific machine to perform a grep search with options.
func queryMachine(machineURL, pattern, options string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	// Build the request URL with pattern and options
	url := fmt.Sprintf("%s/grep?pattern=%s&options=%s", machineURL, pattern, options)
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
func localGrep(pattern, options string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	// Split the options into arguments
	optionArgs := strings.Fields(options)

	// Construct the full grep command with options
	cmdArgs := append(optionArgs, pattern, "machine.log") // Adjust "machine.log" as needed
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		results <- fmt.Sprintf("Error performing local grep: %v\nOutput: %s", err, output)
		return
	}

	results <- fmt.Sprintf("Results from local machine:\n%s", string(output))
}

func main() {
	// Prompt the user for the pattern to search
	var pattern string
	fmt.Print("Enter the pattern to search: ")
	fmt.Scanln(&pattern)

	// Prompt the user for grep options
	var options string
	fmt.Print("Enter any additional grep options (or leave empty for none): ")
	fmt.Scanln(&options)

	// URL encode the options to handle spaces and special characters
	encodedOptions := strings.ReplaceAll(options, " ", "+")

	results := make(chan string, len(machines)+1)
	var wg sync.WaitGroup

	// Perform local grep
	wg.Add(1)
	go localGrep(pattern, options, &wg, results)

	// Perform grep on each remote machine
	for _, machine := range machines {
		wg.Add(1)
		go queryMachine(machine, pattern, encodedOptions, &wg, results)
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Print results as they arrive
	totalMatches := 0
	for result := range results {
		fmt.Println(result)

		// Count total number of matching lines by splitting the result by newline
		lines := strings.Split(result, "\n")
		for _, line := range lines {
			if line != "" && !strings.HasPrefix(line, "Error") && strings.Contains(line, ":") {
				totalMatches++
			}
		}
	}

	fmt.Printf("\nTotal matching lines: %d\n", totalMatches)
}
