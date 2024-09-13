package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"sync"
	"time"
)

// List of VM addresses in the cluster
var machines = []string{
	"http://fa24-cs425-0701.cs.illinois.edu:8080",
	"http://fa24-cs425-0702.cs.illinois.edu:8080",
	"http://fa24-cs425-0703.cs.illinois.edu:8080",
	"http://fa24-cs425-0704.cs.illinois.edu:8080",
	"http://fa24-cs425-0705.cs.illinois.edu:8080",
	"http://fa24-cs425-0706.cs.illinois.edu:8080",
	"http://fa24-cs425-0707.cs.illinois.edu:8080",
	"http://fa24-cs425-0708.cs.illinois.edu:8080",
	"http://fa24-cs425-0709.cs.illinois.edu:8080",
	"http://fa24-cs425-0710.cs.illinois.edu:8080",
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

	// Perform local grep
	wg.Add(1)
	go localGrep(pattern, &wg, results)

	// Perform grep on each remote machine
	for _, machine := range machines {
		wg.Add(1)
		go queryMachine(machine, pattern, &wg, results)
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