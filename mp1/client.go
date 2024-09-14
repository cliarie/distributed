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

var machines = []string{
	"http://fa24-cs425-0701.cs.illinois.edu:8080",
	"http://fa24-cs425-0702.cs.illinois.edu:8080",
	// "http://fa24-cs425-0703.cs.illinois.edu:8080",
	// "http://fa24-cs425-0704.cs.illinois.edu:8080",
	// "http://fa24-cs425-0705.cs.illinois.edu:8080",
	// "http://fa24-cs425-0706.cs.illinois.edu:8080",
	// "http://fa24-cs425-0707.cs.illinois.edu:8080",
	// "http://fa24-cs425-0708.cs.illinois.edu:8080",
	// "http://fa24-cs425-0709.cs.illinois.edu:8080",
	// "http://fa24-cs425-0710.cs.illinois.edu:8080",
	// Add more VMs as needed
}

// queryMachine sends a grep request to a remote machine.
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

	// Read and return the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		results <- fmt.Sprintf("Error reading response from %s: %v", machineURL, err)
		return
	}
	results <- fmt.Sprintf("%s\n", string(body))
}

// localGrep runs the grep command locally on the machine's log file.
func localGrep(pattern, options string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	// Add the mandatory flags: -n (line numbers) and -H (show filenames)
	mandatoryFlags := []string{"-n", "-H"}

	// Split options into arguments
	optionArgs := strings.Fields(options)

	// Construct the grep command: mandatory flags + user options + pattern + log file
	cmdArgs := append(mandatoryFlags, optionArgs...)
	cmdArgs = append(cmdArgs, pattern, "vm2.log")

	// Execute the grep command
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
	}
}
