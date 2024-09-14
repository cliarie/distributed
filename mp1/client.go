package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"strconv"
	"time"
	"os"
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
}

func queryMachine(machineURL, pattern, options string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

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
	results <- fmt.Sprintf("%s", string(body))
}

func main() {
	if len(os.Args) < 2 {
        fmt.Println("Usage: [args] [pattern]")
        os.Exit(1)
    }

    pattern := os.Args[len(os.Args)-1]
    options := os.Args[1 : len(os.Args)-1]
	for _, option := range options {
        if !strings.HasPrefix(option, "-") {
            fmt.Printf("Invalid option: %s. All options must start with '-'.\n", option)
            os.Exit(1)
        }
    }
	encodedOptions := strings.Join(options, "+")

	results := make(chan string, len(machines)+1)
	var wg sync.WaitGroup

	for _, machine := range machines {
		wg.Add(1)
		go queryMachine(machine, pattern, encodedOptions, &wg, results)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	total := 0
	for result := range results {
		fmt.Printf(result)
		if strings.Contains(encodedOptions, "-c"){
			index := strings.Index(result, ":")
			matches, _ := strconv.Atoi(result[index + 1:len(result) - 1])
			total += matches	
		}

	}
	if strings.Contains(encodedOptions, "-c"){
		fmt.Printf("Total matches across all logs/machines: %d\n", total)
	}

}
