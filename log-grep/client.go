package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"net/url"
	"sync"
	"strconv"
	"time"
	"os"
)

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

func queryMachine(machineURL, machineNumber, pattern, options string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	pattern = url.QueryEscape(pattern)
	// fmt.Printf("%s\n", pattern)
	url := fmt.Sprintf("%s/grep?machineNumber=%s&pattern=%s&options=%s", machineURL, machineNumber, pattern, options)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		// results <- fmt.Sprintf("Error querying %s: %v", machineURL, err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
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

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go queryMachine(machines[i], strconv.Itoa(i + 1), pattern, encodedOptions, &wg, results)
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
			if index < 0 {
				continue
			}
			matches, _ := strconv.Atoi(result[index + 1:len(result) - 1])
			total += matches	
		}

	}
	if strings.Contains(encodedOptions, "-c"){
		fmt.Printf("Total matches across all logs/machines: %d\n", total)
	}

}
