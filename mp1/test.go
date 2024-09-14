package main

import (
    "bytes"
    "os/exec"
	"fmt"
)

const letters = "abcdefghijklmnopqrstuvwxyz"
set := make(map[string]struct{})

func generateRandomWord(length int) string {
	
	word := make([]byte, length)
	for i := range word {
		word[i] = letters[rand.Intn(len(letters))]
	}

	set[string(word)] = struct{}{}
	return string(word)
}

func main() {
    testCases := []struct {
        name           string
        args           []string
        expectedOutput string
    }{
        {
            name:           "Invalid Input Length",
            args:           []string{},
            expectedOutput: "Usage: [args] [pattern]\n",
        },
		{
            name:           "Invalid Args",
            args:           []string{"13132","-a","ok"},
            expectedOutput: "Invalid option: 13132. All options must start with '-'.\n",
        },
    }

    for _, tc := range testCases {
		cmd := exec.Command("go", "run", "client.go")
		cmd.Args = append(cmd.Args, tc.args...)
        var out bytes.Buffer
        var errOut bytes.Buffer
        cmd.Stdout = &out
        cmd.Stderr = &errOut
		cmd.Run()

        actualOutput := out.String()
        if actualOutput != tc.expectedOutput {
            fmt.Printf("Output mismatch for %s.\nExpected:\n%s\nGot:\n%s\n", tc.name, tc.expectedOutput, actualOutput)
        } else {
            fmt.Printf("Test %s passed.\n", tc.name)
        }
    }

	//distributed tests
	//first generate some known and some random stuff in each log file
	//then write tests for grep for inputs that 
	// rare, frequent, somewhat frequent
	// x
	// one/some/all logs
	// generate log (exists on all machines)
	// just generates a log with the given text by the caller
	// we will call and have 5 known line the same across all logs
	// rare: 1x in 1/5/10 logs
	// somewhat frequent: 20x in 1/5/10 logs
	// frequent: 100x in 1/5/10 logs
	// Input generation: inputs[10]
	//  1 2 3 ... 10
	//  ->

	var logs [10][]string

	rareAll := generateRandomWord(4)
	for i := 0; i < 10; i++ {
		logs[i] = append(logs[i], rareAll, "\n")
	}

	rareSome := generateRandomWord(4)
	for i := 0; i < 5; i++ {
		logs[i] = append(logs[i], rareSome, "\n")
	}

	rareOne := generateRandomWord(4)
	for i := 0; i < 1; i++ {
		logs[i] = append(logs[i], rareOne, "\n")
	}

	// Print the logs to verify
	for i, log := range logs {
		fmt.Printf("logs[%d]: %s\n", i, log)
	}

	frequentAll := generateRandomWord(4)

	// "vm2:8000/upload-to-test-log"
	// vm -> write to test.log\

	// -> run client
	// -> check answers
	
}