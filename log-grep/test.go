package main

import (
    "bytes"
    "os/exec"
	"fmt"
	"math/rand"
    "net/http"
	"strings"
	"strconv"
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

const letters = "abcdefghijklmnopqrstuvwxyz"
var set = make(map[string]struct{})

func generateRandomWord(length int) string {
	for {
		word := make([]byte, length)
		for i := range word {
			word[i] = letters[rand.Intn(len(letters))]
		}
		wordStr := string(word)

		if _, exists := set[wordStr]; !exists {
			set[wordStr] = struct{}{}
			return wordStr
		}
	}
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

	passed := 0
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
			passed += 1
        }
    }

	var logs [10][]string

	rareCount := rand.Intn(3) + 1
	rareAll := generateRandomWord(4)
	for i := 0; i < 10; i++ {
		for j := 0; j < rareCount; j++ {
			logs[i] = append(logs[i], rareAll, "\n")
		}
	}
	rareSome := generateRandomWord(4)
	for i := 0; i < 4; i++ {
		for j := 0; j < rareCount; j++ {
			logs[i] = append(logs[i], rareSome, "\n")
		}
	}
	rareOne := generateRandomWord(4)
	for i := 0; i < 1; i++ {
		for j := 0; j < rareCount; j++ {
			logs[i] = append(logs[i], rareOne, "\n")
		}
	}


	freqCount := rand.Intn(50) + 50
	freqAll := generateRandomWord(4)
	for i := 0; i < 10; i++ {
		for j := 0; j < freqCount; j++ {
			logs[i] = append(logs[i], freqAll, "\n")
		}
	}
	freqSome := generateRandomWord(4)
	for i := 0; i < 4; i++ {
		for j := 0; j < freqCount; j++ {
			logs[i] = append(logs[i], freqSome, "\n")
		}
	}
	freqOne := generateRandomWord(4)
	for i := 0; i < 1; i++ {
		for j := 0; j < freqCount; j++ {
			logs[i] = append(logs[i], freqOne, "\n")
		}
	}

	
	somewhatFreqCount := rand.Intn(10) + 10
	somewhatFreqAll := generateRandomWord(4)
	for i := 0; i < 10; i++ {
		for j := 0; j < somewhatFreqCount; j++ {
			logs[i] = append(logs[i], somewhatFreqAll, "\n")
		}
	}
	somewhatFreqSome := generateRandomWord(4)
	for i := 0; i < 4; i++ {
		for j := 0; j < somewhatFreqCount; j++ {
			logs[i] = append(logs[i], somewhatFreqSome, "\n")
		}
	}
	somewhatFreqOne := generateRandomWord(4)
	for i := 0; i < 1; i++ {
		for j := 0; j < somewhatFreqCount; j++ {
			logs[i] = append(logs[i], somewhatFreqOne, "\n")
		}
	}

	for i := 0; i < 10; i++ {
		payload := []byte(strings.Join(logs[i], ""))
		req, _ := http.NewRequest("POST", machines[i] + "/upload", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "text/plain")
		client := &http.Client{}
		resp, _ := client.Do(req)
		resp.Body.Close()
	}

	cmd := exec.Command("go", "run", "client.go", "-c", "-t", rareAll)
    var out bytes.Buffer
    cmd.Stdout = &out
	cmd.Run()
	parts := strings.Fields(out.String())
	count, _ := strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("rareAll matches: %d\n", count)
	if count == rareCount * 10 {
        fmt.Println("rareAll case passed")
		passed += 1
    } else {
        fmt.Println("rareAll case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", rareSome)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("rareSome matches: %d\n", count)
	if count == rareCount * 4 {
        fmt.Println("rareSome case passed")
		passed += 1
    } else {
        fmt.Println("rareSome case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", rareOne)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("rareOne matches: %d\n", count)
	if count == rareCount * 1 {
        fmt.Println("rareOne case passed")
		passed += 1
    } else {
        fmt.Println("rareOne case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", freqAll)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("freqAll matches: %d\n", count)
	if count == freqCount * 10 {
        fmt.Println("freqAll case passed")
		passed += 1
    } else {
        fmt.Println("freqAll case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", freqSome)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("freqSome matches: %d\n", count)
	if count == freqCount * 4 {
        fmt.Println("freqSome case passed")
		passed += 1
    } else {
        fmt.Println("freqSome case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", freqOne)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("freqOne matches: %d\n", count)
	if count == freqCount * 1 {
        fmt.Println("freqOne case passed")
		passed += 1
    } else {
        fmt.Println("freqOne case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", somewhatFreqAll)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("somewhatFreqAll matches: %d\n", count)
	if count == somewhatFreqCount * 10 {
        fmt.Println("somewhatFreqAll case passed")
		passed += 1
    } else {
        fmt.Println("somewhatFreqAll case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", somewhatFreqSome)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("somewhatFreqSome matches: %d\n", count)
	if count == somewhatFreqCount * 4 {
        fmt.Println("somewhatFreqSome case passed")
		passed += 1
    } else {
        fmt.Println("somewhatFreqSome case failed")
    }
	out.Reset()

	cmd = exec.Command("go", "run", "client.go", "-c", "-t", somewhatFreqOne)
    cmd.Stdout = &out
	cmd.Run()
	parts = strings.Fields(out.String())
	count, _ = strconv.Atoi(parts[len(parts) - 1])
	fmt.Printf("somewhatFreqOne matches: %d\n", count)
	if count == somewhatFreqCount * 1 {
        fmt.Println("somewhatFreqOne case passed")
		passed += 1
    } else {
        fmt.Println("somewhatFreqOne case failed")
    }
	out.Reset()

	fmt.Printf("\n\n %d/11 cases passed.\n", passed)

	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest("DELETE", machines[i] + "/delete", nil)
		client := &http.Client{}
		resp, _ := client.Do(req)
		defer resp.Body.Close()
	}
}