package main

import (
    "bytes"
    "os/exec"
	"fmt"
)

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
}