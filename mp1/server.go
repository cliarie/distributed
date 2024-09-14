package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

// grepHandler processes the pattern and options from the client, executes grep, and returns the result.
func grepHandler(w http.ResponseWriter, r *http.Request) {
	// Get the pattern and options from the query parameters
	pattern := r.URL.Query().Get("pattern")
	options := r.URL.Query().Get("options")

	if pattern == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	// Split the options by spaces to get individual arguments
	optionArgs := strings.Fields(options) // Split options into arguments

	// Construct the command arguments: options, pattern, and the log file path
	cmdArgs := append(optionArgs, pattern, "machine.log") // Adjust "machine.log" as needed

	// Execute the grep command
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput() // CombinedOutput captures both stdout and stderr
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				// Exit code 1 indicates no matches were found; handle this gracefully
				w.WriteHeader(http.StatusOK) // Return a 200 OK with empty body
				return
			}
		}
		// Return both the error and the output for debugging purposes
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v\nOutput: %s", err, output), http.StatusInternalServerError)
		return
	}

	// Return the grep output as the response
	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	// Handle requests to /grep
	http.HandleFunc("/grep", grepHandler)

	// Define the port to listen on
	const port = ":8080"
	fmt.Printf("Server starting on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
