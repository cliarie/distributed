package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

// grepHandler handles HTTP requests, executing grep with the provided pattern and options.
func grepHandler(w http.ResponseWriter, r *http.Request) {
	// Get 'pattern' and 'options' query parameters
	pattern := r.URL.Query().Get("pattern")
	options := r.URL.Query().Get("options")

	// Check if pattern is provided
	if pattern == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	// Add the mandatory flags: -n (line numbers) and -H (show filenames)
	mandatoryFlags := []string{"-n", "-H"}

	// Split options into a slice, using Fields to handle multiple options
	optionArgs := strings.Fields(options) // Split options string into arguments

	// Construct full grep command arguments: mandatory flags + options + pattern + file
	cmdArgs := append(mandatoryFlags, optionArgs...)
	cmdArgs = append(cmdArgs, pattern, "vm2.log") 

	// Log the grep command for debugging
	fmt.Printf("Executing: grep %v\n", cmdArgs)

	// Execute the grep command
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput() // CombinedOutput captures both stdout and stderr
	if err != nil {
		// Log and return both the error and the output for easier debugging
		log.Printf("grep failed: %v\nOutput: %s", err, output)
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v\nOutput: %s", err, output), http.StatusInternalServerError)
		return
	}

	// Return the grep output
	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	// Register the /grep endpoint
	http.HandleFunc("/grep", grepHandler)

	// Start the HTTP server on port 8080
	const port = ":8080"
	fmt.Printf("Server starting on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
