package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

// grepHandler handles HTTP requests to perform a grep search with user-provided options.
func grepHandler(w http.ResponseWriter, r *http.Request) {
	// Retrieve the 'pattern' and 'options' query parameters
	query := r.URL.Query().Get("pattern")
	options := r.URL.Query().Get("options")

	if query == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	// Split the options by spaces (for multi-word options)
	optionArgs := strings.Fields(options)

	// Construct the full command with pattern and options
	cmdArgs := append(optionArgs, query, "machine.log") // Replace "machine.log" with the actual log file path

	// Execute the grep command
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v", err), http.StatusInternalServerError)
		return
	}

	// Send back the result with the matching lines
	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	// Register the grep handler for /grep
	fmt.Printf("Here!")
	http.HandleFunc("/grep", grepHandler)

	// Define the port to listen on
	const port = ":8080"
	fmt.Printf("Starting server on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
