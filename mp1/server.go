package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
)

// grepHandler handles HTTP requests to perform a grep search on the local log file.
func grepHandler(w http.ResponseWriter, r *http.Request) {
	// Get the complete grep command from the query parameter
	grepCommand := r.URL.Query().Get("command")
	if grepCommand == "" {
		http.Error(w, "Grep command query parameter is missing", http.StatusBadRequest)
		return
	}

	// Split the grep command into arguments
	args := strings.Fields(grepCommand)
	args := append(args, "vm1.log")

	// Build the command: grep <options> <pattern> <file>
	cmd := exec.Command("grep", args...)
	
	// Execute the grep command
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v", err), http.StatusInternalServerError)
		return
	}

	// Send the output as the response
	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	// Register the grep handler for /grep
	fmt.Printf("Here!")
	http.HandleFunc("/grep", grepHandler)

	// Define the port to listen on (8080 by default)
	const port = ":8080"
	fmt.Printf("Starting server on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
