package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
)

// grepHandler handles HTTP requests to perform a grep search on the local log file.
func grepHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("pattern")
	if query == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	// Execute the grep command on the local machine's log file
	cmd := exec.Command("grep", "-c", query, "machine.log") // Update "machine.log" to your log file path
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	// Register the grep handler for /grep
	http.HandleFunc("/grep", grepHandler)

	// Define the port to listen on (8080 by default)
	const port = ":8080"
	fmt.Printf("Starting server on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
