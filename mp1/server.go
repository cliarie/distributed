package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

func grepHandler(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	options := r.URL.Query().Get("options")

	if pattern == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	optionArgs := strings.Fields(options) 
	optionArgs = append(optionArgs, "-n", "-H")

	cmdArgs := append(optionArgs, pattern, "vm1.log")

	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput() 
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				w.WriteHeader(http.StatusOK) 
				return
			}
		}
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v\nOutput: %s", err, output), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func main() {
	http.HandleFunc("/grep", grepHandler)

	const port = ":8080"
	fmt.Printf("Server starting on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
