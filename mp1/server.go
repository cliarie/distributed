package main

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
    "io/ioutil"
	"os"
)

func grepHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Grep Request");
	pattern := r.URL.Query().Get("pattern")
	options := r.URL.Query().Get("options")

	if pattern == "" {
		http.Error(w, "Pattern query parameter is missing", http.StatusBadRequest)
		return
	}

	optionArgs := strings.Fields(options)
	filename := "vm1.log"
	newOptionArgs := []string{}
    for _, arg := range optionArgs {
        if arg == "-t" {
            filename = "test.log"
        } else {
            newOptionArgs = append(newOptionArgs, arg)
        }
    }
	newOptionArgs = append(newOptionArgs, "-n", "-H")
	
	cmdArgs := append(newOptionArgs, pattern, filename)

	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput() 
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				w.WriteHeader(http.StatusOK) 
				return
			}
		}
		http.Error(w, fmt.Sprintf("Failed to execute grep: %v\nOutput: %s\n", err, output), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(output)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Upload Request");
    if r.Method != http.MethodPost {
        http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
        return
    }

	body, err := ioutil.ReadAll(r.Body)
    err = ioutil.WriteFile("test.log", body, 0777)
    if err != nil {
        http.Error(w, "Failed to write to file", http.StatusInternalServerError)
        return
    }

    fmt.Fprintf(w, "Text successfully written to test.log")
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Delete Request");
    if r.Method != http.MethodDelete {
        http.Error(w, "Only DELETE method is allowed", http.StatusMethodNotAllowed)
        return
    }
    os.Remove("test.log")
    fmt.Fprintf(w, "File test.log successfully deleted")
}

func main() {
	http.HandleFunc("/grep", grepHandler)
    http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/delete", deleteHandler)

	const port = ":8080"
	fmt.Printf("Server starting on %s...\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
