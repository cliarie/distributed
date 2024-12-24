// Filters lines based on whether the entire line contains a given value.
package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./filter <filter_value>")
		os.Exit(1)
	}
	filterValue := os.Args[1]

	scanner := bufio.NewScanner(os.Stdin)
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	for scanner.Scan() {
		line := scanner.Text()
		// Check if the line contains the filter value
		if strings.Contains(line, filterValue) {
			record := parseCSVLine(line)
			writer.Write(record)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

// Parse a single CSV line into a slice of strings
func parseCSVLine(line string) []string {
	r := csv.NewReader(strings.NewReader(line))
	record, _ := r.Read()
	return record
}