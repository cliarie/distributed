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
		record := parseCSVLine(line)
		// Filter by Sign_Post_Type
		if len(record) >= 4 && strings.TrimSpace(record[3]) == filterValue {
			writer.Write(record)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

func parseCSVLine(line string) []string {
	r := csv.NewReader(strings.NewReader(line))
	record, _ := r.Read()
	return record
}
