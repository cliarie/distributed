// Transforms the filtered lines to output only the OBJECTID and Sign_Type columns.
package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	for scanner.Scan() {
		line := scanner.Text()
		record := parseCSVLine(line)
		// Output only OBJECTID and Sign_Type
		if len(record) >= 2 {
			output := []string{record[2], record[3]}
			writer.Write(output)
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
