// Aggregates the counts of Category for each key (Sign_Post_Type).
package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	counts := make(map[string]int)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		record := parseCSVLine(line)
		// Aggregate counts by Category
		if len(record) >= 7 {
			category := strings.TrimSpace(record[8])
			counts[category]++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		return
	}

	// Output the aggregated counts
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()
	for key, count := range counts {
		writer.Write([]string{key, fmt.Sprintf("%d", count)})
	}
}

func parseCSVLine(line string) []string {
	r := csv.NewReader(strings.NewReader(line))
	record, _ := r.Read()
	return record
}
