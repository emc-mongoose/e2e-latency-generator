package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	OP_TYPE_CODE_CREATE = 1
	OP_TYPE_CODE_READ   = 2
	STATUS_CODE_SUCC    = 4
)

func main() {
	args := os.Args
	if len(args) == 2 {
		run(args)
	} else {
		printUsage()
	}
}

func printUsage() {
	fmt.Println("End-to-end latency generator command line options:\n <op_trace_csv_input_file>")
}

type opTraceRecord struct {
	ItemPath           string
	ReqTimeStartMicros int64
	DurationMicros     int64
}

func run(args []string) {
	inFile, err := os.Open(args[1])
	if err != nil {
		fmt.Println("Failed to open the input file " + args[1])
		return
	}
	defer inFile.Close()
	csvReader := csv.NewReader(bufio.NewReader(inFile))
	createRrecords := make(map[string]opTraceRecord)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Failed to read the record from the input file")
			break
		}
		itemPath := line[1]
		opTypeCode, _ := strconv.Atoi(line[2])
		statusCode, _ := strconv.Atoi(line[3])
		timeStartMicros, _ := strconv.ParseInt(line[4], 10, 64)
		durationMicros, _ := strconv.ParseInt(line[5], 10, 64)
		if statusCode != STATUS_CODE_SUCC {
			continue
		}
		switch opTypeCode {
		case OP_TYPE_CODE_CREATE:
			rec := opTraceRecord{
				ItemPath:           itemPath,
				ReqTimeStartMicros: timeStartMicros,
				DurationMicros:     durationMicros,
			}
			createRrecords[rec.ItemPath] = rec
		case OP_TYPE_CODE_READ:
			createRec, found := createRrecords[itemPath]
			if found {
				latencyMicros, err := strconv.ParseInt(line[6], 10, 64)
				if err == nil {
					e2eLat := timeStartMicros + latencyMicros - createRec.ReqTimeStartMicros - createRec.DurationMicros
					fmt.Println(itemPath + "," + strconv.FormatInt(createRec.ReqTimeStartMicros, 10) + "," + strconv.FormatInt(e2eLat, 10))
					delete(createRrecords, itemPath)
				}
			}
		}
	}
}
