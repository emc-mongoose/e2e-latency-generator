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
	OP_TYPE_CODE_CREATE   = 1
	OP_TYPE_CODE_READ     = 2
	STATUS_CODE_SUCC      = 4
	HEATMAP_X_STEP_MICROS = 1000000
	HEATMAP_Y_STEP_COUNT  = 100
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
	createRecords := make(map[string]opTraceRecord)
	minReqTimeStartMicros := int64(0)
	minReqTimeStartMicrosWasSet := false
	e2eLatMax := int64(0)
	e2eLatSrcData := make([][]int64, 1)
	xOffset := 0
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
			if !minReqTimeStartMicrosWasSet {
				minReqTimeStartMicros = timeStartMicros
				minReqTimeStartMicrosWasSet = true
			}
			rec := opTraceRecord{
				ReqTimeStartMicros: timeStartMicros,
				DurationMicros:     durationMicros,
			}
			createRecords[itemPath] = rec
		case OP_TYPE_CODE_READ:
			createRec, found := createRecords[itemPath]
			if found {
				latencyMicros, err := strconv.ParseInt(line[6], 10, 64)
				if err == nil {
					e2eLat := timeStartMicros + latencyMicros - createRec.ReqTimeStartMicros - createRec.DurationMicros
					if e2eLat > e2eLatMax {
						e2eLatMax = e2eLat
					}
					timeOffsetMicros := createRec.ReqTimeStartMicros - minReqTimeStartMicros
					xOffsetNew := int(timeOffsetMicros / HEATMAP_X_STEP_MICROS)
					if xOffset < xOffsetNew {
						xOffset = xOffsetNew
						for {
							e2eLatSrcData = append(e2eLatSrcData, make([]int64, 1))
							if xOffset < len(e2eLatSrcData) {
								break
							}
						}
						e2eLatSrcData = append(e2eLatSrcData, make([]int64, 1))
						e2eLatSrcData[xOffset][0] = e2eLat
					} else {
						e2eLatSrcData[xOffset] = append(e2eLatSrcData[xOffset], e2eLat)
					}
					fmt.Println(itemPath + "," + strconv.FormatInt(timeOffsetMicros, 10) + "," + strconv.FormatInt(e2eLat, 10))
					delete(createRecords, itemPath)
				}
			}
		}
	}
	yStepSize := float64(e2eLatMax) / HEATMAP_Y_STEP_COUNT
	maxCount := 0
	counts := make([][HEATMAP_Y_STEP_COUNT]int, len(e2eLatSrcData))
	for colPos, e2eLatCol := range e2eLatSrcData {
		for rowPos := 0; rowPos < HEATMAP_Y_STEP_COUNT; rowPos++ {
			minVal := yStepSize * float64(rowPos)
			maxVal := minVal + yStepSize
			count := 0
			for _, val := range e2eLatCol {
				if float64(val) >= minVal && float64(val) < maxVal {
					count++
				}
			}
			if count > maxCount {
				maxCount = count
			}
			counts[colPos][rowPos] = count
		}
	}
	fmt.Println("Yohoho")
}
