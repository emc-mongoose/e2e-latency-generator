package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"math"
	"os"
	"strconv"
)

const (
	OP_TYPE_CODE_CREATE   = 1
	OP_TYPE_CODE_READ     = 2
	STATUS_CODE_SUCC      = 4
	HEATMAP_X_STEP_MICROS = 1000000
	HEATMAP_Y_STEP_COUNT  = 100
	HEATMAP_MAX_VALUE     = 255
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

	// open the source csv file
	inFile, err := os.Open(args[1])
	if err != nil {
		fmt.Println("Failed to open the input file " + args[1])
		return
	}
	defer inFile.Close()
	csvReader := csv.NewReader(bufio.NewReader(inFile))

	// produce the latency data
	createRecords := make(map[string]opTraceRecord)
	minReqTimeStartMicros := int64(0)
	minReqTimeStartMicrosWasSet := false
	latMin := int64(9223372036854775807) // max value
	latMax := int64(0)
	latSrcData := make([][]int64, 1)
	latSrcData[0] = make([]int64, 1)
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
					lat := timeStartMicros + latencyMicros - createRec.ReqTimeStartMicros - createRec.DurationMicros
					if lat < latMin {
						latMin = lat
					}
					if lat > latMax {
						latMax = lat
					}
					timeOffsetMicros := createRec.ReqTimeStartMicros - minReqTimeStartMicros
					xOffset := int(timeOffsetMicros / HEATMAP_X_STEP_MICROS)
					for {
						if len(latSrcData) > xOffset {
							break
						}
						latSrcData = append(latSrcData, make([]int64, 1))
					}
					latSrcData[xOffset] = append(latSrcData[xOffset], lat)
					// print raw latency data to the standard output
					fmt.Println(
						itemPath + "," + strconv.FormatInt(timeOffsetMicros, 10) + "," +
							strconv.FormatInt(lat, 10))
					delete(createRecords, itemPath)
				}
			}
		}
	}

	// produce the counts on the logarithmic scale
	maxToMinRatio := float64(latMax) / float64(latMin)
	yStepFactor := math.Pow(maxToMinRatio, 1.0/HEATMAP_Y_STEP_COUNT)
	maxCount := 0
	seqLen := len(latSrcData)
	counts := make([][HEATMAP_Y_STEP_COUNT]int, seqLen)
	for colPos, latCol := range latSrcData {
		for rowPos := 0; rowPos < HEATMAP_Y_STEP_COUNT; rowPos++ {
			cellFrom := float64(latMin) * math.Pow(yStepFactor, float64(rowPos))
			cellTo := cellFrom * yStepFactor
			count := 0
			for _, val := range latCol {
				if float64(val) >= cellFrom && float64(val) < cellTo {
					count++
				}
			}
			if count > maxCount {
				maxCount = count
			}
			counts[colPos][rowPos] = count
		}
	}

	// draw the output image
	heatMapImg := image.NewRGBA(image.Rect(0, 0, seqLen, HEATMAP_Y_STEP_COUNT))
	// fill with white
	bgColor := color.RGBA{255, 255, 255, 255}
	draw.Draw(heatMapImg, heatMapImg.Bounds(), &image.Uniform{bgColor}, image.ZP, draw.Src)
	for colPos := range counts {
		for rowPos := 0; rowPos < HEATMAP_Y_STEP_COUNT; rowPos++ {
			v := uint8(HEATMAP_MAX_VALUE - HEATMAP_MAX_VALUE*counts[colPos][rowPos]/maxCount)
			c := color.RGBA{v, v, v, 255}
			heatMapImg.Set(colPos, HEATMAP_Y_STEP_COUNT-rowPos, c)
		}
	}
	w, err := os.Create("heatmap.png")
	if err != nil {
		panic(err)
	}
	defer w.Close()
	err = png.Encode(w, heatMapImg)
	if err != nil {
		panic(err)
	}
}
