package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/djimenez/iconv-go"
	"github.com/gogs/chardet"
)

func processCsv(ctx context.Context, fileEventCh chan fileEvent) chan fileProcessorEvent {

	resultCh := make(chan fileProcessorEvent)
	sendResult := func(r *csvProcessorEvent) bool {
		select {
		case <-ctx.Done():
			return true
		case resultCh <- r:
			return false
		}
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("csv closing...")

		for {
			select {
			case <-ctx.Done():
				return
			case fileEvent, ok := <-fileEventCh:
				if !ok {
					return
				}

				if fileEvent.getError() != nil {
					sendResult(&csvProcessorEvent{
						err: fileEvent.getError(),
					})
					break
				}

				parseCSV(ctx, fileEvent.file(), sendResult)
			}
		}

	}()

	return resultCh
}

func parseCSV(ctx context.Context, file *os.File, sendResult func(*csvProcessorEvent) bool) {

	defaultSeparator := ','
	if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
		for _, r := range v {
			defaultSeparator = r
		}
	}

	convertToUTF8, exist := os.LookupEnv("CONVERT_TO_UTF8")
	encodingReader, err := getEncodingReader(exist && (convertToUTF8 == "true"), file)
	if err != nil {
		sendResult(&csvProcessorEvent{
			err: wrapError(fmt.Errorf("parseCSV: failed to create encoding Reader for %v file %w", file.Name(), err)),
		})
		return
	}

	reader := csv.NewReader(encodingReader)
	reader.Comma = defaultSeparator

	headers, err := reader.Read()
	if err != nil {
		if err != io.EOF {
			sendResult(&csvProcessorEvent{
				err: wrapError(fmt.Errorf("parseCSV: failed to read %v file header %w", file.Name(), err)),
			})
		}
		return
	}

	c := 1
	for {
		data, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			sendResult(&csvProcessorEvent{
				err: wrapError(fmt.Errorf("parseCSV: failed to read %v file %w", file.Name(), err)),
			})
			break
		}
		c++

		row := make(map[string]string)
		for i, header := range headers {
			row[header] = data[i]
		}

		if isDone := sendResult(&csvProcessorEvent{
			line: c,
			data: row,
			file: file,
		}); isDone {
			return
		}

	}

}

func getEncodingReader(convertToUTF8 bool, file *os.File) (io.Reader, error) {
	if convertToUTF8 {
		return file, nil
	} else {
		encoding, err := detectEncoing(file)
		// fmt.Println(*encoding)
		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: %w", err)
		}
		return iconv.NewReader(file, *encoding, "utf-8")
	}
}

func detectEncoing(f *os.File) (*string, error) {
	f, err := os.Open(f.Name())
	// defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("detectEncoing: failed to open the file %w", err)
	}

	var buff bytes.Buffer
	scanner := bufio.NewScanner(f)
	numLineToSample := 0
	for scanner.Scan() {
		buff.Write([]byte(scanner.Text()))
		if numLineToSample >= 10 {
			break
		}
		numLineToSample++
	}

	if err = scanner.Err(); err != nil {
		if err != io.EOF {
			return nil, fmt.Errorf("detectEncoing: failed to sample file to detect encoding %w", err)
		}
	}

	chardet := chardet.NewTextDetector()
	chardetResult, err := chardet.DetectBest(buff.Bytes())

	if err != nil {
		return nil, fmt.Errorf("detectEncoing: failed to detect encoding")
	}
	return &chardetResult.Charset, nil
}
