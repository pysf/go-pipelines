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

type outputProcessor interface {
	sendMessages(rows map[string]string) error
}

func processCsv(ctx context.Context, fileEventCh chan fileEvent) chan fileProcessorEvent {

	resultCh := make(chan fileProcessorEvent)
	go func() {
		defer close(resultCh)

		for {
			select {
			case <-ctx.Done():
				return
			case fileEvent, ok := <-fileEventCh:
				if !ok {
					return
				}

				if fileEvent.getError() != nil {
					resultCh <- &csvProcessorEvent{
						err: fileEvent.getError(),
					}
					break
				}

				parseCSV(ctx, fileEvent.file(), resultCh)

			}
		}

	}()

	return resultCh
}

func parseCSV(ctx context.Context, file *os.File, output chan fileProcessorEvent) {
	reader, err := csvReader(file)
	if err != nil {
		output <- &csvProcessorEvent{
			err: fmt.Errorf("parseCSV:  %w", err),
		}
		return
	}

	headers, err := reader.Read()

	if err != nil {
		if err != io.EOF {
			output <- &csvProcessorEvent{
				err: fmt.Errorf("parseCSV: failed to read %v file header %w", file.Name(), err),
			}
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
			output <- &csvProcessorEvent{
				err: fmt.Errorf("parseCSV: failed to read %v file %w", file.Name(), err),
			}
			break
		}

		row := make(map[string]string)
		for i, header := range headers {
			row[header] = data[i]
		}

		select {
		case <-ctx.Done():
			return
		case output <- &csvProcessorEvent{
			line: c,
			data: row,
			file: file,
		}:
		}
		c++
	}

}

func csvReader(file *os.File) (*csv.Reader, error) {

	defaultSeparator := ','
	if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
		for _, r := range v {
			defaultSeparator = r
		}
	}

	encodingReader, err := getEncodingReader(file)
	if err != nil {
		return nil, fmt.Errorf("csvReader: failed to create encoding Reader for %v file %w", file.Name(), err)
	}

	csvReader := csv.NewReader(encodingReader)
	csvReader.Comma = defaultSeparator

	return csvReader, nil

}

func getEncodingReader(file *os.File) (io.Reader, error) {

	convertToUTF8, exist := os.LookupEnv("CONVERT_TO_UTF8")
	if exist && convertToUTF8 == "false" {
		return file, nil
	} else {
		encoding, err := detectEncoing(file)
		fmt.Println(*encoding)
		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: %w", err)
		}

		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: failed to initiate converter for %v encoding %w", encoding, err)
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
