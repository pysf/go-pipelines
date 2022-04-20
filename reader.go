package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	"github.com/djimenez/iconv-go"
	"github.com/gogs/chardet"
)

func read(ctx context.Context, pipeEventCh chan pipelineEvent) chan pipelineEvent {

	resultCh := make(chan pipelineEvent)
	go func() {
		defer close(resultCh)

		for {
			select {
			case <-ctx.Done():
				return
			case pipeEvent, ok := <-pipeEventCh:
				if !ok {
					return
				}

				if pipeEvent.err != nil {
					resultCh <- pipeEvent
					break
				}

				processFile(&pipeEvent)
				resultCh <- pipeEvent
			}
		}

	}()

	return resultCh
}

func processFile(pipeEvent *pipelineEvent) {

	defaultSeparator := ','
	if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
		r, _ := utf8.DecodeRuneInString(v)
		defaultSeparator = r
	}

	encodingReader, err := getEncodingReader(pipeEvent)
	if err != nil {
		pipeEvent.err = fmt.Errorf("processFile: failed to create encoding Reader %w", err)
		return
	}

	csvReader := csv.NewReader(encodingReader)
	csvReader.Comma = defaultSeparator

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(record)
		// send line to kafka
		// set process status
	}

}

func getEncodingReader(pipeEvent *pipelineEvent) (io.Reader, error) {
	f, err := os.Open(pipeEvent.file.Name())

	if err != nil {
		return nil, fmt.Errorf("getEncodingReader: failed to open the file %v %w", err, pipeEvent.file.Name())
	}

	convertToUTF8, exist := os.LookupEnv("CONVERT_TO_UTF8")
	if exist && convertToUTF8 == "false" {
		return f, err
	} else {
		encoding, err := detectEncoing(pipeEvent.file)
		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: %w", err)
		}

		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: failed to initiate converter for %v encoding %w", err, encoding)
		}
		return iconv.NewReader(f, *encoding, "utf-8")
	}
}

func detectEncoing(f *os.File) (*string, error) {
	f, err := os.Open(f.Name())
	defer f.Close()
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
