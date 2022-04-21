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

func read(ctx context.Context, parserStageCh chan ParserStage) chan ParserStage {

	resultCh := make(chan ParserStage)
	go func() {
		defer close(resultCh)

		for {
			select {
			case <-ctx.Done():
				return
			case parserStage, ok := <-parserStageCh:
				if !ok {
					return
				}

				parserStageEvent := parserStage.(*ParserStageEvent)
				if parserStageEvent.err != nil {
					resultCh <- parserStage
					break
				}
				parserStageEvent.onDone()
				processFile(parserStageEvent)
				resultCh <- parserStage
			}
		}

	}()

	return resultCh
}

func processFile(parserStageEvent *ParserStageEvent) error {

	defaultSeparator := ','
	if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
		for _, r := range v {
			defaultSeparator = r
		}
	}

	encodingReader, err := getEncodingReader(parserStageEvent)
	if err != nil {
		return fmt.Errorf("processFile: failed to create encoding Reader %w", err)
	}

	csvReader := csv.NewReader(encodingReader)
	csvReader.Comma = defaultSeparator

	headers, err := csvReader.Read()

	if err != nil {
		if err != io.EOF {
			return err
		}
	}

	for {
		data, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		row := make(map[string]string)
		for i, header := range headers {
			row[header] = data[i]
		}

		fmt.Println(row)
		// send line to kafka
		// set process status
	}
	return nil

}

func getEncodingReader(parserStageEvent *ParserStageEvent) (io.Reader, error) {
	f := parserStageEvent.file

	convertToUTF8, exist := os.LookupEnv("CONVERT_TO_UTF8")
	if exist && convertToUTF8 == "false" {
		return f, nil
	} else {
		encoding, err := detectEncoing(f)
		fmt.Println(*encoding)
		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: %w", err)
		}

		if err != nil {
			return nil, fmt.Errorf("getEncodingReader: failed to initiate converter for %v encoding %w", encoding, err)
		}
		return iconv.NewReader(f, *encoding, "utf-8")
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
