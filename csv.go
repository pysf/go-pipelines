package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

func processCsv(ctx context.Context, fileEventCh chan fileInfo) chan fileRow {

	resultCh := make(chan fileRow)
	sendResult := func(r *csvRow) {
		select {
		case <-ctx.Done():
			return
		case resultCh <- r:
			return
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
					sendResult(&csvRow{
						err: fileEvent.getError(),
					})
					break
				}

				sep := ','
				if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
					for _, r := range v {
						sep = r
					}
				}

				reader := csv.NewReader(fileEvent.file())
				reader.Comma = sep

				headers, err := reader.Read()
				if err != nil {
					if err != io.EOF {
						sendResult(&csvRow{
							err: wrapError(fmt.Errorf("parseCSV: failed to read %v file header %w", fileEvent.file().Name(), err)),
						})
					}
					break
				}

				lineCounter := 1
				for {
					data, err := reader.Read()

					if err == io.EOF {
						break
					}

					if err != nil {
						sendResult(&csvRow{
							err: wrapError(fmt.Errorf("parseCSV: failed to read %v file %w", fileEvent.file().Name(), err)),
						})
						break
					}
					lineCounter++

					row := make(map[string]string)
					for i, header := range headers {
						row[header] = data[i]
					}

					sendResult(&csvRow{
						line: lineCounter,
						data: row,
						file: fileEvent.file(),
					})
				}

				if fileEvent.getOnDone() != nil {
					f := *fileEvent.getOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}

type csvRow struct {
	err  error
	line int
	file *os.File
	data interface{}
	done *func()
}

func (ki *csvRow) getOnDone() *func() {
	return ki.done
}

func (ki *csvRow) lineNumber() int {
	return ki.line
}

func (ki *csvRow) fileName() string {
	return ki.file.Name()
}

func (ki *csvRow) getData() interface{} {
	return ki.data
}

func (ki *csvRow) getError() error {
	return ki.err
}
