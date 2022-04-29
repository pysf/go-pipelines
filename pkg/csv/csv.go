package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/pysf/go-pipelines/pkg/pipeline"
)

func Process(ctx context.Context, fileEventCh chan pipeline.FileInfo) chan pipeline.FileRow {

	resultCh := make(chan pipeline.FileRow)
	sendResult := func(r *CsvRow) {
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

				if fileEvent.GetError() != nil {
					sendResult(&CsvRow{
						err: fileEvent.GetError(),
					})
					break
				}

				sep := ','
				if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
					for _, r := range v {
						sep = r
					}
				}

				reader := csv.NewReader(fileEvent.File())
				reader.Comma = sep

				headers, err := reader.Read()
				if err != nil {
					if err != io.EOF {
						sendResult(&CsvRow{
							err: pipeline.WrapError(fmt.Errorf("parseCSV: failed to read %v file header %w", fileEvent.File().Name(), err)),
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
						sendResult(&CsvRow{
							err: pipeline.WrapError(fmt.Errorf("parseCSV: failed to read %v file %w", fileEvent.File().Name(), err)),
						})
						break
					}
					lineCounter++

					row := make(map[string]string)
					for i, header := range headers {
						row[header] = data[i]
					}

					sendResult(&CsvRow{
						line: lineCounter,
						data: row,
						file: fileEvent.File(),
					})
				}

				if fileEvent.GetOnDone() != nil {
					f := *fileEvent.GetOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}

type CsvRow struct {
	err  error
	line int
	file *os.File
	data interface{}
	done *func()
}

func (ki *CsvRow) GetOnDone() *func() {
	return ki.done
}

func (ki *CsvRow) LineNumber() int {
	return ki.line
}

func (ki *CsvRow) FileName() string {
	return ki.file.Name()
}

func (ki *CsvRow) GetData() interface{} {
	return ki.data
}

func (ki *CsvRow) GetError() error {
	return ki.err
}
