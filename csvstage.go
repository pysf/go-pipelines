package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
)

func NewCSVProcessor(sep rune) *csvProcessor {

	return &csvProcessor{
		sep: sep,
	}
}

type csvProcessor struct {
	sep rune
}

func (cp *csvProcessor) ProcessCSV(ctx context.Context, fileEventCh chan FileInfo) chan FileRow {

	resultCh := make(chan FileRow)
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

		for {
			select {
			case <-ctx.Done():
				return
			case fileInfo, ok := <-fileEventCh:
				if !ok {
					return
				}

				if fileInfo.GetError() != nil {
					sendResult(&csvRow{
						err: fileInfo.GetError(),
					})
					break
				}

				// sep := ','
				// if v, exist := os.LookupEnv("DEFAULT_CSV_SEPARATOR"); exist {
				// 	for _, r := range v {
				// 		sep = r
				// 	}
				// }

				reader := csv.NewReader(fileInfo.File())
				reader.Comma = cp.sep

				header, err := reader.Read()
				if err != nil {
					if err != io.EOF {
						sendResult(&csvRow{
							err: wrapError(fmt.Errorf("parseCSV: failed to read %v file header %w", fileInfo.FileName(), err)),
						})
					}
					break
				}

				lineCounter := 1

				for {

					line, err := reader.Read()
					if err != nil {
						if err != io.EOF {
							sendResult(&csvRow{
								err: wrapError(fmt.Errorf("parseCSV: failed to read %v file row %w", fileInfo.FileName(), err)),
							})
						}
						break
					}

					row := make(map[string]string)
					for i, header := range header {
						row[header] = line[i]
					}

					if line != nil {
						sendResult(&csvRow{
							line:     lineCounter,
							data:     row,
							fileName: fileInfo.FileName(),
						})
					}

				}

				if fileInfo.GetOnDone() != nil {
					f := *fileInfo.GetOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}

type csvRow struct {
	err      error
	line     int
	fileName string
	data     interface{}
	done     *func()
}

func (ki *csvRow) GetOnDone() *func() {
	return ki.done
}

func (ki *csvRow) LineNumber() int {
	return ki.line
}

func (ki *csvRow) Data() interface{} {
	return ki.data
}

func (ki *csvRow) GetError() error {
	return ki.err
}

func (e *csvRow) FileName() string {
	return e.fileName
}
