package csv

import (
	"os"
)

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
