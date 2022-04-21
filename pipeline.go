package main

import (
	"os"
)

type ParserStage interface {
	// getError() error
	// setError(error)
	getFile() *os.File
	setFile(*os.File)
}

type ParserStageEvent struct {
	file   *os.File
	err    error
	onDone func()
}

func (e *ParserStageEvent) getFile() *os.File {
	return e.file
}

func (e *ParserStageEvent) setFile(f *os.File) {
	e.file = f
}

// func (e *ParserStageEvent) getError() error {
// 	return e.err
// }

// func (e *ParserStageEvent) setError(err error) {
// 	e.err = err
// }
