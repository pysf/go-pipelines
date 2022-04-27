package main

import "os"

type finalStageEvent interface {
	getError() error
}

type fileInfo interface {
	file() *os.File
	getError() error
	done()
}

type fileRow interface {
	lineNumber() int
	fileName() string
	getData() interface{}
	getError() error
}

type s3Notification interface {
	bucket() string
	key() string
	getError() error
	done()
}
