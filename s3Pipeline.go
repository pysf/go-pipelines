package main

import (
	"os"
)

type s3PipelineEvent struct {
	f       *os.File
	err     error
	message rawSQSRecord
	onDone  func()
}

func (e *s3PipelineEvent) bucket() string {
	return e.message.S3.Bucket.Name
}

func (e *s3PipelineEvent) key() string {
	return e.message.S3.Object.Key
}

func (e *s3PipelineEvent) done() {
	e.onDone()
}

func (e *s3PipelineEvent) getError() error {
	return e.err
}

func (e *s3PipelineEvent) file() *os.File {
	return e.f
}

type fileProcessorEvent interface {
	lineNumber() int
	fileName() string
	getData() interface{}
	getError() error
}

type csvProcessorEvent struct {
	err  error
	line int
	file *os.File
	data interface{}
}

func (ki *csvProcessorEvent) lineNumber() int {
	return ki.line
}

func (ki *csvProcessorEvent) fileName() string {
	return ki.file.Name()
}

func (ki *csvProcessorEvent) getData() interface{} {
	return ki.data
}

func (ki *csvProcessorEvent) getError() error {
	return ki.err
}
