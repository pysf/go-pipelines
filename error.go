package main

import "runtime/debug"

type pipelineError struct {
	Inner      error
	Message    string
	Stacktrace string
	Misc       map[string]interface{}
}

func wrapError(err error) *pipelineError {
	return &pipelineError{
		Inner:      err,
		Stacktrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (f *pipelineError) Error() string {
	return f.Inner.Error()
}
