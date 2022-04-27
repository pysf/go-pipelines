package main

import (
	"encoding/json"
	"runtime/debug"
)

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

func (pe *pipelineError) Error() string {
	return pe.Inner.Error()
}

func (pe *pipelineError) toJSON() ([]byte, error) {
	data := map[string]interface{}{
		"meta":    pe.Misc,
		"message": pe.Error(),
	}
	return json.Marshal(data)
}
