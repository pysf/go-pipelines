package main

import (
	"encoding/json"
	"runtime/debug"
)

type appError struct {
	Inner      error
	Message    string
	Stacktrace string
	Misc       map[string]interface{}
}

func wrapError(err error) *appError {
	return &appError{
		Inner:      err,
		Stacktrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (pe *appError) Error() string {
	return pe.Inner.Error()
}

func (pe *appError) toJSON() ([]byte, error) {
	data := map[string]interface{}{
		"meta":    pe.Misc,
		"message": pe.Error(),
	}
	return json.Marshal(data)
}
