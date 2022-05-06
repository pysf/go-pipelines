package pipeline

import (
	"encoding/json"
	"runtime/debug"
)

type AppError struct {
	Inner      error
	Message    string
	Stacktrace string
	Misc       map[string]interface{}
}

func wrapError(err error) *AppError {
	return &AppError{
		Inner:      err,
		Stacktrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (pe *AppError) Error() string {
	return pe.Inner.Error()
}

func (pe *AppError) toJSON() ([]byte, error) {
	data := map[string]interface{}{
		"meta":    pe.Misc,
		"message": pe.Error(),
	}
	return json.Marshal(data)
}
