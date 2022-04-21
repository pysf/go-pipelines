package main

import (
	"context"
	"fmt"
)

var QUEUE string = "s3-events"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parserEventCh := read(ctx, fetch(ctx, sqsEvents(ctx, &QUEUE)))

	for parserEvent := range parserEventCh {
		parserStageEvent := parserEvent.(*ParserStageEvent)
		if parserStageEvent.err != nil {
			fmt.Printf("Error: %v \n", parserStageEvent.err)
			continue
		}
		fmt.Printf("file name: %v \n", parserStageEvent.file.Name())

	}
}
