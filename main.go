package main

import (
	"context"
	"fmt"
)

var QUEUE string = "s3-events"

func main() {

	ctx, _ := context.WithCancel(context.Background())

	fetchCh := fetch(ctx, getEvent(ctx, &QUEUE))

	for {

		select {
		case e, ok := <-fetchCh:
			if !ok {
				return
			}
			if e.err != nil {
				fmt.Printf("Error: %v \n", e.err)
				return
			}
			fmt.Printf("file: %v \n", e.file)
			fmt.Printf("processed: %v \n", e.processed)
			fmt.Printf("file name: %v \n", e.sqsMsg.S3.Object.Key)
		}

	}
}
