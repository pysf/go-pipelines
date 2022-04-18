package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pysf/s3-to-kafka-go/pkg/s3"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetchRequest := make(chan s3.FetchRequest)
	response := s3.Fetch(ctx, fetchRequest)

	re := s3.FetchRequest{
		Bucket: "pysf-kafka-to-s3",
		Key:    "order_delivery_items.csv",
	}

	go func() {
		time.Sleep(4 * time.Second)
		cancel()
	}()

	go func() {
		for {
			fmt.Println("Sending Req...")
			time.Sleep(1 * time.Second)
			fetchRequest <- re
		}
	}()
	// msg := "There was an unexpected issue; please report this as a bug."

	for re := range response {
		if re.Err != nil {
			var fechErr *s3.FetchError
			errors.As(re.Err, &fechErr)
			fmt.Println(fechErr)
			fmt.Println(errors.Is(fechErr, &s3.FetchError{}))
			continue
		}

	}

}
