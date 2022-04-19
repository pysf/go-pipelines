package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type fetchRequest struct {
	Bucket string
	Key    string
}

type fetchResponse struct {
	Err  error
	File *os.File
}

type fetchError struct {
	Inner      error
	Message    string
	Stacktrace string
	Misc       map[string]interface{}
}

func wrapError(err error) *fetchError {
	return &fetchError{
		Inner:      err,
		Stacktrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (f *fetchError) Error() string {
	return f.Inner.Error()
}

func fetch(ctx context.Context, fileEvents chan fileEvent) chan fileEvent {

	response := make(chan fileEvent)

	go func() {
		defer close(response)

		for {

			select {
			case <-ctx.Done():
				return
			case fEvent, ok := <-fileEvents:
				if !ok {
					return
				}

				if fEvent.err != nil {
					response <- fEvent
					return
				}

				sess, err := session.NewSession(&aws.Config{
					Region: aws.String("eu-central-1"),
				})
				if err != nil {
					fEvent.err = fmt.Errorf("fetch: failed to connect to aws %w", err)
					response <- fEvent
					return
				}

				key := fEvent.sqsMsg.S3.Object.Key
				bucket := fEvent.sqsMsg.S3.Bucket.Name
				downloader := s3manager.NewDownloader(sess)
				f, err := os.Create(fmt.Sprintf("%v-%v", time.Now(), key))

				if err != nil {
					fEvent.err = fmt.Errorf("fetch: failed to create a new tmp file %w", err)
					response <- fEvent
					return
				}

				n, err := downloader.Download(f, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})

				if err != nil {

					var s3Error awserr.Error

					if errors.As(err, &s3Error) {
						switch s3Error.Code() {
						case s3.ErrCodeNoSuchKey:
							fEvent.err = wrapError(fmt.Errorf("fetch: %v file not found %w ", key, err))
						case s3.ErrCodeNoSuchBucket:
							fEvent.err = wrapError(fmt.Errorf("fetch: %v bucket not found %w ", key, err))
						default:
							fEvent.err = fmt.Errorf("fetch: failed to download the file %v %w ", key, err)
						}

					} else {
						fEvent.err = fmt.Errorf("fetch: failed to download the file %v %w ", key, err)
					}

				} else {
					fmt.Printf("File downloaded, %d bytes\n", n)
					fEvent.file = f
					fEvent.processed = true
				}

				response <- fEvent

			}
		}
	}()

	return response
}
