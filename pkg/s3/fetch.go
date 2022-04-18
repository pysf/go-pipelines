package s3

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

type FetchRequest struct {
	Bucket string
	Key    string
}

type FetchResponse struct {
	Err  error
	File *os.File
}

type FetchError struct {
	Inner      error
	Message    string
	Stacktrace string
	Misc       map[string]interface{}
}

func WrapError(err error) *FetchError {
	return &FetchError{
		Inner:      err,
		Stacktrace: string(debug.Stack()),
		Misc:       make(map[string]interface{}),
	}
}

func (f *FetchError) Error() string {
	return f.Inner.Error()
}

func Fetch(ctx context.Context, fetchRequests chan FetchRequest) chan FetchResponse {

	response := make(chan FetchResponse)

	go func() {
		defer close(response)

		for {

			select {
			case <-ctx.Done():
				return
			case <-fetchRequests:
				fr := <-fetchRequests

				sess, err := session.NewSession(&aws.Config{
					Region: aws.String("eu-central-1"),
				})
				if err != nil {
					response <- FetchResponse{
						Err: WrapError(fmt.Errorf("Fetch: Failed to connect to aws %w", err)),
					}
					return
				}

				downloader := s3manager.NewDownloader(sess)
				f, err := os.Create(fmt.Sprintf("%v-%v.txt", time.Now(), fr.Key))

				if err != nil {
					response <- FetchResponse{
						Err: WrapError(fmt.Errorf("Fetch: Failed to create a new tmp file %w", err)),
					}
					return
				}

				n, err := downloader.Download(f, &s3.GetObjectInput{
					Bucket: aws.String(fr.Bucket),
					Key:    aws.String(fr.Key),
				})

				if err != nil {

					var s3Error awserr.Error

					if errors.As(err, &s3Error) {
						switch s3Error.Code() {
						case s3.ErrCodeNoSuchKey:
							response <- FetchResponse{
								Err: WrapError(fmt.Errorf("Fetch: %v file not found %w ", fr.Key, err)),
							}
						case s3.ErrCodeNoSuchBucket:
							response <- FetchResponse{
								Err: WrapError(fmt.Errorf("Fetch: %v bucket not found %w ", fr.Key, err)),
							}
						default:
							response <- FetchResponse{
								Err: WrapError(fmt.Errorf("Fetch: failed to download the file %v %w ", fr.Key, err)),
							}
							return
						}

					} else {
						response <- FetchResponse{
							Err: WrapError(fmt.Errorf("Fetch: failed to download the file %v %w ", fr.Key, err)),
						}
						return
					}

				}

				fmt.Printf("File downloaded, %d bytes\n", n)

				response <- FetchResponse{
					File: f,
				}

			}
		}
	}()

	return response
}
