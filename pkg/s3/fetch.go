package s3

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pysf/go-pipelines/pkg/pipeline"
)

func Fetch(ctx context.Context, s3NotificationCh chan pipeline.S3Notification) chan pipeline.FileInfo {

	resultCh := make(chan pipeline.FileInfo)

	sendResult := func(r *s3File) {
		select {
		case <-ctx.Done():
			return
		case resultCh <- r:
			return
		}
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("S3 closing")

		for {

			select {
			case <-ctx.Done():
				return
			case sqsMsg, ok := <-s3NotificationCh:
				if !ok {
					return
				}

				if sqsMsg.GetError() != nil {
					sendResult(&s3File{
						err: sqsMsg.GetError(),
					})
					break
				}

				file, err := download(sqsMsg.Bucket(), sqsMsg.Key())

				sendResult(&s3File{
					f:   file,
					err: err,
				})

				if sqsMsg.GetOnDone() != nil {
					f := *sqsMsg.GetOnDone()
					f()
				}
			}
		}
	}()

	return resultCh
}

func download(bucket, key string) (*os.File, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		return nil, fmt.Errorf("download: failed to connect to aws %w", err)
	}

	downloader := s3manager.NewDownloader(sess)
	f, err := ioutil.TempFile("", fmt.Sprintf("%v-*", key))
	if err != nil {
		return nil, fmt.Errorf("download: failed to create a new tmp file %w", err)
	}

	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		os.Remove(f.Name())
		var s3Error awserr.Error

		if errors.As(err, &s3Error) {
			switch s3Error.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, pipeline.WrapError(fmt.Errorf("download: %v file not found %w ", key, err))
			case s3.ErrCodeNoSuchBucket:
				return nil, pipeline.WrapError(fmt.Errorf("download: %v bucket not found %w ", key, err))
			default:
				return nil, fmt.Errorf("download: failed to download the file %v %w ", key, err)
			}

		} else {
			return nil, fmt.Errorf("download: failed to download the file %v %w ", key, err)
		}

	}

	return f, nil
}

type s3File struct {
	f    *os.File
	err  error
	done *func()
}

func (e *s3File) GetOnDone() *func() {
	return e.done
}

func (e *s3File) GetError() error {
	return e.err
}

func (e *s3File) File() *os.File {
	return e.f
}
