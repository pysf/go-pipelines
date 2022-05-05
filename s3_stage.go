package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

func NewS3Stage(conf *aws.Config) s3Stage {
	sess := session.Must(session.NewSession())

	downloader := s3manager.NewDownloader(sess)

	s3Client := s3Client{
		downloader,
	}

	return s3Stage{
		s3Client,
	}
}

type s3Stage struct {
	client s3Client
}

func (stg s3Stage) FetchFile(ctx context.Context, s3NotificationCh chan S3Notification) chan FileInfo {

	resultCh := make(chan FileInfo)

	sendResult := func(r *S3File) {
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
					sendResult(&S3File{
						err: sqsMsg.GetError(),
					})
					break
				}
				fmt.Println(sqsMsg.Bucket())
				fmt.Println(sqsMsg.Key())

				file, err := stg.client.fetch(sqsMsg.Bucket(), sqsMsg.Key())

				if err != nil {
					sendResult(&S3File{
						err: err,
					})
					break
				}

				sendResult(&S3File{
					f:        file,
					fileName: file.Name(),
					err:      err,
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

type s3Client struct {
	downloader s3manageriface.DownloaderAPI
}

func (s *s3Client) fetch(bucket, key string) (*os.File, error) {

	tmpf, err := ioutil.TempFile("", fmt.Sprintf("%v-*", key))
	if err != nil {
		return nil, fmt.Errorf("download: failed to create a new tmp file %w", err)
	}

	_, err = s.downloader.Download(tmpf, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		// os.Remove(tmpf.Name())
		var s3Error awserr.Error

		if errors.As(err, &s3Error) {
			switch s3Error.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, wrapError(fmt.Errorf("download: %v file not found %w ", key, err))
			case s3.ErrCodeNoSuchBucket:
				return nil, wrapError(fmt.Errorf("download: %v bucket not found %w ", key, err))
			default:
				return nil, fmt.Errorf("download: failed to download the file %v %w ", key, err)
			}

		} else {
			return nil, fmt.Errorf("download: failed to download the file %v %w ", key, err)
		}

	}

	return tmpf, nil
}

type S3File struct {
	f        io.Reader
	err      error
	fileName string
	done     *func()
}

func (f *S3File) GetOnDone() *func() {
	return f.done
}

func (f *S3File) GetError() error {
	return f.err
}

func (f *S3File) FileName() string {
	return f.fileName
}

func (f *S3File) File() io.Reader {
	return f.f
}
