package pipeline

import (
	"io"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

type downloaderMock struct {
	s3manageriface.DownloaderAPI
	fileText string
}

func (sm downloaderMock) Download(destination io.WriterAt, s3Object *s3.GetObjectInput, fn ...func(*s3manager.Downloader)) (int64, error) {
	n, err := destination.WriteAt([]byte(sm.fileText), 0)
	if err != nil {
		return 0, err
	}
	return int64(n), err
}

func TestDownload(t *testing.T) {

	cases := []struct {
		fileContent string
		bucket      string
		key         string
		expectedRes []byte
		expectedErr error
	}{
		{
			fileContent: "hello",
			bucket:      "bucketName",
			key:         "test.csv",
			expectedRes: []byte("hello"),
			expectedErr: nil,
		},
	}

	for i, c := range cases {

		s3Client := s3Client{
			downloader: downloaderMock{
				fileText: c.fileContent,
			},
		}
		f, err := s3Client.fetch(c.bucket, c.key)

		if err != nil {
			t.Fatalf("case (%d) unexpected method called", i)
		}

		fileContent := make([]byte, len(c.expectedRes))
		_, err = f.Read(fileContent)

		if err != nil {
			t.Fatalf("case (%d) unexpected method called at reading file", i)
		}

		if !reflect.DeepEqual(fileContent, c.expectedRes) {
			t.Fatalf("case (%d) expecting %v got %v ", i, c.expectedRes, fileContent)
		}

	}

}
