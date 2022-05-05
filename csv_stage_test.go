package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProcessCSV(t *testing.T) {

	cases := []struct {
		fileInfo   FileInfo
		csvContent string
		sep        rune
		expect     map[string]string
	}{
		{
			fileInfo: &S3File{
				f:        strings.NewReader("name;family;age\npayam;yousefi;38\n"),
				fileName: "test.csv",
			},
			sep: rune(';'),
			expect: map[string]string{
				"name":   "payam",
				"family": "yousefi",
				"age":    "38",
			},
		},
	}

	for i, c := range cases {
		csvProcessor := csvProcessor{
			sep: c.sep,
		}
		ctx, cancel := context.WithCancel(context.Background())

		fileInfoCh := make(chan FileInfo)
		resultCh := csvProcessor.ProcessCSV(ctx, fileInfoCh)

		go func() {
			fileInfoCh <- c.fileInfo
		}()

		select {
		case csvRow := <-resultCh:
			if !reflect.DeepEqual(csvRow.Data(), c.expect) {
				t.Fatalf("%d ,expected %v , got  %v", i, c.expect, csvRow)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("%d ,timedout!", i)
		}
		cancel()

	}

}
