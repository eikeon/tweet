package tweet

import (
	"bufio"
	"compress/gzip"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/eikeon/funnelsort"
	"launchpad.net/goamz/s3"
)

type S3TweetWriter struct {
	bucket  *s3.Bucket
	name    string
	current string
	f       *os.File
	gw      *gzip.Writer
	out     *bufio.Writer
}

func NewS3TweetWriter(name string, bucket *s3.Bucket) TweetWriter {
	tw := &S3TweetWriter{name: name, bucket: bucket}

	f, err := os.OpenFile(tw.fileName(), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	tw.f = f
	gw, err := gzip.NewWriterLevel(tw.f, gzip.BestSpeed)
	if err != nil {
		panic(err)
	}
	tw.gw = gw
	tw.out = bufio.NewWriterSize(tw.gw, 16*4096)
	return tw
}

func (w *S3TweetWriter) fileName() string {
	return path.Join("/mnt", strings.Replace(w.name, "/", "-", -1))
}

func (w *S3TweetWriter) Write(item funnelsort.Item) {
	ti := item.(tweetItem)
	ti.Write(w.out)
}

func (w *S3TweetWriter) Close() {
	w.out.Flush()
	w.out = nil
}

func (w *S3TweetWriter) Flush() {
	w.Close()
	err := w.gw.Close()
	if err != nil {
		log.Fatal("error closing gzip:", err)
	}
	err = w.f.Close()
	if err != nil {
		log.Fatal("error closing file:", err)
	}

	file_name := w.fileName()

	fi, err := os.Stat(file_name)
	if err != nil {
		log.Fatal("error statting file:", err)
	}
retryPut:
	f, err := os.Open(file_name)
	if err != nil {
		log.Fatal("error opening file:", err)
	}
	reader := bufio.NewReader(f)
	length := fi.Size()

	err = w.bucket.PutReader(w.name, reader, length, "application/x-gzip", s3.Private)
	f.Close()
	if err != nil {
		log.Println("error writing to s3:", err)
		time.Sleep(1 * time.Second)
		goto retryPut
	}
	err = os.Remove(file_name)
	if err != nil {
		log.Println(err)
	}
}
