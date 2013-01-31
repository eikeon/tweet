package tweet

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"path"
	"sync"
	"time"

	"github.com/eikeon/funnelsort"
	"launchpad.net/goamz/s3"
)

type s3task struct {
	buf  *bytes.Buffer
	name string
}

type SplitTweetWriter struct {
	bucket  *s3.Bucket
	name    string
	current string
	out     *bytes.Buffer
	ch      chan s3task
	wg      sync.WaitGroup
}

func NewSplitTweetWriter(name string, bucket *s3.Bucket) TweetWriter {
	tw := &SplitTweetWriter{name: name, bucket: bucket}
	tw.ch = make(chan s3task, 1000)
	tw.wg = sync.WaitGroup{}
	for i := 10; i > 0; i-- {
		tw.wg.Add(1)
		go tw.upload()
	}
	return tw
}

func (w *SplitTweetWriter) tweetFile(tweet tweetItem) string {
	t := time.Unix(0, tweet.Key()).In(time.UTC)
	year := fmt.Sprintf("%04d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())
	day := fmt.Sprintf("%02d", t.Day())
	hour := fmt.Sprintf("%02d", t.Hour())
	filename := w.name + ".gz"
	return path.Join(year, month, day, hour, filename)
}

func (w *SplitTweetWriter) getWriter(tweet tweetItem) io.Writer {
	p := w.tweetFile(tweet)
	if p != w.current {
		w.Flush()
		w.current = p
		w.out = new(bytes.Buffer)
	}
	return w.out
}
func (w *SplitTweetWriter) Write(item funnelsort.Item) {
	ti := item.(tweetItem)
	ti.Write(w.getWriter(ti))
}

func (w *SplitTweetWriter) Flush() {
	if w.out != nil {
		w.ch <- s3task{w.out, w.current}
		w.out = nil
	}
}

func (w *SplitTweetWriter) Close() {
	w.Flush()
	close(w.ch)
	w.wg.Wait()
}

func (w *SplitTweetWriter) upload() {
	for b := range w.ch {
		out := new(bytes.Buffer)
		gw, err := gzip.NewWriterLevel(out, gzip.BestSpeed)
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(gw, bytes.NewReader(b.buf.Bytes()))
		if err != nil {
			log.Fatal("error copying to gzip writer", err)
		}
		gw.Close()

		data := out.Bytes()
	put:
		if err := w.bucket.PutReader(b.name, bytes.NewReader(data), int64(len(data)), "application/x-gzip", s3.Private); err != nil {
			log.Println("error writing to s3:", err)
			time.Sleep(time.Second)
			goto put
		}
	}
	w.wg.Done()
}
