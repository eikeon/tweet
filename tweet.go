package tweet

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"runtime"
	"sync"
	"time"

	"code.google.com/p/snappy-go/snappy"
	"github.com/eikeon/funnelsort"
	"launchpad.net/goamz/s3"
)

type TweetWriter interface {
	Write(item funnelsort.Item)
	Close()
	Flush()
}

type tweetItem []byte

func (i tweetItem) Key() int64 {
	return int64(binary.LittleEndian.Uint64(i[0:8]))
}

func (i tweetItem) Value() []byte {
	l := binary.LittleEndian.Uint32(i[8:12])
	return i[12 : 12+l]
}

func (i tweetItem) Less(b funnelsort.Item) bool {
	return i.Key() < b.(tweetItem).Key()
}

func (i tweetItem) Bytes() []byte {
	return i
}

func (t tweetItem) Write(out io.Writer) {
	v := t.Value()
	t, err := snappy.Decode(nil, v)
	if err != nil {
		panic(err)
	}
	out.Write(t)
	out.Write([]byte("\n"))
}

func NewItem(b []byte) funnelsort.Item {
	l := binary.LittleEndian.Uint32(b[8:12])
	return tweetItem(b[0 : 12+l])
}

func keyTweet(tw *tweetItem) (rout *tweetItem) {
	et, err := snappy.Encode(nil, *tw)
	if err != nil {
		panic(err)
	}
	var tweet struct {
		Created_At string
	}
	if err := json.Unmarshal(*tw, &tweet); err == nil {
		if t, err := time.Parse(time.RubyDate, tweet.Created_At); err == nil {
			l := len(et)
			out := make(tweetItem, 12+l)

			key := t.UnixNano()
			out[0] = byte(key)
			out[1] = byte(key >> 8)
			out[2] = byte(key >> 16)
			out[3] = byte(key >> 24)
			out[4] = byte(key >> 32)
			out[5] = byte(key >> 40)
			out[6] = byte(key >> 48)
			out[7] = byte(key >> 56)

			out[8] = byte(l)
			out[9] = byte(l >> 8)
			out[10] = byte(l >> 16)
			out[11] = byte(l >> 24)

			copy(out[12:12+l], et)

			rout = &out
		} else {
			log.Fatal("Could not parse time:", err)
		}
	} else {
		log.Fatal("Could not unmarshal:", string(*tw))
	}
	return
}

func keyTweets(unkeyedTweets chan tweetItem, tweets chan tweetItem) {
	for tw := range unkeyedTweets {
		tweets <- *keyTweet(&tw)
	}
}

func NewTweetReader(reader *bufio.Reader) *TweetReader {
	unkeyedTweets := make(chan tweetItem, 4096)
	tweets := make(chan tweetItem, 4096)

	go func() {
		for {
			if b, isPrefix, err := reader.ReadLine(); err == nil {
				if isPrefix == true {
					log.Fatal("line too long")
				}
				l := len(b)
				tb := make(tweetItem, l)
				copy(tb, b)
				unkeyedTweets <- tb
			} else {
				if err == io.EOF {
					break
				} else {
					log.Fatal("tweet reader ERROR:", err)
				}
			}
		}
		close(unkeyedTweets)
	}()

	go func() {
		wg := sync.WaitGroup{}
		for i := runtime.NumCPU(); i > 0; i-- {
			wg.Add(1)
			go func() {
				keyTweets(unkeyedTweets, tweets)
				wg.Done()
			}()
		}
		wg.Wait()
		close(tweets)
	}()

	return &TweetReader{tweets}
}

func NewTweetReaderSerial(reader *bufio.Reader) *TweetReader {
	tweets := make(chan tweetItem, 4096)

	go func() {
		for {
			if b, isPrefix, err := reader.ReadLine(); err == nil {
				if isPrefix == true {
					log.Fatal("line too long")
				}
				ti := tweetItem(b)
				tweets <- *keyTweet(&ti)
			} else {
				if err == io.EOF {
					break
				} else {
					log.Fatal("tweet reader ERROR:", err)
				}
			}
		}
		close(tweets)
	}()

	return &TweetReader{tweets}
}

type TweetReader struct {
	tweets chan tweetItem
}

func (tr *TweetReader) Read() (item funnelsort.Item) {
	tw, ok := <-tr.tweets
	if ok {
		item = tw
	}
	return item
}

func Sort(r io.Reader, w TweetWriter) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	funnelsort.NewItem = NewItem

	br := bufio.NewReaderSize(r, 1<<24)
	in := NewTweetReader(br)
	funnelsort.FunnelSort(in, w)
}

func Merge(buffers []funnelsort.Buffer, out TweetWriter) {
	funnelsort.NewItem = NewItem
	funnelsort.Merge(buffers, out)
	out.Flush()
}

func GetReader(b *s3.Bucket, path string) (reader *TweetReader, err error) {
	body, err := b.GetReader(path)
	if err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(body)
	if err != nil {
		return nil, err
	}

	reader = NewTweetReaderSerial(bufio.NewReaderSize(gr, 16*4096))

	return reader, err
}

func GetBuffer(b *s3.Bucket, path string) (buffer funnelsort.Buffer, err error) {
	reader, err := GetReader(b, path)

	buffer = funnelsort.NewBuffer()
	for {
		if item := reader.Read(); item != nil {
			buffer.Write(item)
		} else {
			break
		}
	}
	return buffer, nil
}
