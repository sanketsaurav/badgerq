package badgerq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// message stuff copied from github.com/blueshift-labs/nsq/nsqd/message.go
const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func NewMessage(id MessageID, body []byte, timestamp time.Time) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: timestamp.UnixNano(),
	}
}

var NSQCutOffFunc = func(key []byte) bool {
	if len(key) < 8 {
		return true
	}

	return int64(binary.BigEndian.Uint64(key[:8]))/int64(time.Second) >
		time.Now().UnixNano()/int64(time.Second)
}

var NSQKeyExtractor = func(data []byte) ([]byte, error) {
	if len(data) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(data))
	}

	timestamp := make([]byte, 8)
	copy(timestamp, data[:8])
	messageID := data[10 : 10+MsgIDLength]

	return append(timestamp, messageID...), nil
}

var TestLogger = func(lvl LogLevel, f string, args ...interface{}) {
	msg := fmt.Sprintf("[%s] ", lvl) + fmt.Sprintf(f, args...)
	fmt.Println(msg)
}

var BenchLogger = func(lvl LogLevel, f string, args ...interface{}) {
	return
}

func TestNSQKeyExtractor(t *testing.T) {
	body := []byte("hello world")
	ts := time.Now()
	msg := NewMessage(MessageID(uuid.NewV4()), body, ts)
	var buf bytes.Buffer

	_, err := msg.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error serializing nsq message: %s", err)
	}

	key, err := NSQKeyExtractor(buf.Bytes())
	if err != nil {
		t.Fatalf("error extracking key from nsq message: %s", err)
	}

	if int64(binary.BigEndian.Uint64(key[:8])) != ts.UnixNano() {
		t.Fatalf("failed to extract the timestamp out of data")
	}
}

func NewMessageData(t testInterface, id uuid.UUID, data []byte, ts time.Time) []byte {
	msg := NewMessage(MessageID(id), data, ts)
	var buf bytes.Buffer

	_, err := msg.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error serializing nsq message: %s", err)
	}

	return buf.Bytes()
}

func NewDataBlob(size int) []byte {
	var buf bytes.Buffer
	for i := 0; i < size; i++ {
		buf.Write([]byte("x"))
	}
	return buf.Bytes()
}

type testInterface interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func testBadgerQ(t testInterface,
	bufSize int64, msgSize, totalCurWrites, totalFutureWrites, moreFutureMessages, totalCurReads int,
	idleWait, syncInterval time.Duration,
	logger func(lvl LogLevel, f string, args ...interface{})) {
	os.RemoveAll("tmp/test")
	defer os.RemoveAll("tmp/test")

	q := New("test", "tmp/test", bufSize, idleWait, syncInterval, logger, NSQKeyExtractor, NSQCutOffFunc)
	if q == nil {
		t.Fatalf("failed to start badgerq")
	}

	if q.Depth() != 0 {
		t.Fatalf("badgerq depth should be 0")
	}

	curWritesDone := make(chan struct{})
	futureWritesDone := make(chan struct{})
	curReadsDone := make(chan struct{})
	futureTime := time.Now().Add(30 * time.Second)

	logger(INFO, "start writing future messages")
	go func() {
		var futureWriteWg sync.WaitGroup
		futureWriteWg.Add(totalFutureWrites)
		for i := 0; i < totalFutureWrites; i++ {
			go func() {
				defer futureWriteWg.Done()
				msg := NewMessageData(t, uuid.NewV4(), NewDataBlob(msgSize), futureTime)
				if err := q.Put(msg); err != nil {
					t.Errorf("error putting data to badgerq: %s", err)
				}
			}()
		}
		futureWriteWg.Wait()
		close(futureWritesDone)
	}()

	logger(INFO, "start writing current messages")
	go func() {
		var curWriteWg sync.WaitGroup
		curWriteWg.Add(totalCurWrites)
		for i := 0; i < totalCurWrites; i++ {
			go func() {
				defer curWriteWg.Done()
				msg := NewMessageData(t, uuid.NewV4(), NewDataBlob(msgSize), time.Now())
				if err := q.Put(msg); err != nil {
					t.Errorf("error putting data to badgerq: %s", err)
				}
			}()
		}
		curWriteWg.Wait()
		close(curWritesDone)
	}()

	// all cur reads should be before future timestamp
	logger(INFO, "start reading current messages")
	go func() {
		msgs := map[string]bool{}
		for i := 0; i < totalCurReads; i++ {
			msg := <-q.ReadChan()
			key, err := NSQKeyExtractor(msg)
			if err != nil {
				t.Errorf("error extracting key from msg: %s", err)
			}

			if int64(binary.BigEndian.Uint64(key[:8])) >= futureTime.UnixNano() {
				t.Errorf("future message was received")
			}

			var msgID [16]byte
			copy(msgID[:], key[8:])
			if msgs[uuid.UUID(msgID).String()] {
				t.Errorf("duplicated messages received")
			} else {
				msgs[uuid.UUID(msgID).String()] = true
			}
		}

		close(curReadsDone)
	}()

	<-futureWritesDone
	logger(INFO, "finish writing future messages")
	<-curWritesDone
	logger(INFO, "finish writing current messages")
	<-curReadsDone
	logger(INFO, "finish reading current messages")

	// wait for read loop to decrease the counter for consumed messages
	<-time.After(time.Second)

	// there should be 100 future writes on disk, 180 cur reads finished, 10 cur reads in buffer and 10 cur reads on disk
	expectedDepth := int64(totalFutureWrites + totalCurWrites - totalCurReads)
	curDepth := q.Depth()
	if curDepth != expectedDepth {
		t.Fatalf("badgerq depth should be %d instead of %d", expectedDepth, curDepth)
	}

	// closing the badgerq should save the buffered/inflight msg back on disk
	if err := q.Close(); err != nil {
		t.Fatalf("error closing badgerq")
	}

	// read back the depth should be the same as it was before close
	q = New("test", "tmp/test", bufSize, idleWait, syncInterval, logger, NSQKeyExtractor, NSQCutOffFunc)
	curDepth = q.Depth()
	if curDepth != expectedDepth {
		t.Fatalf("badgerq depth should be %d instead of %d", expectedDepth, curDepth)
	}

	// flush out the rest of cur reads
	logger(INFO, "flushing rest of current messages")
	msgs := map[string]bool{}
	for i := 0; i < totalCurWrites-totalCurReads; i++ {
		msg := <-q.ReadChan()
		key, err := NSQKeyExtractor(msg)
		if err != nil {
			t.Fatalf("error extracting key from msg: %s", err)
		}

		if int64(binary.BigEndian.Uint64(key[:8])) >= futureTime.UnixNano() {
			t.Fatalf("future message were received")
		}

		var msgID [16]byte
		copy(msgID[:], key[8:])
		if msgs[uuid.UUID(msgID).String()] {
			t.Errorf("duplicated messages received")
		} else {
			msgs[uuid.UUID(msgID).String()] = true
		}
	}

	// should halt at reading future message
	logger(INFO, "halt at reading future messages")
	select {
	case <-time.After(2 * idleWait):
	case <-q.ReadChan():
		t.Fatalf("reading should halt for future messages")
	}

	// try empty out the badgerq
	if err := q.Empty(); err != nil {
		t.Fatalf("error emptying badgerq: %s", err)
	}

	if q.Depth() != 0 {
		t.Fatalf("empty badgerq should have depth of 0")
	}

	// should still be OK to write for both current and future
	logger(INFO, "writing more future messages")
	for i := 0; i < moreFutureMessages; i++ {
		msg := NewMessageData(t, uuid.NewV4(), NewDataBlob(msgSize), futureTime)
		if err := q.Put(msg); err != nil {
			t.Fatalf("error putting data to badgerq: %s", err)
		}
	}

	logger(INFO, "writing more current messages")
	msg := NewMessageData(t, uuid.NewV4(), NewDataBlob(msgSize), time.Now())
	if err := q.Put(msg); err != nil {
		t.Fatalf("error putting data to badgerq: %s", err)
	}

	// should be able to read for current message
	logger(INFO, "reading more current messages")
	msg = <-q.ReadChan()
	key, err := NSQKeyExtractor(msg)
	if err != nil {
		t.Fatalf("error extracting key from msg: %s", err)
	}
	if int64(binary.BigEndian.Uint64(key[:8])) >= futureTime.UnixNano() {
		t.Fatalf("future message were received")
	}

	// should halt at reading future message
	logger(INFO, "halt at reading future messages")
	select {
	case <-time.After(2 * idleWait):
	case <-q.ReadChan():
		t.Fatalf("reading should halt for future messages")
	}

	// depth should be equal to moreFutureMessages
	curDepth = q.Depth()
	if curDepth != int64(moreFutureMessages) {
		t.Fatalf("badgerq depth should be %d instead of %d", moreFutureMessages, curDepth)
	}

	if err := q.Delete(); err != nil {
		t.Fatalf("failed to delete the badgerq: %s", err)
	}

	// reopen it and see if the data is emptied
	q = New("test", "tmp/test", bufSize, idleWait, syncInterval, logger, NSQKeyExtractor, NSQCutOffFunc)
	defer q.Delete()
	if q.Depth() != 0 {
		t.Fatalf("empty badgerq should have depth of 0")
	}
}

func TestBadgerQ(t *testing.T) {
	for n := 0; n < 50; n++ {
		bufSize := rand.Int63n(1000)
		msgSize := rand.Intn(200000) + 400000     // 0.4MB - 0.6MB
		totalFutureWrites := rand.Intn(100) + 100 // 100 - 200
		totalCurWrites := rand.Intn(300) + 100    // 100 - 400
		moreFutureMessages := rand.Intn(50) + 50  // 50 - 100
		totalCurReads := rand.Intn(totalCurWrites)
		idleWait := time.Duration(rand.Intn(900)+100) * time.Millisecond     // 100ms - 1s
		syncInterval := time.Duration(rand.Intn(900)+100) * time.Millisecond // 100ms - 1s

		testBadgerQ(t, bufSize, msgSize, totalCurWrites, totalFutureWrites, moreFutureMessages, totalCurReads, idleWait, syncInterval, TestLogger)
	}
}

func benchmarkBadgerQPut(b *testing.B, msgSize int) {
	os.RemoveAll("tmp/test")
	defer os.RemoveAll("tmp/test")

	q := New("test", "tmp/test", 1000, 50*time.Millisecond, 60*time.Second, BenchLogger, NSQKeyExtractor, NSQCutOffFunc)
	defer q.Delete()

	go func() {
		for _ = range q.ReadChan() {
		}
	}()

	msgBlob := NewDataBlob(msgSize)
	for n := 0; n < b.N; n++ {
		msg := NewMessageData(b, uuid.NewV4(), msgBlob, time.Now())
		if err := q.Put(msg); err != nil {
			b.Errorf("error putting data to badgerq: %s", err)
		}
		b.SetBytes(int64(len(msg)))
	}
}

// 1kb
func BenchmarkBadgerQPut1024(b *testing.B) {
	benchmarkBadgerQPut(b, 1024)
}

// 5kb
func BenchmarkBadgerQPut5120(b *testing.B) {
	benchmarkBadgerQPut(b, 5120)
}

// 10kb
func BenchmarkBadgerQPut10240(b *testing.B) {
	benchmarkBadgerQPut(b, 10240)
}

// 50kb
func BenchmarkBadgerQPut51200(b *testing.B) {
	benchmarkBadgerQPut(b, 51200)
}

// 100kb
func BenchmarkBadgerQPut102400(b *testing.B) {
	benchmarkBadgerQPut(b, 102400)
}

// 500kb
func BenchmarkBadgerQPut512000(b *testing.B) {
	benchmarkBadgerQPut(b, 512000)
}

// 1MB
func BenchmarkBadgerQPut1024000(b *testing.B) {
	benchmarkBadgerQPut(b, 1024000)
}

func benchmarkBadgerQRead(b *testing.B, msgSize int) {
	os.RemoveAll("tmp/test")
	defer os.RemoveAll("tmp/test")

	q := New("test", "tmp/test", 1000, 50*time.Millisecond, 60*time.Second, BenchLogger, NSQKeyExtractor, NSQCutOffFunc)
	defer q.Delete()

	msgBlob := NewDataBlob(msgSize)
	now := time.Now()
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)

		for {
			select {
			case <-stop:
				return
			default:
				msg := NewMessageData(b, uuid.NewV4(), msgBlob, now)
				q.Put(msg)
			}
		}
	}()

	for n := 0; n < b.N; n++ {
		data := <-q.ReadChan()
		b.SetBytes(int64(len(data)))
	}

	close(stop)
	<-stopped
}

// 1kb
func BenchmarkBadgerQRead1024(b *testing.B) {
	benchmarkBadgerQRead(b, 1024)
}

// 5kb
func BenchmarkBadgerQRead5120(b *testing.B) {
	benchmarkBadgerQRead(b, 5120)
}

// 10kb
func BenchmarkBadgerQRead10240(b *testing.B) {
	benchmarkBadgerQRead(b, 10240)
}

// 50kb
func BenchmarkBadgerQRead51200(b *testing.B) {
	benchmarkBadgerQRead(b, 51200)
}

// 100kb
func BenchmarkBadgerQRead102400(b *testing.B) {
	benchmarkBadgerQRead(b, 102400)
}

// 500kb
func BenchmarkBadgerQRead512000(b *testing.B) {
	benchmarkBadgerQRead(b, 512000)
}

// 1MB
func BenchmarkBadgerQRead1024000(b *testing.B) {
	benchmarkBadgerQRead(b, 1024000)
}
