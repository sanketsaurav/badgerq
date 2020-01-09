package badgerq

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	pb "github.com/dgraph-io/badger/v2/pb"
)

const GCDiscardRatio = 0.5

var GCInterval = 1 * time.Minute

// logging stuff copied from github.com/blueshift-labs/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

// logging stuff copied from github.com/dgraph-io/badger/logger.go
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

func (l AppLogFunc) Errorf(msg string, args ...interface{}) {
	l(ERROR, msg, args...)
}

func (l AppLogFunc) Warningf(msg string, args ...interface{}) {
	l(WARN, msg, args...)
}

func (l AppLogFunc) Infof(msg string, args ...interface{}) {
	l(INFO, msg, args...)
}

func (l AppLogFunc) Debugf(msg string, args ...interface{}) {
	l(DEBUG, msg, args...)
}

// logging stuff copied from github.com/blueshift-labs/nsq/nsqd/backend_queue.go
type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type badgerq struct {
	name         string
	dir          string
	depth        int64
	db           *badger.DB
	logger       AppLogFunc
	readChan     chan []byte
	bufferSize   int
	buffer       chan []byte
	idleWait     time.Duration
	stop         chan struct{}
	gcStopped    chan struct{}
	readStopped  chan struct{}
	scanStopped  chan struct{}
	keyExtractor func([]byte) ([]byte, error)
	cutOffFunc   func([]byte) bool
	emptyLock    sync.RWMutex
}

func New(name, dir string, bufferSize int, idleWait time.Duration, logger AppLogFunc,
	keyExtractor func([]byte) ([]byte, error),
	cutOffFunc func([]byte) bool) *badgerq {
	if err := os.MkdirAll(dir, 0774); err != nil {
		logger(ERROR, "BADGERQ(%s) failed to open - %s", name, err)
		return nil
	}

	db, err := badger.Open(
		badger.DefaultOptions(dir).
			WithSyncWrites(true).
			WithTruncate(true).
			WithLogger(logger))
	if err != nil {
		logger(ERROR, "BADGERQ(%s) failed to open - %s", name, err)
		return nil
	}
	q := &badgerq{
		name:         name,
		dir:          dir,
		db:           db,
		logger:       logger,
		readChan:     make(chan []byte),
		bufferSize:   bufferSize,
		buffer:       make(chan []byte, bufferSize),
		idleWait:     idleWait,
		stop:         make(chan struct{}),
		gcStopped:    make(chan struct{}),
		readStopped:  make(chan struct{}),
		scanStopped:  make(chan struct{}),
		keyExtractor: keyExtractor,
		cutOffFunc:   cutOffFunc,
	}

	if err := q.retrieveMetaData(); err != nil {
		logger(ERROR, "BADGERQ(%s) failed to retrieveMetaData - %s", name, err)
		db.Close()
		return nil
	}
	logger(INFO, "BADGERQ(%s) current depth: %d", name, atomic.LoadInt64(&q.depth))

	go q.gcLoop()
	go q.scanLoop()
	go q.readLoop()
	logger(INFO, "BADGERQ(%s) successfully opened", name)
	return q
}

func (q *badgerq) Depth() int64 {
	return int64(atomic.LoadInt64(&q.depth))
}

func (q *badgerq) Put(data []byte) (err error) {
	q.emptyLock.RLock()
	defer q.emptyLock.RUnlock()

	defer func() {
		if err == nil {
			atomic.AddInt64(&q.depth, 1)
		}
	}()

	var key []byte
	key, err = q.keyExtractor(data)
	if err != nil {
		return
	}

	return q.db.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(&badger.Entry{
			Key:   key,
			Value: data,
		})
	})
}

func (q *badgerq) ReadChan() <-chan []byte {
	return q.readChan
}

func (q *badgerq) readLoop() {
	defer close(q.readStopped)

	var data []byte
	defer func() {
		if data != nil {
			if err := q.Put(data); err != nil {
				q.logger(ERROR, "BADGERQ(%s) failed to put back unconsumed data - %s", q.name, err)
			}
		}
	}()

	for {
		select {
		case <-q.stop:
			return
		case data = <-q.buffer:
			select {
			case <-q.stop:
				return
			case q.readChan <- data:
				atomic.AddInt64(&q.depth, -1)
				data = nil
			}
		}
	}
}

func (q *badgerq) scanLoop() {
	defer close(q.scanStopped)

	for {
		select {
		case <-q.stop:
			return
		case <-time.After(q.idleWait):
			q.db.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = q.bufferSize
				it := txn.NewIterator(opts)
				defer it.Close()

				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					key := item.KeyCopy(nil)
					if q.cutOffFunc(key) {
						break
					}

					if data, err := item.ValueCopy(nil); err != nil {
						q.logger(ERROR, "BADGERQ(%s) error reading data - %s", q.name, err)
						break
					} else {
						select {
						case <-q.stop:
							return nil
						case <-time.After(q.idleWait):
							return nil
						case q.buffer <- data:
							if err := txn.Delete(key); err != nil {
								q.logger(ERROR, "BADGERQ(%s) failed to delete buffered data - %s", q.name, err)
								return nil
							}
						}
					}
				}

				return nil
			})

			// q.db.Sync()
		}
	}
}

func (q *badgerq) gcLoop() {
	defer close(q.gcStopped)

	for {
		select {
		case <-q.stop:
			return
		case <-time.After(GCInterval):
		again:
			if err := q.db.RunValueLogGC(GCDiscardRatio); err != nil {
				if err != badger.ErrNoRewrite {
					q.logger(ERROR, "BADGERQ(%s) failed to run value log gc - %s", q.name, err)
				}
			} else {
				goto again
			}
		}
	}
}

func (q *badgerq) Close() error {
	close(q.stop)
	<-q.scanStopped
	<-q.readStopped
	<-q.gcStopped
	close(q.buffer)
	close(q.readChan)

	wb := q.db.NewWriteBatch()
	for data := range q.buffer {
		key, err := q.keyExtractor(data)
		if err != nil {
			q.logger(ERROR, "BADGERQ(%s) failed to put back unconsumed data - %s", q.name, err)
		} else {
			err = wb.Set(key, data)
			if err != nil {
				q.logger(ERROR, "BADGERQ(%s) failed to put back unconsumed data - %s", q.name, err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		q.logger(ERROR, "BADGERQ(%s) failed to flush unconsumed data - %s", q.name, err)
	}
	wb.Cancel()

	// if err := q.db.Sync(); err != nil {
	// 	q.logger(ERROR, "BADGERQ(%s) failed to sync data to disk - %s", q.name, err)
	// }

	q.logger(INFO, "BADGERQ(%s) closed", q.name)
	return q.db.Close()
}

func (q *badgerq) Delete() error {
	close(q.stop)
	<-q.scanStopped
	<-q.readStopped
	<-q.gcStopped
	close(q.buffer)
	close(q.readChan)

	err := q.db.DropAll()
	_err := q.db.Close()

	q.logger(INFO, "BADGERQ(%s) deleted", q.name)
	if err != nil {
		return err
	}
	return _err
}

func (q *badgerq) Empty() error {
	close(q.stop)
	<-q.scanStopped
	<-q.readStopped
	<-q.gcStopped
	close(q.buffer)

	q.buffer = make(chan []byte, q.bufferSize)
	q.stop = make(chan struct{})
	q.gcStopped = make(chan struct{})
	q.readStopped = make(chan struct{})
	q.scanStopped = make(chan struct{})

	q.emptyLock.Lock()
	defer q.emptyLock.Unlock()

	err := q.db.DropAll()
	if err != nil {
		q.logger(ERROR, "BADGERQ(%s) failed to drop all data - %s", q.name, err)
	}

	_err := q.retrieveMetaData()
	if _err != nil {
		q.logger(ERROR, "BADGERQ(%s) failed to retrieveMetaData - %s", q.name, _err)
	}

	go q.gcLoop()
	go q.scanLoop()
	go q.readLoop()
	q.logger(INFO, "BADGERQ(%s) emptied", q.name)

	if err != nil {
		return err
	}
	return _err
}

func (q *badgerq) retrieveMetaData() error {
	atomic.StoreInt64(&q.depth, 0)
	stream := q.db.NewStream()
	stream.NumGo = runtime.NumCPU()
	stream.LogPrefix = fmt.Sprintf("BADGERQ(%s) Streaming: ", q.name)

	stream.Send = func(list *pb.KVList) error {
		atomic.AddInt64(&q.depth, int64(len(list.Kv)))
		return nil
	}

	return stream.Orchestrate(context.Background())
}
