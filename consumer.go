package clickhouse_20200328

import (
	"context"
	"errors"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type consumer struct {
	connPtr **clickhouse.Conn
	rowsCh  <-chan string
	cfg     clickHouseConfig
	logger  *logp.Logger

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// concurrency control
	maxConcurrency int
	semaphore      chan struct{}
}

// newConsumer creates a consumer that will read rows from rowsCh and write to ClickHouse.
// connPtr is a pointer to the client's *clickhouse.Conn so the consumer can access the connection when available.
func newConsumer(connPtr **clickhouse.Conn, rowsCh <-chan string, cfg clickHouseConfig, logger *logp.Logger) *consumer {
	c := &consumer{
		connPtr:        connPtr,
		rowsCh:         rowsCh,
		cfg:            cfg,
		logger:         logger,
		stopCh:         make(chan struct{}),
		maxConcurrency: cfg.CkWriteMaxConcurrency,
		semaphore:      make(chan struct{}, cfg.CkWriteMaxConcurrency),
	}

	c.wg.Add(1)
	go c.run()
	return c
}

func (c *consumer) run() {
	defer c.wg.Done()

	// buffer for batching
	batchBuf := make([]string, 0, c.cfg.CkWriteBatchSize)
	flushTicker := time.NewTicker(10 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-c.stopCh:
			// flush remaining
			if len(batchBuf) > 0 {
				_ = c.flush(batchBuf)
			}
			return
		case row, ok := <-c.rowsCh:
			if !ok {
				// channel closed, flush and exit
				if len(batchBuf) > 0 {
					_ = c.flush(batchBuf)
				}
				return
			}
			batchBuf = append(batchBuf, row)
			if len(batchBuf) >= c.cfg.CkWriteBatchSize {
				toFlush := make([]string, len(batchBuf))
				copy(toFlush, batchBuf)
				batchBuf = batchBuf[:0]
				// flush asynchronously with concurrency control
				c.wg.Add(1)
				go func(rows []string) {
					defer c.wg.Done()
					c.semaphore <- struct{}{}        // acquire semaphore
					defer func() { <-c.semaphore }() // release semaphore
					_ = c.flush(rows)
				}(toFlush)
			}
		case <-flushTicker.C:
			if len(batchBuf) > 0 {
				toFlush := make([]string, len(batchBuf))
				copy(toFlush, batchBuf)
				batchBuf = batchBuf[:0]
				c.wg.Add(1)
				go func(rows []string) {
					defer c.wg.Done()
					c.semaphore <- struct{}{}        // acquire semaphore
					defer func() { <-c.semaphore }() // release semaphore
					_ = c.flush(rows)
				}(toFlush)
			}
		}
	}
}

// flush writes rows to ClickHouse using a short-lived batch
func (c *consumer) flush(rows []string) error {
	if rows == nil || len(rows) == 0 {
		return nil
	}

	if c.connPtr == nil || *c.connPtr == nil {
		c.logger.Errorf("no clickhouse connection available for flush")
		return errors.New("no connection")
	}
	conn := *c.connPtr

	ts1 := time.Now().UnixMilli()
	sql := "insert into " + c.cfg.Table

	batch, err := (*conn).PrepareBatch(context.TODO(), sql)
	if err != nil {
		c.logger.Errorf("consumer prepare batch err: %v", err)
		return err
	}

	ts2 := time.Now().UnixMilli()
	for _, r := range rows {
		if err := batch.Append(r); err != nil {
			c.logger.Errorf("batch append err: %v", err)
		}
	}
	ts3 := time.Now().UnixMilli()
	if err := batch.Send(); err != nil {
		c.logger.Errorf("consumer send err: %v", err)
		return err
	}
	ts4 := time.Now().UnixMilli()
	c.logger.Infof("consumer flushed %d rows, prepare: %d ms, append: %d ms, send: %d ms, total: %d ms", len(rows), ts2-ts1, ts3-ts2, ts4-ts3, ts4-ts1)
	return nil
}

// Stop signals the consumer to stop and waits for background goroutines to finish
func (c *consumer) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.wg.Wait()
	})
}
