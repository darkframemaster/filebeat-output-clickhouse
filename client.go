package clickhouse_20200328

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// implement outputs.Client interface，
type client struct {
	observer outputs.Observer
	connect  *clickhouse.Conn
	config   clickHouseConfig
	logger   *logp.Logger
	// rows channel for background consumer
	rowsChan chan string
	// background consumer that flushes rows to ClickHouse
	consumer *consumer
	// closed flag set when Close() is called to avoid sending to closed channel
	closed uint32
}

func newClient(
	observer outputs.Observer,
	config clickHouseConfig,
) *client {
	c := &client{
		observer: observer,
		config:   config,
		logger:   logp.NewLogger(logSelector),
	}

	if config.OutputBatchSize < 0 {
		panic("CkWriteBatchSize must be > 0")
	}
	if config.CkWriteBatchSize <= 0 {
		panic("CkWriteBatchSize must be >= 0")
	}
	if config.CkWriteMaxConcurrency <= 0 {
		panic("CkWriteMaxConcurrency must be > 0")
	}

	c.logger.Infof("clickhouse client config: %+v", config)

	c.rowsChan = make(chan string, config.OutputBatchSize)

	// create consumer that will read from rowsChan and write to ClickHouse
	c.consumer = newConsumer(&c.connect, c.rowsChan, config, c.logger)

	return c
}

func (c *client) Connect() error {
	c.logger.Debugf("connect")

	var err error
	connect, err := clickhouse.Open(&clickhouse.Options{
		Addr: c.config.Hosts,
		Auth: clickhouse.Auth{
			Database: c.config.Database,
			Username: c.config.User,
			Password: c.config.Password,
		},
	})
	if err != nil {
		c.logger.Errorf("error connecting to clickhouse: %s", err)
		panic(err)
	}

	if err := connect.Ping(context.TODO()); err != nil {
		c.logger.Errorf("error ping to clickhouse: %s", err)
		panic(err)
	}

	c.connect = &connect
	return err
}

func (c *client) Close() error {
	c.logger.Infof("close connection")

	// stop consumer gracefully
	if c.consumer != nil {
		c.consumer.Stop()
	}

	// mark closed and close rows channel
	atomic.StoreUint32(&c.closed, 1)
	if c.rowsChan != nil {
		close(c.rowsChan)
	}

	if c.connect != nil {
		return (*c.connect).Close()
	}
	return nil
}

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	if c == nil {
		panic("no client")
	}
	if batch == nil {
		panic("no batch")
	}

	ts1 := time.Now().UnixMilli()
	events := batch.Events()
	c.observer.NewBatch(len(events))

	rows := c.extractData(events)

	// enqueue rows to consumer channel; block if channel is full to provide backpressure
	for _, r := range rows {
		if c.rowsChan == nil || atomic.LoadUint32(&c.closed) == 1 {
			continue
		}
		c.rowsChan <- r
	}
	ts2 := time.Now().UnixMilli()

	// when enqueued successfully, ACK the batch
	batch.ACK()
	ts3 := time.Now().UnixMilli()
	c.logger.Infof("enqueued %d rows to channel, enqueue %d ms, total %d ms", len(rows), ts2-ts1, ts3-ts1)
	return nil
}

func (c *client) String() string {
	return "clickhouse(" + strings.Join(c.config.Hosts, ";") + ")"
}

func (c *client) extractData(events []publisher.Event) []string {
	rows := make([]string, len(events))
	for i := range events {
		data := events[i].Content.Fields.String()
		rows[i] = data

		c.logger.Debugf("event content: %v", data)
	}
	return rows
}

func (c *client) generateSql() string {
	return fmt.Sprint("insert into ", c.config.Table)
}
