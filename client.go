package clickhouse_20200328

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type client struct {
	observer outputs.Observer
	connect  *clickhouse.Conn
	config   clickHouseConfig
	logger   *logp.Logger
}

func newClient(
	observer outputs.Observer,
	config clickHouseConfig,
) *client {
	return &client{
		observer: observer,
		config:   config,
		logger:   logp.NewLogger(logSelector),
	}
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
		c.logger.Debugf("error connecting to clickhouse: %s", err)
		c.sleepBeforeRetry(err)
		return err
	}

	if err := connect.Ping(context.TODO()); err != nil {
		c.logger.Debugf("error ping to clickhouse: %s", err)
		c.sleepBeforeRetry(err)
		return err
	}

	c.connect = &connect
	return err
}

func (c *client) Close() error {
	c.logger.Infof("close connection")
	return (*c.connect).Close()
}

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	if c == nil {
		panic("no client")
	}
	if batch == nil {
		panic("no batch")
	}

	events := batch.Events()
	c.observer.NewBatch(len(events))

	rows := c.extractData(events)
	sql := c.generateSql()

	err := c.batchInsert(sql, rows)
	if err != nil {
		c.sleepBeforeRetry(err)
		batch.RetryEvents(events)
	} else {
		batch.ACK()
	}

	return err
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

func (c *client) batchInsert(sql string, rows []string) error {
	stTs := time.Now().UnixMilli()
	batch, err := (*c.connect).PrepareBatch(context.TODO(), sql)
	if err != nil {
		c.logger.Errorf("error preparing batch: %s", err)
		return err
	}

	for _, row := range rows {
		err := batch.Append(row)
		if err != nil {
			c.logger.Warnf("error appending row: %s", err)
		}
	}

	err = batch.Send()
	edTs := time.Now().UnixMilli()
	c.logger.Infof("inserted %d rows, cost: %d ms", len(rows), edTs-stTs)
	return err
}

func (c *client) sleepBeforeRetry(err error) {
	c.logger.Errorf("will sleep for %v seconds because an error occurs: %s", c.config.RetryInterval, err)
	time.Sleep(time.Second * time.Duration(c.config.RetryInterval))
}
