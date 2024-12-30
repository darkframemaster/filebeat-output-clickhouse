package clickhouse_20200328

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type client struct {
	observer outputs.Observer
	codec    codec.Codec
	connect  *sql.DB
	config   clickHouseConfig
	logger   *logp.Loggera
}

type rowResult struct {
	Row []interface{}
	E   error
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
	logger.Debugf("connect")

	var err error
	connect := clickhouse.OpenDB(&clickhouse.Options{
		Addr: c.config.Hosts,
		Auth: clickhouse.Auth{
			Database: c.config.Database,
			Username: c.config.User,
			Password: c.config.Password,
		},
	})
	if err == nil {
		if e := connect.Ping(); e != nil {
			err = e
		}
	}
	if err != nil {
		logger.Debugf("error connecting to clickhouse: %s", err)
		c.sleepBeforeRetry(err)
	}
	c.connect = connect

	return err
}

func (c *client) Close() error {
	logger.Debugf("close connection")
	return c.connect.Close()
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

	err := c.batchInsert(sql, rows, nil)
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

func (c *client) extractData(events []publisher.Event) [][]interface{} {
	cSize := len(c.config.Columns)
	rows := make([][]interface{}, len(events))
	for i, event := range events {
		content := event.Content
		logger.Infof("event content: %v", content)
		row := make([]interface{}, cSize)
		for i, c := range c.config.Columns {
			if _, e := content.Fields[c]; e {
				row[i], _ = content.GetValue(c)
			}
		}
		rows[i] = row
	}

	return rows
}

func (c *client) generateSql() string {
	cSize := len(c.config.Columns)
	var columnStr, valueStr strings.Builder
	for i, cl := range c.config.Columns {
		columnStr.WriteString(cl)
		valueStr.WriteString("?")
		if i < cSize-1 {
			columnStr.WriteString(",")
			valueStr.WriteString(",")
		}
	}

	return fmt.Sprint("insert into ", c.config.Table, " (", columnStr.String(), ") values (", valueStr.String(), ")")
}

func (c *client) batchInsert(sql string, rows [][]interface{}, errRows map[int]rowResult) error {
	if errRows == nil {
		errRows = make(map[int]rowResult)
	}

	var err error
	tx, err := c.connect.Begin()
	if err == nil {
		stmt, e := tx.Prepare(sql)
		if e == nil {
			for i, row := range rows {
				if !reflect.DeepEqual(errRows[i], rowResult{}) {
					continue
				}
				_, e := stmt.Exec(row...)
				if e != nil {
					_, r := e.(*column.ErrUnexpectedType)
					if !r {
						err = e
						break
					} else {
						err = e
						errRows[i] = rowResult{
							row,
							e,
						}
					}
				}
			}
		} else {
			err = e
		}
		if stmt != nil {
			defer stmt.Close()
		}
	}
	if err != nil {
		if tx != nil {
			tx.Rollback()
		}
		if c.config.SkipUnexpectedTypeRow && len(errRows) > 0 {
			return c.batchInsert(sql, rows, errRows)
		}
	} else {
		if tx != nil {
			e := tx.Commit()
			if e != nil {
				err = e
			} else {
				if len(errRows) > 0 {
					for _, r := range errRows {
						logp.Err("Skip this row of data: '%s', error: '%s'", r.Row, r.E)
					}
				}
			}
		}
	}

	return err
}

func (c *client) sleepBeforeRetry(err error) {
	logp.Err("will sleep for %v seconds because an error occurs: %s", c.config.RetryInterval, err)
	time.Sleep(time.Second * time.Duration(c.config.RetryInterval))
}
