package clickhouse_20200328

import (
	"errors"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
)

const (
	logSelector = "clickhouse"
)

func init() {
	outputs.RegisterType("clickhouse", makeClickHouse)
}

func makeClickHouse(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	logger := logp.NewLogger(logSelector)

	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}
	logger.Debugf("clickhouse output: %v", config)

	if len(config.Table) == 0 {
		return outputs.Fail(errors.New("ClickHouse: the table name must be set"))
	}

	if len(config.Columns) == 0 {
		return outputs.Fail(errors.New("ClickHouse: the table columns must be set"))
	}

	client := newClient(observer, config)

	return outputs.Success(config.BulkMaxSize, config.MaxRetries, client)
}
