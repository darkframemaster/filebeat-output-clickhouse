package clickhouse_20200328

import (
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type clickHouseConfig struct {
	Hosts                 []string     `config:"hosts"`
	User                  string       `config:"user"`
	Password              string       `config:"password"`
	Database              string       `config:"database"`
	Table                 string       `config:"table"`
	Columns               []string     `config:"columns"`
	Codec                 codec.Config `config:"codec"`
	BulkMaxSize           int          `config:"bulk_max_size"`
	MaxRetries            int          `config:"max_retries"`
	RetryInterval         int          `config:"retry_interval"`
	SkipUnexpectedTypeRow bool         `config:"skip_unexpected_type_row"`
}

var (
	defaultConfig = clickHouseConfig{
		Hosts:                 []string{"127.0.0.1:9000"},
		BulkMaxSize:           1000,
		MaxRetries:            3,
		RetryInterval:         60,
		SkipUnexpectedTypeRow: false,
	}
)
