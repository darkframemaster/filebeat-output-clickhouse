package clickhouse_20200328

type clickHouseConfig struct {
	Hosts    []string `config:"hosts"`
	User     string   `config:"user"`
	Password string   `config:"password"`
	Database string   `config:"database"`
	Table    string   `config:"table"`
	// columns 是 ck 的列字段
	// columns_alias 是 ck 字段到 日志 字段的映射
	Columns      []string          `config:"columns"`
	ColumnsAlias map[string]string `config:"columns_alias"`

	BulkMaxSize           int  `config:"bulk_max_size"`
	MaxRetries            int  `config:"max_retries"`
	RetryInterval         int  `config:"retry_interval"`
	SkipUnexpectedTypeRow bool `config:"skip_unexpected_type_row"`
}

var (
	defaultConfig = clickHouseConfig{
		Hosts:                 []string{"127.0.0.1:9000"},
		BulkMaxSize:           50000,
		MaxRetries:            3,
		RetryInterval:         3,
		SkipUnexpectedTypeRow: false,
	}
)
