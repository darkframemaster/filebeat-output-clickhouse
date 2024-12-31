package clickhouse_20200328

type clickHouseConfig struct {
	Hosts    []string `config:"hosts"`
	User     string   `config:"user"`
	Password string   `config:"password"`
	Database string   `config:"database"`
	Table    string   `config:"table"`
	Column   string   `config:"column"` // columns 是 ck 的列字段，目前只支持单列

	BulkMaxSize   int `config:"bulk_max_size"`
	MaxRetries    int `config:"max_retries"`
	RetryInterval int `config:"retry_interval"`
}

var (
	defaultConfig = clickHouseConfig{
		Hosts:         []string{"127.0.0.1:9000"},
		BulkMaxSize:   50000,
		MaxRetries:    3,
		RetryInterval: 3,
	}
)
