package clickhouse_20200328

type clickHouseConfig struct {
	Hosts    []string `config:"hosts"`
	User     string   `config:"user"`
	Password string   `config:"password"`
	Database string   `config:"database"`
	Table    string   `config:"table"`

	// OutputBatchSize: the beat output client publish buffer size. (beats side)
	OutputBatchSize int `config:"bulk_max_size"`

	// CkWriteBatchSize:
	// - ck consumer channel buffer size. (consumer side)
	// - the max batch size for each ck insert. (ck side)
	// For performance perpose, this value should not be too small.
	CkWriteBatchSize int `config:"consumer_batch_size"`

	// CkWriteMaxConcurrency: the max number of concurrent ck insert goroutines. (consumer side)
	CkWriteMaxConcurrency int `config:"consumer_max_concurrency"`

	MaxRetries int `config:"max_retries"`
}

var (
	defaultConfig = clickHouseConfig{
		Hosts:                 []string{"127.0.0.1:9000"},
		OutputBatchSize:       50000,
		CkWriteBatchSize:      50000,
		CkWriteMaxConcurrency: 10,
		MaxRetries:            3,
	}
)
