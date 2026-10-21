package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const stability = component.StabilityLevelAlpha

// componentType is the name used in collector configs: exporters: [sv].
var componentType = component.MustNewType("sv")

// NewFactory returns the svexporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(componentType, createDefaultConfig,
		exporter.WithLogs(createLogsExporter, stability))
}

func createDefaultConfig() component.Config {
	httpCfg := confighttp.NewDefaultClientConfig()
	httpCfg.Timeout = 30 * time.Second

	queue := exporterhelper.NewDefaultQueueConfig()
	queue.Sizer = exporterhelper.RequestSizerTypeBytes
	queue.QueueSize = 512 << 20
	queue.BlockOnOverflow = true
	queue.NumConsumers = 4
	batch := exporterhelper.NewDefaultBatchConfig()
	batch.Sizer = exporterhelper.RequestSizerTypeBytes
	batch.FlushTimeout = 5 * time.Minute
	batch.MinSize = maxBatchSize
	batch.MaxSize = maxBatchSize
	queue.Batch = configoptional.Some(batch)

	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval = 5 * time.Second
	retry.MaxInterval = 5 * time.Minute
	retry.MaxElapsedTime = 0 // never drop a batch for taking too long

	return &Config{
		ClientConfig:  httpCfg,
		ExportTimeout: 2 * time.Minute,
		QueueConfig:   configoptional.Some(queue),
		BackOffConfig: retry,
		Upload:        UploadConfig{Timeout: 60 * time.Second},
		OfflineSlots:  OfflineSlotsConfig{PoolSize: maxPoolSize, CheckInterval: time.Minute},
		HoldInterval:  15 * time.Minute,
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	e, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, e.pushLogs,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: c.ExportTimeout}),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithQueue(c.QueueConfig),
	)
}
