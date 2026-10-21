package svexporter

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func loadConfig(t *testing.T, id string) (*Config, error) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	sub, err := cm.Sub(id)
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, sub.Unmarshal(cfg))
	return cfg, confmap.Validate(cfg)
}

func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig(t, "sv")
	require.NoError(t, err)
	assert.Equal(t, "node-a", cfg.Instance)
	assert.Equal(t, time.Duration(0), cfg.BackOffConfig.MaxElapsedTime, "never drop a batch")
	q := cfg.QueueConfig.Get()
	require.NotNil(t, q)
	assert.Equal(t, component.MustNewID("file_storage"), *q.StorageID)
	assert.Equal(t, exporterhelper.RequestSizerTypeBytes, q.Sizer)
	assert.True(t, q.BlockOnOverflow)
	b := q.Batch.Get()
	require.NotNil(t, b, "batching happens in the queue, after persistence")
	assert.Equal(t, 5*time.Minute, b.FlushTimeout)
	assert.Equal(t, int64(16<<20), b.MinSize)
	assert.Equal(t, int64(16<<20), b.MaxSize)
	assert.Equal(t, exporterhelper.RequestSizerTypeBytes, b.Sizer)
	assert.Equal(t, 8, cfg.OfflineSlots.PoolSize)

	cfg, err = loadConfig(t, "sv/custom")
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.OfflineSlots.PoolSize)
	assert.Equal(t, 10*time.Second, cfg.QueueConfig.Get().Batch.Get().FlushTimeout)
	assert.Equal(t, 10*time.Second, cfg.ClientConfig.Timeout, "timeout is the sv-be client's")
	assert.Equal(t, 5*time.Minute, cfg.ExportTimeout, "export_timeout is separate")

	_, err = loadConfig(t, "sv/bad_instance")
	assert.ErrorContains(t, err, "instance")
	_, err = loadConfig(t, "sv/no_auth")
	assert.ErrorContains(t, err, "auth is required")
}

func TestConfigStruct(t *testing.T) {
	require.NoError(t, componenttest.CheckConfigStruct(createDefaultConfig()))
}

func TestInstanceNames(t *testing.T) {
	for _, ok := range []string{"web-1", "node.eu_1", "a", strings.Repeat("x", 63), "..."} {
		assert.True(t, validInstance(ok), ok)
	}
	for _, bad := range []string{"", ".", "..", "a/b", "pod name", strings.Repeat("x", 64), "ünï"} {
		assert.False(t, validInstance(bad), bad)
	}
	assert.Equal(t, "my-host.local", sanitizeInstance("my host.local"))
	assert.Equal(t, strings.Repeat("h", 63), sanitizeInstance(strings.Repeat("h", 80)))
	assert.Equal(t, "unknown", sanitizeInstance(""))
	assert.Equal(t, "unknown", sanitizeInstance(".."))
}

func TestFactoryPipeline(t *testing.T) {
	f := newFakeSV(t)
	cfg := testConfig(f, 0)
	q := cfg.QueueConfig.Get()
	q.Batch.Get().FlushTimeout = 50 * time.Millisecond
	host := newOAuthHost(t, f, time.Minute)

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(componentType), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), host))
	require.NoError(t, exp.ConsumeLogs(context.Background(), testLogs(2)))
	require.NoError(t, exp.ConsumeLogs(context.Background(), testLogs(3)))
	require.Eventually(t, func() bool { _, _, _, n := f.snapshot(); return n == 1 }, 5*time.Second, 10*time.Millisecond,
		"both requests are merged into one batch after the queue")
	require.NoError(t, exp.Shutdown(context.Background()))

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, body := range f.objects {
		assert.Len(t, gunzipLines(t, body), 5)
	}
}
