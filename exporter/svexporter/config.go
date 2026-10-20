// Package svexporter uploads logs to the customer's own bucket through
// StreamVessel: each batch becomes one gzipped NDJSON object, PUT to a URL
// that sv-be presigns for it.
package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config is the svexporter configuration.
type Config struct {
	// ClientConfig is the HTTP client for sv-be. Endpoint is the ingest base,
	// e.g. https://app.streamvessel.com/be/ingest, and Auth names the
	// oauth2client extension. It is never used for the PUT to the bucket.
	ClientConfig confighttp.ClientConfig `mapstructure:",squash"`

	// TimeoutConfig bounds one export attempt: presign plus PUT.
	TimeoutConfig exporterhelper.TimeoutConfig `mapstructure:",squash"`

	// QueueConfig is the persistent sending queue. Batching happens here,
	// after persistence, so nothing the receivers handed over is lost on a crash.
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// BackOffConfig: max_elapsed_time is 0 so a batch is retried until it lands.
	BackOffConfig configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// Instance is the name this host or container reports, which becomes a
	// folder level in the object key. Empty means the host name.
	Instance string `mapstructure:"instance"`

	// Upload configures the plain client that PUTs to presigned URLs.
	Upload UploadConfig `mapstructure:"upload"`

	// OfflineSlots configures the pool of pre-minted upload slots used while
	// sv-be does not answer.
	OfflineSlots OfflineSlotsConfig `mapstructure:"offline_slots"`

	// HoldInterval is how long a batch waits after the storage provider
	// refused it (400, 409, 501) three times in a row.
	HoldInterval time.Duration `mapstructure:"hold_interval"`
}

// UploadConfig configures the PUT to the bucket.
type UploadConfig struct {
	// Timeout of a single PUT.
	Timeout time.Duration `mapstructure:"timeout"`
}

// OfflineSlotsConfig configures the offline slot pool.
type OfflineSlotsConfig struct {
	// PoolSize is how many slots to hold; 0 disables the pool.
	PoolSize int `mapstructure:"pool_size"`
	// CheckInterval is how often the pool is checked for refill.
	CheckInterval time.Duration `mapstructure:"check_interval"`
}

const (
	maxPoolSize  = 8
	maxBatchSize = 16 << 20
)

var instanceRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,63}$`)

// validInstance reports whether name is an instance name sv-be accepts.
func validInstance(name string) bool {
	return instanceRe.MatchString(name) && name != "." && name != ".."
}

var _ component.Config = (*Config)(nil)

// Validate checks the configuration.
func (cfg *Config) Validate() error {
	var errs []error
	if cfg.ClientConfig.Endpoint == "" {
		errs = append(errs, errors.New("endpoint is required (the sv-be ingest base URL, e.g. ${env:SV_SERVER}/be/ingest)"))
	} else if u, err := url.Parse(cfg.ClientConfig.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("endpoint %q is not an http(s) URL", cfg.ClientConfig.Endpoint))
	}
	if !cfg.ClientConfig.Auth.HasValue() {
		errs = append(errs, errors.New("auth is required: point it at the oauth2client extension"))
	}
	if cfg.Instance != "" && !validInstance(cfg.Instance) {
		errs = append(errs, fmt.Errorf("instance %q must match [A-Za-z0-9._-]{1,63} and must not be \".\" or \"..\"", cfg.Instance))
	}
	if cfg.Upload.Timeout <= 0 {
		errs = append(errs, errors.New("upload.timeout must be positive"))
	}
	if cfg.OfflineSlots.PoolSize < 0 || cfg.OfflineSlots.PoolSize > maxPoolSize {
		errs = append(errs, fmt.Errorf("offline_slots.pool_size must be between 0 and %d", maxPoolSize))
	}
	if cfg.OfflineSlots.PoolSize > 0 && cfg.OfflineSlots.CheckInterval <= 0 {
		errs = append(errs, errors.New("offline_slots.check_interval must be positive"))
	}
	if cfg.HoldInterval <= 0 {
		errs = append(errs, errors.New("hold_interval must be positive"))
	}
	if q := cfg.QueueConfig.Get(); q != nil && q.Batch.HasValue() {
		if b := q.Batch.Get(); b.Sizer == exporterhelper.RequestSizerTypeBytes && (b.MaxSize == 0 || b.MaxSize > maxBatchSize) {
			errs = append(errs, fmt.Errorf("sending_queue.batch.max_size must be between 1 and %d bytes", maxBatchSize))
		}
	}
	return errors.Join(errs...)
}
