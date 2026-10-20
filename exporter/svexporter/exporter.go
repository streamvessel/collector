package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const (
	// maxForbidden is how many fresh URLs a batch gets after PUT 403s
	// before the attempt backs off as a signing or clock problem.
	maxForbidden = 3
	// maxRefused is how many times the provider may refuse a batch with
	// 400, 409 or 501 before it is held for an operator.
	maxRefused = 3
)

type svExporter struct {
	cfg      *Config
	set      exporter.Settings
	logger   *zap.Logger
	instance string
	now      func() time.Time

	sv *svClient
	// put is a plain client with no auth: the OAuth2 extension wraps only the
	// sv-be client, and an Authorization header beside the URL signature is
	// refused by S3 and would leak the token to the storage provider.
	put *http.Client

	pool      *slotPool
	stopPool  context.CancelFunc
	poolDone  sync.WaitGroup
	refusedMu sync.Mutex
	refusedBy map[string]int // batch sha256 -> provider refusals so far
	hostname  string
}

func newExporter(cfg *Config, set exporter.Settings) (*svExporter, error) {
	host, _ := os.Hostname()
	instance := cfg.Instance
	if instance == "" {
		instance = sanitizeInstance(host)
	}
	if !validInstance(instance) {
		return nil, fmt.Errorf("instance name %q is invalid; set instance (SV_INSTANCE) to [A-Za-z0-9._-]{1,63}", instance)
	}
	return &svExporter{
		cfg:       cfg,
		set:       set,
		logger:    set.Logger,
		instance:  instance,
		hostname:  host,
		now:       time.Now,
		put:       newPlainClient(cfg.Upload.Timeout),
		refusedBy: map[string]int{},
	}, nil
}

// newPlainClient returns the client for presigned PUTs: proxy from the
// environment, no auth, no redirects (a redirected PUT would not match its
// signature and would carry the body to another host).
func newPlainClient(timeout time.Duration) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// sanitizeInstance turns a host name into a valid instance name.
func sanitizeInstance(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	if s == "" || s == "." || s == ".." {
		return "unknown"
	}
	return s
}

func (e *svExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.cfg.ClientConfig.ToClient(ctx, host.GetExtensions(), e.set.TelemetrySettings)
	if err != nil {
		return err
	}
	e.sv = &svClient{http: client, endpoint: strings.TrimRight(e.cfg.ClientConfig.Endpoint, "/")}
	if q := e.cfg.QueueConfig.Get(); q == nil || q.StorageID == nil {
		e.logger.Warn("sending_queue has no storage: queued batches are lost on restart; set sending_queue.storage to a file_storage extension")
	}
	e.logger.Info("StreamVessel exporter started", zap.String("instance", e.instance))
	if e.cfg.OfflineSlots.PoolSize > 0 {
		e.pool = &slotPool{size: e.cfg.OfflineSlots.PoolSize, client: e.sv, instance: e.instance, logger: e.logger, now: e.now}
		pctx, cancel := context.WithCancel(context.Background())
		e.stopPool = cancel
		e.poolDone.Add(1)
		go func() {
			defer e.poolDone.Done()
			e.pool.run(pctx, e.cfg.OfflineSlots.CheckInterval)
		}()
	}
	return nil
}

func (e *svExporter) shutdown(context.Context) error {
	if e.stopPool != nil {
		e.stopPool()
		e.poolDone.Wait()
	}
	return nil
}

func (e *svExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	b, err := encodeBatch(ld, e.hostname, e.now())
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("encoding batch: %w", err))
	}
	if b.records == 0 {
		return nil
	}
	return e.upload(ctx, b)
}

func (e *svExporter) upload(ctx context.Context, b *batch) error {
	req := uploadRequest{
		Instance: e.instance,
		StartTs:  formatTime(b.start),
		EndTs:    formatTime(b.end),
		Size:     int64(len(b.body)),
		SHA256:   b.sha256,
	}
	for forbidden := 0; ; {
		t, err := e.sv.presign(ctx, req)
		if err != nil {
			return e.presignFailed(ctx, b, err)
		}
		res := e.putBatch(ctx, t, b)
		switch {
		case res.err != nil:
			return fmt.Errorf("PUT %s: %w", t.Key, res.err)
		case res.ok():
			e.forgetRefusals(b)
			e.logger.Debug("Uploaded batch", zap.String("key", t.Key), zap.Int("records", b.records), zap.Int("bytes", len(b.body)), zap.Bool("already_stored", res.status == http.StatusPreconditionFailed))
			return nil
		case res.status == http.StatusForbidden:
			forbidden++
			if forbidden < maxForbidden {
				e.logger.Warn("Storage refused the presigned PUT; asking for a new URL", zap.String("key", t.Key), zap.String("provider_code", res.code), zap.Int("attempt", forbidden))
				continue
			}
			e.logger.Error("Storage refused the presigned PUT with 403 for every URL: check the connection's credentials and this host's clock", zap.String("key", t.Key), zap.String("provider_code", res.code))
			return fmt.Errorf("PUT %s: HTTP 403 %s after %d URLs", t.Key, res.code, forbidden)
		case res.status == http.StatusBadRequest || res.status == http.StatusConflict || res.status == http.StatusNotImplemented:
			return e.refused(b, t, res)
		default:
			return withRetryAfter(fmt.Errorf("PUT %s: HTTP %d %s", t.Key, res.status, res.code), res.retry)
		}
	}
}

// presignFailed applies the response table to a failed /uploads call.
func (e *svExporter) presignFailed(ctx context.Context, b *batch, err error) error {
	var ae *apiError
	if !errors.As(err, &ae) {
		return err
	}
	if ae.permanent() {
		if e.pool != nil && (ae.code == "invalid_client" || ae.code == "agent_revoked") {
			e.pool.clear()
		}
		if ae.code == "invalid_client" {
			e.logger.Error("sv-be refused this agent's client credentials (invalid_client): the agent was revoked or its secret deleted. Uploads stop; dropping the batch. Fix the secret file or create a new agent.",
				zap.Int("records", b.records), zap.Error(err))
		} else {
			e.logger.Error("sv-be refused the upload permanently; dropping the batch", zap.String("reason", ae.code), zap.Int("status", ae.status), zap.Int("records", b.records), zap.Error(err))
		}
		return consumererror.NewPermanent(err)
	}
	if ae.unavailable() && e.pool != nil {
		if t := e.pool.take(); t != nil {
			e.logger.Warn("sv-be unavailable; uploading to an offline slot", zap.String("key", t.Key), zap.Error(err))
			res := e.putBatch(ctx, t, b)
			if res.err == nil && res.status/100 == 2 {
				e.forgetRefusals(b)
				return nil
			}
			// A slot key is not this batch's hash, so 412 means the slot was
			// used already; anything else is a failed attempt. Back off either way.
			e.logger.Warn("Offline slot upload failed", zap.String("key", t.Key), zap.Int("status", res.status), zap.String("provider_code", res.code), zap.Error(res.err))
		}
	}
	return withRetryAfter(err, ae.retry)
}

// refused handles 400, 409 and 501 on the PUT: a provider refusing a signed
// header or a conditional write racing. Retried with backoff up to
// maxRefused times, then the batch is held for HoldInterval per attempt.
func (e *svExporter) refused(b *batch, t *target, res putResult) error {
	e.refusedMu.Lock()
	e.refusedBy[b.sha256]++
	n := e.refusedBy[b.sha256]
	e.refusedMu.Unlock()
	err := fmt.Errorf("PUT %s: provider refused with HTTP %d %s", t.Key, res.status, res.code)
	if n <= maxRefused {
		e.logger.Warn("Storage provider refused the upload; retrying", zap.String("key", t.Key), zap.Int("status", res.status), zap.String("provider_code", res.code), zap.Int("refusals", n))
		return err
	}
	e.logger.Error("Storage provider keeps refusing this batch; holding it until an operator fixes the connection",
		zap.String("key", t.Key), zap.Int("status", res.status), zap.String("provider_code", res.code), zap.Int("refusals", n), zap.Duration("hold", e.cfg.HoldInterval))
	return exporterhelper.NewThrottleRetry(err, e.cfg.HoldInterval)
}

func (e *svExporter) forgetRefusals(b *batch) {
	e.refusedMu.Lock()
	delete(e.refusedBy, b.sha256)
	e.refusedMu.Unlock()
}

type putResult struct {
	status int
	code   string // provider error code from an XML error body
	retry  time.Duration
	err    error // transport error: no answer
}

// ok: stored now, or 412 because the key, which carries the batch hash,
// already exists.
func (r putResult) ok() bool {
	return r.err == nil && (r.status/100 == 2 || r.status == http.StatusPreconditionFailed)
}

// putBatch PUTs the body with exactly the presigned headers, through the
// plain client.
func (e *svExporter) putBatch(ctx context.Context, t *target, b *batch) putResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.URL, bytes.NewReader(b.body))
	if err != nil {
		return putResult{err: err}
	}
	req.ContentLength = int64(len(b.body))
	for k, v := range t.Headers {
		switch http.CanonicalHeaderKey(k) {
		case "Content-Length":
			if n, err := strconv.ParseInt(v, 10, 64); err != nil || n != req.ContentLength {
				return putResult{err: fmt.Errorf("presigned Content-Length %q does not match the body (%d bytes)", v, req.ContentLength)}
			}
		case "Host":
			req.Host = v
		default:
			req.Header.Set(k, v)
		}
	}
	resp, err := e.put.Do(req)
	if err != nil {
		return putResult{err: err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return putResult{
		status: resp.StatusCode,
		code:   providerCode(body),
		retry:  retryAfter(resp.Header.Get("Retry-After")),
	}
}

// providerCode extracts <Code> from an S3-style XML error body.
func providerCode(body []byte) string {
	var e struct {
		Code string `xml:"Code"`
	}
	if xml.Unmarshal(body, &e) != nil {
		return ""
	}
	return e.Code
}

func withRetryAfter(err error, d time.Duration) error {
	if d > 0 {
		return exporterhelper.NewThrottleRetry(err, d)
	}
	return err
}
