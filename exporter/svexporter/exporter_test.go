package svexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func testLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "web-1.example")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	base := time.Date(2026, 10, 20, 12, 0, 0, 123456789, time.UTC)
	for i := range n {
		lr := lrs.AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.Body().SetStr("GET /index.html")
		lr.Attributes().PutStr("log.file.path", "/var/log/nginx/access.log")
		lr.Attributes().PutStr("status", "200")
		lr.Attributes().PutStr("method", "GET")
	}
	return ld
}

func gunzipLines(t *testing.T, body []byte) []map[string]any {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	raw, err := io.ReadAll(zr)
	require.NoError(t, err)
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m), l)
		out = append(out, m)
	}
	return out
}

func TestNDJSONShape(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "web-1")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()

	parsed := lrs.AppendEmpty()
	parsed.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 10, 20, 12, 0, 0, 123456789, time.UTC)))
	parsed.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 10, 20, 13, 0, 0, 0, time.UTC)))
	parsed.SetSeverityNumber(plog.SeverityNumberWarn2)
	parsed.Body().SetStr(`GET /a?x=<b>&y "quoted"`)
	parsed.Attributes().PutStr("log.file.path", "/var/log/nginx/access.log")
	parsed.Attributes().PutStr("status", "404")
	parsed.Attributes().PutInt("bytes", 512)

	observed := lrs.AppendEmpty()
	observed.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 10, 20, 11, 59, 0, 500, time.UTC)))
	observed.SetSeverityText("ERROR")
	observed.Body().SetStr("boom")
	observed.Attributes().PutInt("http.response.status_code", 503)

	rl2 := ld.ResourceLogs().AppendEmpty() // no host.name: falls back
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetEmptyMap().PutStr("k", "v")

	now := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	b, err := encodeBatch(ld, "fallback-host", now)
	require.NoError(t, err)
	assert.Equal(t, 3, b.records)
	sum := sha256.Sum256(b.body)
	assert.Equal(t, hex.EncodeToString(sum[:]), b.sha256)
	assert.Equal(t, time.Date(2026, 10, 20, 11, 59, 0, 500, time.UTC), b.start)
	assert.Equal(t, now, b.end)

	lines := gunzipLines(t, b.body)
	require.Len(t, lines, 3)
	assert.Equal(t, map[string]any{
		"timestamp": "2026-10-20T12:00:00.123456Z", // parsed time wins, 6 digits
		"level":     "warn",
		"status":    float64(404),
		"message":   `GET /a?x=<b>&y "quoted"`,
		"host":      "web-1",
		"file":      "/var/log/nginx/access.log",
		"attrs":     map[string]any{"bytes": float64(512)},
	}, lines[0])
	assert.Equal(t, map[string]any{
		"timestamp": "2026-10-20T11:59:00Z", // observed time; sub-microsecond dropped
		"level":     "error",
		"status":    float64(503),
		"message":   "boom",
		"host":      "web-1",
		"attrs":     map[string]any{},
	}, lines[1])
	assert.Equal(t, "2026-10-20T14:00:00Z", lines[2]["timestamp"])
	assert.Equal(t, `{"k":"v"}`, lines[2]["message"])
	assert.Equal(t, "fallback-host", lines[2]["host"])
	assert.NotContains(t, lines[2], "level")

	// Field order is fixed, and HTML characters are not escaped.
	zr, _ := gzip.NewReader(bytes.NewReader(b.body))
	raw, _ := io.ReadAll(zr)
	first := strings.SplitN(string(raw), "\n", 2)[0]
	assert.True(t, strings.HasPrefix(first, `{"timestamp":"2026-10-20T12:00:00.123456Z","level":"warn","status":404,"message":"GET /a?x=<b>&y \"quoted\"","host":"web-1","file":`), first)
}

func TestFormatTimeAtMostSixDigits(t *testing.T) {
	assert.Equal(t, "2026-01-02T03:04:05.000001Z", formatTime(time.Date(2026, 1, 2, 3, 4, 5, 1999, time.UTC)))
	assert.Equal(t, "2026-01-02T03:04:05.1Z", formatTime(time.Date(2026, 1, 2, 3, 4, 5, 100000000, time.UTC)))
	assert.Equal(t, "2026-01-02T01:04:05Z", formatTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 2*3600))))
}

func TestUploadPutCarriesNoAuthorization(t *testing.T) {
	f := newFakeSV(t)
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))

	require.NoError(t, e.pushLogs(context.Background(), testLogs(3)))

	f.mu.Lock()
	defer f.mu.Unlock()
	// The sv-be call carried the bearer token...
	require.Equal(t, []string{"Bearer svla_1"}, f.authSeen)
	// ...and the PUT carried none, only the presigned headers.
	require.Len(t, f.puts, 1)
	put := f.puts[0]
	assert.Empty(t, put.Header.Get("Authorization"))
	assert.Equal(t, "application/gzip", put.Header.Get("Content-Type"))
	assert.Equal(t, "*", put.Header.Get("If-None-Match"))
	assert.NotEmpty(t, put.Header.Get("X-Amz-Checksum-Sha256"))
	require.Len(t, f.objects, 1)

	req := f.uploadsReqs[0]
	assert.Equal(t, "web-1", req.Instance)
	assert.Equal(t, "2026-10-20T12:00:00.123456Z", req.StartTs)
	assert.Equal(t, "2026-10-20T12:00:02.123456Z", req.EndTs)
	for _, body := range f.objects {
		assert.Equal(t, int64(len(body)), req.Size)
		sum := sha256.Sum256(body)
		assert.Equal(t, hex.EncodeToString(sum[:]), req.SHA256)
		assert.Len(t, gunzipLines(t, body), 3)
	}
}

func TestTokenRefresh(t *testing.T) {
	f := newFakeSV(t)
	f.tokenTTL = 2
	// Refresh when less than 1s is left: the second push needs a new token.
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Second))

	require.NoError(t, e.pushLogs(context.Background(), testLogs(1)))
	time.Sleep(1100 * time.Millisecond)
	require.NoError(t, e.pushLogs(context.Background(), testLogs(2)))

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Equal(t, 2, f.tokens)
	assert.Equal(t, []string{"Bearer svla_1", "Bearer svla_2"}, f.authSeen)
}

func TestInvalidTokenRetriesWithNextToken(t *testing.T) {
	f := newFakeSV(t)
	f.tokenTTL = 2
	f.validToken = "svla_2" // sv-be has dropped the first token
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Second))

	err := e.pushLogs(context.Background(), testLogs(1))
	require.Error(t, err)
	assert.False(t, consumererror.IsPermanent(err), "401 invalid_token must be retried")
	assert.Contains(t, err.Error(), "invalid_token")

	time.Sleep(1100 * time.Millisecond) // the extension fetches a new token
	require.NoError(t, e.pushLogs(context.Background(), testLogs(1)))
}

func TestInvalidClientIsPermanentWithoutRetryLoop(t *testing.T) {
	f := newFakeSV(t)
	f.tokenError = "invalid_client"
	e, logs := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))

	err := e.pushLogs(context.Background(), testLogs(1))
	require.Error(t, err)
	assert.True(t, consumererror.IsPermanent(err))
	_, uploads, puts, _ := f.snapshot()
	assert.Zero(t, uploads)
	assert.Zero(t, puts)
	assert.Equal(t, 1, logs.FilterMessageSnippet("invalid_client").Len())
}

func TestUploadsResponseTable(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      any
		permanent bool
	}{
		{"agent revoked", 403, map[string]string{"error": "agent_revoked"}, true},
		{"scope changed", 403, map[string]string{"error": "scope_changed"}, true},
		{"bad request", 400, map[string]string{"error": "invalid_instance"}, true},
		{"rate limited", 429, map[string]string{"error": "rate_limited"}, false},
		{"server error", 503, nil, false},
		{"unknown 403", 403, map[string]string{"error": "something_else"}, false},
		{"not found (ingest disabled)", 404, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSV(t)
			f.uploadsResp = func() (int, any) { return tt.status, tt.body }
			e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
			err := e.pushLogs(context.Background(), testLogs(1))
			require.Error(t, err)
			assert.Equal(t, tt.permanent, consumererror.IsPermanent(err), err.Error())
			_, _, puts, _ := f.snapshot()
			assert.Zero(t, puts)
		})
	}
}

func TestPutResponseTable(t *testing.T) {
	tests := []struct {
		name     string
		statuses []int
		wantErr  bool
		presigns int
		objects  int
	}{
		{"stored", nil, false, 1, 1},
		{"412 already stored is success", []int{412}, false, 1, 0},
		{"403 then a new URL works", []int{403, 200}, false, 2, 1},
		{"403 three times backs off", []int{403, 403, 403}, true, 3, 0},
		{"500 from storage is retried", []int{500}, true, 1, 0},
		{"409 is retried", []int{409}, true, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeSV(t)
			f.putStatus = tt.statuses
			e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
			err := e.pushLogs(context.Background(), testLogs(1))
			if tt.wantErr {
				require.Error(t, err)
				assert.False(t, consumererror.IsPermanent(err))
			} else {
				require.NoError(t, err)
			}
			_, presigns, _, objects := f.snapshot()
			assert.Equal(t, tt.presigns, presigns)
			assert.Equal(t, tt.objects, objects)
		})
	}
}

func TestPutRefusedIsHeldAfterThreeRetries(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusNotImplemented} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			f := newFakeSV(t)
			f.putStatus = []int{status, status, status, status}
			e, logs := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
			ld := testLogs(1)
			for i := 1; i <= maxRefused; i++ {
				err := e.pushLogs(context.Background(), ld)
				require.Error(t, err)
				assert.False(t, consumererror.IsPermanent(err))
			}
			assert.Equal(t, maxRefused, logs.FilterMessageSnippet("refused the upload; retrying").Len())
			assert.Equal(t, 0, logs.FilterMessageSnippet("holding it").Len())
			assert.Equal(t, maxRefused, logs.FilterFieldKey("provider_code").FilterMessageSnippet("retrying").Len())

			err := e.pushLogs(context.Background(), ld)
			require.Error(t, err)
			assert.False(t, consumererror.IsPermanent(err), "a held batch is kept, not dropped")
			assert.Equal(t, 1, logs.FilterMessageSnippet("holding it").Len())

			require.NoError(t, e.pushLogs(context.Background(), ld)) // provider fixed
			assert.Empty(t, e.refusedBy)
		})
	}
}

func TestOfflineSlotWhenSVBeUnavailable(t *testing.T) {
	f := newFakeSV(t)
	e, logs := startExporter(t, testConfig(f, 8), newOAuthHost(t, f, time.Minute))
	require.Eventually(t, func() bool { return e.pool.len() == 8 }, 5*time.Second, 10*time.Millisecond)

	f.mu.Lock()
	f.uploadsResp = func() (int, any) { return 503, nil }
	f.mu.Unlock()
	require.NoError(t, e.pushLogs(context.Background(), testLogs(2)))
	assert.Equal(t, 7, e.pool.len(), "a slot is used once")
	assert.Equal(t, 1, logs.FilterMessageSnippet("offline slot").Len())

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.objects, 1)
	for k := range f.objects {
		assert.Contains(t, k, "lease_")
	}
	assert.Empty(t, f.puts[0].Header.Get("Authorization"))
}

func TestOfflineSlotWhenSVBeDown(t *testing.T) {
	f := newFakeSV(t)
	host := newOAuthHost(t, f, time.Minute)
	e, _ := startExporter(t, testConfig(f, 8), host)
	require.Eventually(t, func() bool { return e.pool.len() == 8 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.pushLogs(context.Background(), testLogs(1))) // token cached

	f.sv.Close() // sv-be stops answering
	require.NoError(t, e.pushLogs(context.Background(), testLogs(2)))
	_, _, _, objects := f.snapshot()
	assert.Equal(t, 2, objects)
}

func TestNoSlotLeftIsRetried(t *testing.T) {
	f := newFakeSV(t)
	f.uploadsResp = func() (int, any) { return 503, nil }
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
	err := e.pushLogs(context.Background(), testLogs(1))
	require.Error(t, err)
	assert.False(t, consumererror.IsPermanent(err))
}

func TestRevokedAgentClearsSlots(t *testing.T) {
	f := newFakeSV(t)
	e, _ := startExporter(t, testConfig(f, 8), newOAuthHost(t, f, time.Minute))
	require.Eventually(t, func() bool { return e.pool.len() == 8 }, 5*time.Second, 10*time.Millisecond)
	f.mu.Lock()
	f.uploadsResp = func() (int, any) { return 403, map[string]string{"error": "agent_revoked"} }
	f.mu.Unlock()
	err := e.pushLogs(context.Background(), testLogs(1))
	assert.True(t, consumererror.IsPermanent(err))
	assert.Zero(t, e.pool.len())
	assert.False(t, e.pool.needsRefill())
}

func TestEmptyBatchIsNotUploaded(t *testing.T) {
	f := newFakeSV(t)
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
	require.NoError(t, e.pushLogs(context.Background(), plog.NewLogs()))
	tokens, uploads, _, _ := f.snapshot()
	assert.Zero(t, tokens+uploads)
}

func TestPresignedContentLengthMismatch(t *testing.T) {
	f := newFakeSV(t)
	f.uploadsResp = func() (int, any) {
		return 200, target{URL: f.storage.URL + "/bucket/x?X-Amz-Signature=a", Headers: map[string]string{"Content-Length": "1"}, Key: "x"}
	}
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
	err := e.pushLogs(context.Background(), testLogs(1))
	require.ErrorContains(t, err, "does not match the body")
	_, _, puts, _ := f.snapshot()
	assert.Zero(t, puts)
}
