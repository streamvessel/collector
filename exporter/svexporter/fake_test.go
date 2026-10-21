package svexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oauth2clientauthextension"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// fakeSV is an httptest fake of sv-be's ingest API and of a bucket that
// checks presigned PUTs the way S3 does.
type fakeSV struct {
	t       *testing.T
	sv      *httptest.Server
	storage *httptest.Server

	mu          sync.Mutex
	tokens      int               // tokens issued
	tokenTTL    int               // expires_in seconds
	tokenError  string            // non-empty: token endpoint answers this OAuth error
	validToken  string            // the token /uploads accepts; "" accepts the latest
	uploadsResp func() (int, any) // overrides /uploads
	leasesResp  func() (int, any) // overrides /leases
	putStatus   []int             // statuses for successive PUTs; 200 once exhausted
	uploadsReqs []uploadRequest
	authSeen    []string // Authorization header of each /uploads call
	leaseCalls  int
	puts        []*http.Request
	objects     map[string][]byte
	signed      map[string]map[string]string // key -> headers the URL was signed with
}

const testClientID = "sva_test"
const testSecret = "svas_secret"

func newFakeSV(t *testing.T) *fakeSV {
	f := &fakeSV{t: t, tokenTTL: 900, objects: map[string][]byte{}, signed: map[string]map[string]string{}}
	f.storage = httptest.NewServer(http.HandlerFunc(f.handlePut))
	f.sv = httptest.NewServer(http.HandlerFunc(f.handleSV))
	t.Cleanup(func() { f.sv.Close(); f.storage.Close() })
	return f
}

func (f *fakeSV) endpoint() string { return f.sv.URL + "/be/ingest" }

func (f *fakeSV) handleSV(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/be/ingest/oauth/token":
		f.handleToken(w, r)
	case "/be/ingest/uploads":
		f.authSeen = append(f.authSeen, r.Header.Get("Authorization"))
		if !f.authorized(w, r) {
			return
		}
		var req uploadRequest
		require.NoError(f.t, json.NewDecoder(r.Body).Decode(&req))
		f.uploadsReqs = append(f.uploadsReqs, req)
		if f.uploadsResp != nil {
			status, body := f.uploadsResp()
			writeJSON(w, status, body)
			return
		}
		key := fmt.Sprintf("p/agent/%s/%s_%s.log.gz", req.Instance, req.StartTs, req.SHA256[:8])
		writeJSON(w, 200, f.sign(key))
	case "/be/ingest/leases":
		f.leaseCalls++
		if !f.authorized(w, r) {
			return
		}
		if f.leasesResp != nil {
			status, body := f.leasesResp()
			writeJSON(w, status, body)
			return
		}
		var slots []target
		for i := range 8 {
			key := fmt.Sprintf("p/agent/lease_%d_%d.log.gz", f.leaseCalls, i)
			slots = append(slots, f.sign(key))
		}
		writeJSON(w, 200, leaseResponse{Slots: slots, Max: 8})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeSV) handleToken(w http.ResponseWriter, r *http.Request) {
	require.NoError(f.t, r.ParseForm())
	require.Equal(f.t, "client_credentials", r.PostForm.Get("grant_type"))
	id, secret, ok := r.BasicAuth()
	if !ok {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if f.tokenError != "" || id != testClientID || secret != testSecret {
		code := f.tokenError
		if code == "" {
			code = "invalid_client"
		}
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": code})
		return
	}
	f.tokens++
	writeJSON(w, 200, map[string]any{
		"access_token": fmt.Sprintf("svla_%d", f.tokens),
		"token_type":   "Bearer",
		"expires_in":   f.tokenTTL,
	})
}

func (f *fakeSV) authorized(w http.ResponseWriter, r *http.Request) bool {
	want := f.validToken
	if want == "" {
		want = fmt.Sprintf("svla_%d", f.tokens)
	}
	if r.Header.Get("Authorization") != "Bearer "+want {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_token"})
		return false
	}
	return true
}

// sign returns a presigned target for key. Like sv-be, it signs only
// if-none-match (AWS) and sends lowercase keys without Host.
func (f *fakeSV) sign(key string) target {
	h := map[string]string{"if-none-match": "*"}
	f.signed[key] = h
	return target{
		URL:       f.storage.URL + "/bucket/" + key + "?X-Amz-Signature=abc",
		Headers:   h,
		Key:       key,
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func (f *fakeSV) handlePut(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	f.puts = append(f.puts, r)
	// Every PUT, to an upload URL or an offline slot: our own unsigned
	// Content-Type, no Content-Encoding, and never the bearer token.
	if got := r.Header.Get("Content-Type"); got != "application/gzip" {
		f.t.Errorf("PUT %s: Content-Type %q, want application/gzip", r.URL.Path, got)
	}
	if got := r.Header.Get("Content-Encoding"); got != "" {
		f.t.Errorf("PUT %s: unexpected Content-Encoding %q", r.URL.Path, got)
	}
	if got := r.Header.Get("Authorization"); got != "" {
		f.t.Errorf("PUT %s: carries Authorization %q", r.URL.Path, got)
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method != http.MethodPut || r.URL.Query().Get("X-Amz-Signature") == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(f.putStatus) > 0 {
		status := f.putStatus[0]
		f.putStatus = f.putStatus[1:]
		if status != 200 {
			w.WriteHeader(status)
			fmt.Fprintf(w, "<Error><Code>Code%d</Code></Error>", status)
			return
		}
	}
	// S3 refuses a request that carries both a query signature and an
	// Authorization header.
	if r.Header.Get("Authorization") != "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "<Error><Code>InvalidArgument</Code></Error>")
		return
	}
	for k, v := range f.signed[key] {
		got := r.Header.Get(k)
		if http.CanonicalHeaderKey(k) == "Content-Length" {
			got = fmt.Sprint(r.ContentLength)
		}
		if got != v {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<Error><Code>SignatureDoesNotMatch</Code></Error>")
			return
		}
	}
	if _, exists := f.objects[key]; exists {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	f.objects[key] = body
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakeSV) snapshot() (tokens, uploads, puts, objects int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokens, len(f.uploadsReqs), len(f.puts), len(f.objects)
}

// testHost provides the oauth2client extension to the exporter.
type testHost struct {
	ext map[component.ID]component.Component
}

func (h *testHost) GetExtensions() map[component.ID]component.Component { return h.ext }

var oauthID = component.MustNewID("oauth2client")

// newOAuthHost starts a real oauth2clientauthextension against the fake,
// with the secret in a file as in production.
func newOAuthHost(t *testing.T, f *fakeSV, expiryBuffer time.Duration) *testHost {
	secretFile := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secretFile, []byte(testSecret+"\n"), 0o600))
	fact := oauth2clientauthextension.NewFactory()
	cfg := fact.CreateDefaultConfig().(*oauth2clientauthextension.Config)
	cfg.TokenURL = f.endpoint() + "/oauth/token"
	cfg.ClientID = testClientID
	cfg.ClientSecretFile = secretFile
	cfg.Timeout = 10 * time.Second
	cfg.ExpiryBuffer = expiryBuffer
	ext, err := fact.Create(context.Background(), extensiontest.NewNopSettings(fact.Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), nil))
	t.Cleanup(func() { _ = ext.Shutdown(context.Background()) })
	return &testHost{ext: map[component.ID]component.Component{oauthID: ext}}
}

func testConfig(f *fakeSV, poolSize int) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.ClientConfig.Endpoint = f.endpoint()
	cfg.ClientConfig.Auth = configoptional.Some(configauth.Config{AuthenticatorID: oauthID})
	cfg.Instance = "web-1"
	cfg.OfflineSlots.PoolSize = poolSize
	cfg.OfflineSlots.CheckInterval = time.Hour
	return cfg
}

// startExporter returns a started exporter and the log observer.
func startExporter(t *testing.T, cfg *Config, host *testHost) (*svExporter, *observer.ObservedLogs) {
	core, logs := observer.New(zap.DebugLevel)
	set := exportertest.NewNopSettings(componentType)
	set.Logger = zap.New(core)
	e, err := newExporter(cfg, set)
	require.NoError(t, err)
	require.NoError(t, e.start(context.Background(), host))
	t.Cleanup(func() { _ = e.shutdown(context.Background()) })
	return e, logs
}
