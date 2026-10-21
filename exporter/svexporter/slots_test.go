package svexporter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// testClock is a clock shared by the pool and the fake's slot accounting.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) add(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.t = c.t.Add(d) }

func newTestPool(t *testing.T, f *fakeSV, clock *testClock) *slotPool {
	e, _ := startExporter(t, testConfig(f, 0), newOAuthHost(t, f, time.Minute))
	f.mu.Lock()
	f.clock = clock.now
	f.mu.Unlock()
	p := newSlotPool(8, e.sv, "web-1", zap.NewNop(), clock.now)
	p.jitter = func(time.Duration) time.Duration { return 0 }
	return p
}

func (f *fakeSV) leases() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaseCalls
}

// sv-be counts every minted, unexpired slot against max, used or not. The
// pool must ask only when one has expired, and keep everything it gets.
func TestSlotPoolMatchesBackendCap(t *testing.T) {
	f := newFakeSV(t)
	clock := &testClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	p := newTestPool(t, f, clock)

	// Startup: empty pool, one lease call fills it.
	require.True(t, p.needsRefill())
	require.NoError(t, p.refill(context.Background()))
	assert.Equal(t, 8, p.len())
	assert.Equal(t, 1, f.leases())

	// Using slots does not free sv-be's count, so no lease call: half-life
	// or a used slot is no reason to ask.
	for range 3 {
		require.NotNil(t, p.take())
	}
	clock.add(40 * time.Minute)
	assert.False(t, p.needsRefill())
	assert.Equal(t, 5, p.len())

	// Within a minute of expiry the slots are no longer handed out, but sv-be
	// still counts them: still no lease call.
	clock.add(19*time.Minute + 30*time.Second)
	assert.Zero(t, p.len())
	assert.False(t, p.needsRefill())

	// Once they expire, the next lease gets a full set again.
	clock.add(time.Minute)
	require.True(t, p.needsRefill())
	require.NoError(t, p.refill(context.Background()))
	assert.Equal(t, 8, p.len())
	assert.Equal(t, 2, f.leases())
	assert.False(t, p.needsRefill())
}

func TestSlotPoolAfterRestart(t *testing.T) {
	f := newFakeSV(t)
	clock := &testClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	first := newTestPool(t, f, clock)
	require.NoError(t, first.refill(context.Background()))

	// A restarted agent holds nothing, but sv-be still counts the old slots:
	// the empty answer must not turn into a lease call every check.
	p := newTestPool(t, f, clock)
	require.True(t, p.needsRefill())
	require.NoError(t, p.refill(context.Background()))
	assert.Zero(t, p.len())
	assert.False(t, p.needsRefill())
	clock.add(emptyLeaseBackoff - time.Second)
	assert.False(t, p.needsRefill())
	clock.add(time.Second)
	assert.True(t, p.needsRefill())
}

func TestSlotPoolLeaseErrors(t *testing.T) {
	f := newFakeSV(t)
	clock := &testClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	p := newTestPool(t, f, clock)

	// 429: never fatal, waits for Retry-After (plus jitter, zero here).
	f.leasesResp = func() (int, any) { return 429, map[string]string{"error": "rate_limited"} }
	require.Error(t, p.refill(context.Background()))
	assert.False(t, p.needsRefill())
	clock.add(10 * time.Second)
	assert.True(t, p.needsRefill())

	// 409: an operator has to act; ask again in an hour.
	f.leasesResp = func() (int, any) { return 409, map[string]string{"error": "too_many_instances"} }
	require.Error(t, p.refill(context.Background()))
	clock.add(59 * time.Minute)
	assert.False(t, p.needsRefill())
	clock.add(time.Minute)
	assert.True(t, p.needsRefill())

	// max below the configured size caps what the pool asks for.
	f.leasesResp = func() (int, any) {
		return 200, leaseResponse{Slots: []target{{URL: "https://s/1", ExpiresAt: clock.now().Add(time.Hour)}}, Max: 1}
	}
	require.NoError(t, p.refill(context.Background()))
	assert.Equal(t, 1, p.len())
	assert.False(t, p.needsRefill())

	// A revoked agent clears the pool and stops refills.
	clock.add(2 * time.Hour)
	f.leasesResp = func() (int, any) { return 403, map[string]string{"error": "agent_revoked"} }
	require.Error(t, p.refill(context.Background()))
	assert.Zero(t, p.len())
	assert.False(t, p.needsRefill())
}

// memStorage is an in-memory storage.Client, like file_storage across a
// restart on the same volume.
type memStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *memStorage) Get(_ context.Context, k string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[k], nil
}

func (m *memStorage) Set(_ context.Context, k string, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string][]byte{}
	}
	m.data[k] = v
	return nil
}

func (m *memStorage) Delete(_ context.Context, k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, k)
	return nil
}

func (m *memStorage) Batch(context.Context, ...*storage.Operation) error { return nil }
func (m *memStorage) Close(context.Context) error                        { return nil }

func TestSlotPoolSurvivesRestart(t *testing.T) {
	f := newFakeSV(t)
	clock := &testClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	store := &memStorage{}

	first := newTestPool(t, f, clock)
	first.store = store
	require.NoError(t, first.refill(context.Background()))
	used := first.take()
	require.NotNil(t, used)

	// Restart on the same volume: the 7 unused slots are back, the used one
	// is not, and sv-be's count of 8 is known, so no lease call is wasted.
	clock.add(10 * time.Minute)
	p := newTestPool(t, f, clock)
	p.store = store
	p.load(context.Background())
	assert.Equal(t, 7, p.len())
	assert.False(t, p.needsRefill())
	for range 7 {
		s := p.take()
		require.NotNil(t, s)
		assert.NotEqual(t, used.URL, s.URL, "a slot is used once, also across a restart")
	}

	// Slots within a minute of expiry are dropped on load.
	clock.add(49*time.Minute + 30*time.Second)
	again := newTestPool(t, f, clock)
	again.store = store
	again.load(context.Background())
	assert.Zero(t, again.len())

	// Another instance name does not pick up these slots.
	other := newSlotPool(8, again.client, "web-2", zap.NewNop(), clock.now)
	other.store = store
	clock.add(-50 * time.Minute)
	other.load(context.Background())
	assert.Zero(t, other.len())
}

// memStorageExt stands in for file_storage.
type memStorageExt struct{ client *memStorage }

func (*memStorageExt) Start(context.Context, component.Host) error { return nil }
func (*memStorageExt) Shutdown(context.Context) error              { return nil }
func (m *memStorageExt) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return m.client, nil
}

func TestExporterKeepsSlotsInQueueStorage(t *testing.T) {
	f := newFakeSV(t)
	storageID := component.MustNewID("file_storage")
	ext := &memStorageExt{client: &memStorage{}}
	host := newOAuthHost(t, f, time.Minute)
	host.ext[storageID] = ext
	cfg := testConfig(f, 8)
	cfg.QueueConfig.Get().StorageID = &storageID

	e, _ := startExporter(t, cfg, host)
	require.Eventually(t, func() bool { return e.pool.len() == 8 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.shutdown(context.Background()))

	restarted, logs := startExporter(t, cfg, host)
	assert.Equal(t, 8, restarted.pool.len())
	assert.Equal(t, 1, f.leases(), "no lease call after a restart that kept its slots")
	for _, entry := range logs.All() {
		for _, v := range entry.ContextMap() {
			assert.NotContains(t, fmt.Sprint(v), "X-Amz-Signature", "slot URLs are credentials")
		}
		assert.NotContains(t, entry.Message, "X-Amz-Signature")
	}
}
