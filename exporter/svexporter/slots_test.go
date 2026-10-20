package svexporter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSlotPool(t *testing.T) {
	f := newFakeSV(t)
	cfg := testConfig(f, 0)
	e, _ := startExporter(t, cfg, newOAuthHost(t, f, time.Minute))

	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	mkSlots := func(n int, life time.Duration) []target {
		var s []target
		for i := range n {
			s = append(s, target{URL: fmt.Sprintf("https://s/%d", i), Key: fmt.Sprint(i), ExpiresAt: now.Add(life)})
		}
		return s
	}
	resp := leaseResponse{Max: 8}
	f.leasesResp = func() (int, any) { return 200, resp }
	p := &slotPool{size: 8, client: e.sv, instance: "web-1", logger: zap.NewNop(), now: func() time.Time { return now }}

	assert.True(t, p.needsRefill())
	assert.Nil(t, p.take())

	// Fill: more than size are trimmed to the freshest size.
	resp.Slots = append(mkSlots(4, 30*time.Minute), mkSlots(6, time.Hour)...)
	require.NoError(t, p.refill(context.Background()))
	assert.Equal(t, 8, p.len())
	assert.False(t, p.needsRefill())

	// Past half-life: refill.
	now = now.Add(16 * time.Minute)
	assert.True(t, p.needsRefill())

	// sv-be at its cap answers nothing new: no more asking until a slot expires.
	resp.Slots = nil
	require.NoError(t, p.refill(context.Background()))
	assert.False(t, p.needsRefill())
	calls := f.leaseCalls

	// take hands out the soonest-expiring usable slot once.
	first := p.take()
	require.NotNil(t, first)
	assert.Equal(t, 7, p.len())

	// Slots with less than a minute left are dropped: the 30-minute ones go,
	// the six 1-hour ones stay.
	now = now.Add(13*time.Minute + 30*time.Second)
	assert.Equal(t, 6, p.len())
	assert.True(t, p.needsRefill())
	assert.Equal(t, calls, f.leaseCalls)

	// max below size caps the pool.
	resp = leaseResponse{Slots: mkSlots(8, 2*time.Hour), Max: 3}
	require.NoError(t, p.refill(context.Background()))
	assert.Equal(t, 3, p.len())

	// A revoked agent clears the pool and stops refills.
	f.leasesResp = func() (int, any) { return 403, map[string]string{"error": "agent_revoked"} }
	require.Error(t, p.refill(context.Background()))
	assert.Equal(t, 0, p.len())
	assert.False(t, p.needsRefill())
}
