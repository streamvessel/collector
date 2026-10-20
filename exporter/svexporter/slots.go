package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// minSlotLife is the least time a slot must have left to be handed out, so
// the PUT does not race its expiry.
const minSlotLife = time.Minute

// slot is a pre-minted upload target and when it was obtained.
type slot struct {
	target
	obtained time.Time
}

// pastHalfLife reports whether less than half of the slot's lifetime remains.
func (s slot) pastHalfLife(now time.Time) bool {
	return now.After(s.obtained.Add(s.ExpiresAt.Sub(s.obtained) / 2))
}

// slotPool holds up to size offline slots, refilled at half-life while sv-be
// answers. A slot is used once: take removes it.
type slotPool struct {
	size     int
	client   *svClient
	instance string
	logger   *zap.Logger
	now      func() time.Time

	mu        sync.Mutex
	slots     []slot
	notBefore time.Time // after a refill that added nothing, wait until a slot expires
	stopped   bool      // sv-be said the agent is revoked: hold and mint nothing
}

// take removes and returns the slot that expires soonest but still has
// minSlotLife left.
func (p *slotPool) take() *target {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropExpiredLocked()
	if len(p.slots) == 0 {
		return nil
	}
	s := p.slots[0]
	p.slots = p.slots[1:]
	return &s.target
}

func (p *slotPool) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropExpiredLocked()
	return len(p.slots)
}

// clear drops every slot and stops refills, after sv-be reported the agent
// revoked: slots would still work until they expire, but must not be used.
func (p *slotPool) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = nil
	p.stopped = true
}

func (p *slotPool) dropExpiredLocked() {
	now := p.now()
	p.slots = slices.DeleteFunc(p.slots, func(s slot) bool {
		return s.ExpiresAt.Sub(now) < minSlotLife
	})
}

// needsRefill reports whether the pool is short or its oldest slot is past
// half-life.
func (p *slotPool) needsRefill() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.now().Before(p.notBefore) {
		return false
	}
	p.dropExpiredLocked()
	if len(p.slots) < p.size {
		return true
	}
	return p.slots[0].pastHalfLife(p.now())
}

// refill asks sv-be for slots and keeps the size freshest.
func (p *slotPool) refill(ctx context.Context) error {
	resp, err := p.client.lease(ctx, p.instance)
	if err != nil {
		var ae *apiError
		if errors.As(err, &ae) && ae.permanent() {
			p.clear()
			p.logger.Error("sv-be refused offline slots permanently; pool cleared", zap.Error(err))
		}
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	added := 0
	for _, t := range resp.Slots {
		if t.URL != "" && t.ExpiresAt.After(now) {
			p.slots = append(p.slots, slot{target: t, obtained: now})
			added++
		}
	}
	p.dropExpiredLocked()
	// Soonest expiry first; keep the freshest size slots.
	slices.SortFunc(p.slots, func(a, b slot) int { return a.ExpiresAt.Compare(b.ExpiresAt) })
	limit := p.size
	if resp.Max > 0 && resp.Max < limit {
		limit = resp.Max
	}
	if extra := len(p.slots) - limit; extra > 0 {
		p.slots = p.slots[extra:]
	}
	p.notBefore = time.Time{}
	if added == 0 && len(p.slots) > 0 {
		// sv-be is at its cap of outstanding slots; ask again once one expires.
		p.notBefore = p.slots[0].ExpiresAt.Add(-minSlotLife)
	}
	return nil
}

// run checks the pool every interval until ctx ends.
func (p *slotPool) run(ctx context.Context, interval time.Duration) {
	for {
		if p.needsRefill() {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := p.refill(rctx); err != nil && ctx.Err() == nil {
				p.logger.Warn("Could not refill offline upload slots", zap.Error(err), zap.Int("held", p.len()))
			}
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
