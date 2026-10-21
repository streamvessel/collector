package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// minSlotLife is the least time a slot must have left to be handed out, so
	// the PUT does not race its expiry.
	minSlotLife = time.Minute
	// emptyLeaseBackoff is how long to wait after sv-be minted nothing: it still
	// counts slots this process does not know about (minted before a restart).
	emptyLeaseBackoff = 10 * time.Minute
	// heldBackoff is how long to wait after a 409 (too_many_instances,
	// connection_unreadable), which needs an operator.
	heldBackoff = time.Hour
)

// slotPool holds offline upload slots from POST /leases. sv-be counts every
// slot it minted and that has not expired against the agent's cap, used or
// not, so the pool asks for more only when one of its slots has expired (or
// at startup), and keeps every slot it gets. A slot is used once: take
// removes it.
type slotPool struct {
	size     int
	client   *svClient
	instance string
	logger   *zap.Logger
	now      func() time.Time
	jitter   func(time.Duration) time.Duration

	mu        sync.Mutex
	slots     []target    // usable, soonest expiry first
	minted    []time.Time // expiry of every slot sv-be minted for us, used or not
	limit     int         // min(size, max from sv-be)
	notBefore time.Time   // no lease call before this
	stopped   bool        // sv-be said the agent is revoked: hold and mint nothing
}

func newSlotPool(size int, client *svClient, instance string, logger *zap.Logger, now func() time.Time) *slotPool {
	return &slotPool{
		size: size, limit: size, client: client, instance: instance, logger: logger, now: now,
		jitter: func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return rand.N(d) //nolint:gosec // spreading retries, not security
		},
	}
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
	t := p.slots[0]
	p.slots = p.slots[1:]
	return &t
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
	p.slots = slices.DeleteFunc(p.slots, func(t target) bool { return t.ExpiresAt.Sub(now) < minSlotLife })
	p.minted = slices.DeleteFunc(p.minted, func(exp time.Time) bool { return !exp.After(now) })
}

// needsRefill reports whether sv-be would mint a slot now: fewer than limit
// of ours are outstanding.
func (p *slotPool) needsRefill() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.now().Before(p.notBefore) {
		return false
	}
	p.dropExpiredLocked()
	return len(p.minted) < p.limit
}

// nextCheck is how long until the soonest outstanding slot expires, capped
// at interval.
func (p *slotPool) nextCheck(interval time.Duration) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	d := interval
	if len(p.minted) > 0 {
		d = min(d, slices.MinFunc(p.minted, time.Time.Compare).Sub(p.now())+time.Second)
	}
	return max(d, time.Second)
}

// refill asks sv-be for slots and keeps all it mints.
func (p *slotPool) refill(ctx context.Context) error {
	resp, err := p.client.lease(ctx, p.instance)
	if err != nil {
		p.leaseFailed(err)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if resp.Max > 0 {
		p.limit = min(p.size, resp.Max)
	}
	for _, t := range resp.Slots {
		if t.URL == "" || !t.ExpiresAt.After(now) {
			continue
		}
		p.minted = append(p.minted, t.ExpiresAt)
		p.slots = append(p.slots, t)
	}
	slices.SortFunc(p.slots, func(a, b target) int { return a.ExpiresAt.Compare(b.ExpiresAt) })
	p.notBefore = time.Time{}
	if len(resp.Slots) == 0 {
		// sv-be counts slots we do not hold (minted before a restart, or by
		// a previous run): wait instead of asking every check.
		p.notBefore = now.Add(emptyLeaseBackoff + p.jitter(emptyLeaseBackoff/2))
	}
	return nil
}

func (p *slotPool) leaseFailed(err error) {
	var ae *apiError
	if !errors.As(err, &ae) {
		return
	}
	switch {
	case ae.permanent():
		p.clear()
		p.logger.Error("sv-be refused offline slots permanently; pool cleared", zap.Error(err))
		return
	case ae.status == http.StatusConflict:
		p.logger.Error("sv-be refused offline slots until an operator acts", zap.String("reason", ae.code), zap.String("description", ae.desc))
		p.backoff(heldBackoff)
	case ae.status == http.StatusTooManyRequests:
		// Lease calls are limited per agent, and many instances share one
		// agent: spread the retries.
		p.backoff(max(ae.retry, 10*time.Second) + p.jitter(time.Minute))
	}
}

func (p *slotPool) backoff(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notBefore = p.now().Add(d)
}

// run checks the pool until ctx ends: at least every interval, and right
// after a held slot expires.
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
		case <-time.After(p.nextCheck(interval)):
		}
	}
}
