package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/collector/extension/xextension/storage"
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
	// credentialsRetry is how often a refused client credential is tried
	// again: one token request per consumer, well under sv-be's limit of 10
	// failures per 5 minutes.
	credentialsRetry = 10 * time.Minute
	// slotsStorageKey is where the pool is kept in the file_storage client.
	slotsStorageKey = "offline_slots"
)

// slotPool holds offline upload slots from POST /leases. sv-be counts every
// slot it minted and that has not expired against the agent's cap, used or
// not, so the pool asks for more only when one of its slots has expired (or
// at startup), and keeps every slot it gets. A slot is used once: take
// removes it.
//
// With a storage client the pool survives restarts on the same volume:
// otherwise a restarted agent would hold nothing while sv-be still counts
// the slots its previous run leased. Stored slots are credentials; their
// URLs are never logged.
type slotPool struct {
	size     int
	client   *svClient
	instance string
	logger   *zap.Logger
	now      func() time.Time
	jitter   func(time.Duration) time.Duration
	store    storage.Client // nil: in memory only

	mu        sync.Mutex
	slots     []target    // usable, soonest expiry first
	minted    []time.Time // expiry of every slot sv-be minted for us, used or not
	limit     int         // min(size, max from sv-be)
	notBefore time.Time   // no lease call before this
	gen       int         // bumped by clear: a lease in flight then is discarded
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

// byExpiry orders slots soonest expiry first, the order take hands them out.
func byExpiry(a, b target) int { return a.ExpiresAt.Compare(b.ExpiresAt) }

// poolState is what is persisted.
type poolState struct {
	Instance string      `json:"instance"`
	Slots    []target    `json:"slots"`
	Minted   []time.Time `json:"minted"`
}

// load restores the pool saved by a previous run of the same instance,
// dropping slots that expired or are about to.
func (p *slotPool) load(ctx context.Context) {
	if p.store == nil {
		return
	}
	data, err := p.store.Get(ctx, slotsStorageKey)
	if err != nil || data == nil {
		if err != nil {
			p.logger.Warn("Could not read saved offline slots", zap.Error(err))
		}
		return
	}
	var st poolState
	if err := json.Unmarshal(data, &st); err != nil || st.Instance != p.instance {
		return // another instance's slots, or unreadable: start empty
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots, p.minted = st.Slots, st.Minted
	p.dropExpiredLocked()
	slices.SortFunc(p.slots, byExpiry)
	p.logger.Info("Restored offline upload slots", zap.Int("usable", len(p.slots)), zap.Int("outstanding", len(p.minted)))
}

// saveLocked persists the pool; p.mu must be held.
func (p *slotPool) saveLocked() {
	if p.store == nil {
		return
	}
	data, err := json.Marshal(poolState{Instance: p.instance, Slots: p.slots, Minted: p.minted})
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = p.store.Set(ctx, slotsStorageKey, data)
		cancel()
	}
	if err != nil {
		p.logger.Warn("Could not save offline slots", zap.Error(err))
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
	p.saveLocked() // a slot is used once, also across a restart
	return &t
}

func (p *slotPool) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropExpiredLocked()
	return len(p.slots)
}

// clear drops every slot after sv-be refused the agent for good (revoked,
// scope changed): slots would still work until they expire, but must not be
// used. Leasing is tried again after heldBackoff, in case it was fixed.
func (p *slotPool) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = nil
	p.gen++
	p.notBefore = p.now().Add(heldBackoff)
	p.saveLocked()
}

// giveBack returns a slot whose PUT never reached the storage provider.
func (p *slotPool) giveBack(t target) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = append(p.slots, t)
	slices.SortFunc(p.slots, byExpiry)
	p.saveLocked()
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
	if p.now().Before(p.notBefore) {
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
	p.mu.Lock()
	gen := p.gen
	p.mu.Unlock()
	resp, err := p.client.lease(ctx, p.instance)
	if err != nil {
		p.leaseFailed(err)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gen != gen {
		return nil // cleared while the lease was in flight: these must not be used
	}
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
	slices.SortFunc(p.slots, byExpiry)
	p.saveLocked()
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
	switch ae.class() {
	case classRevoked, classPermanent:
		p.clear()
		p.logger.Error("sv-be refused offline slots permanently; pool cleared", zap.Error(err))
	case classCredentials:
		p.backoff(credentialsRetry)
	case classHeld:
		p.logger.Error("sv-be refused offline slots until an operator acts", zap.String("reason", ae.code), zap.String("description", ae.desc))
		p.backoff(heldBackoff)
	case classUnavailable:
		if ae.status != http.StatusTooManyRequests {
			return // sv-be down: the next check asks again
		}
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
