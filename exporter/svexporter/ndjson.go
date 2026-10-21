package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// timeLayout is RFC 3339 with at most 6 fractional digits, always UTC.
const timeLayout = "2006-01-02T15:04:05.999999Z07:00"

func formatTime(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format(timeLayout)
}

// line is one NDJSON record. Field order and names are what the sv-fe log
// viewer recognises; do not rename them.
type line struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level,omitempty"`
	Status    *int64         `json:"status,omitempty"`
	Message   string         `json:"message"`
	Host      string         `json:"host,omitempty"`
	File      string         `json:"file,omitempty"`
	Attrs     map[string]any `json:"attrs"`
}

// Attribute keys lifted out of attrs into top-level fields.
var (
	statusKeys = []string{"status", "http.response.status_code", "http.status_code"}
	fileKeys   = []string{"log.file.path", "log.file.name"}
	hostKeys   = []string{"host.name", "k8s.node.name"}
)

// batch is one encoded upload.
type batch struct {
	body    []byte // gzipped NDJSON
	sha256  string // lowercase hex of body
	records int
	start   time.Time
	end     time.Time
}

// encodeBatch turns logs into a gzipped NDJSON batch. fallbackHost is used
// when a record's resource carries no host name.
func encodeBatch(ld plog.Logs, fallbackHost string, now time.Time) (*batch, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)

	b := &batch{}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		host := firstString(rl.Resource().Attributes(), hostKeys)
		if host == "" {
			host = fallbackHost
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				ts := recordTime(lr, now)
				rt := rangeTime(lr, ts, now)
				if b.records == 0 || rt.Before(b.start) {
					b.start = rt
				}
				if b.records == 0 || rt.After(b.end) {
					b.end = rt
				}
				if err := encodeLine(enc, toLine(lr, ts, host)); err != nil {
					return nil, err
				}
				b.records++
			}
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	b.body = buf.Bytes()
	sum := sha256.Sum256(b.body)
	b.sha256 = hex.EncodeToString(sum[:])
	return b, nil
}

// recordTime is the parsed time, else the observed time, else now.
func recordTime(lr plog.LogRecord, now time.Time) time.Time {
	if ts := lr.Timestamp(); ts != 0 {
		return ts.AsTime()
	}
	if ts := lr.ObservedTimestamp(); ts != 0 {
		return ts.AsTime()
	}
	return now
}

const (
	maxRecordAge  = 7 * 24 * time.Hour
	maxRecordLead = time.Minute

	// sv-be refuses startTs over 30 days old and endTs over 5 minutes
	// ahead of its clock; stay inside with room for clock skew.
	serverMaxAge  = 29 * 24 * time.Hour
	serverMaxLead = 4 * time.Minute
)

// rangeTime is what a record contributes to the batch's startTs/endTs, which
// name the object. sv-be refuses a batch for good when startTs is over 30
// days old or endTs over 5 minutes ahead, so the record's time is clamped
// twice; its own timestamp in the NDJSON is left as it is.
//   - To [arrival - 7 days, arrival + 1 min], arrival being when the agent
//     read it: one stale or future-dated record must not decide the range.
//     The observed time is stored with the record, so the range, and the
//     key, stay the same on every retry.
//   - To [now - 29 days, now + 4 min] by this agent's clock: for OTLP the
//     observed time comes from the producer, whose clock may be wrong too.
//     A record already in range is unchanged, so its key stays stable.
func rangeTime(lr plog.LogRecord, ts, now time.Time) time.Time {
	arrival := now
	if obs := lr.ObservedTimestamp(); obs != 0 {
		arrival = obs.AsTime()
	}
	ts = clampTime(ts, arrival.Add(-maxRecordAge), arrival.Add(maxRecordLead))
	return clampTime(ts, now.Add(-serverMaxAge), now.Add(serverMaxLead))
}

func clampTime(t, lo, hi time.Time) time.Time {
	switch {
	case t.Before(lo):
		return lo
	case t.After(hi):
		return hi
	}
	return t
}

// encodeLine writes one NDJSON line. JSON has no NaN or Infinity, so if an
// attribute holds one, those attributes are written as strings instead of
// failing the whole batch. Encode writes nothing when it fails.
func encodeLine(enc *json.Encoder, l line) error {
	err := enc.Encode(l)
	if err == nil {
		return nil
	}
	for k, v := range l.Attrs {
		if _, e := json.Marshal(v); e != nil {
			l.Attrs[k] = fmt.Sprint(v)
		}
	}
	return enc.Encode(l)
}

func toLine(lr plog.LogRecord, ts time.Time, host string) line {
	l := line{
		Timestamp: formatTime(ts),
		Level:     level(lr),
		Message:   lr.Body().AsString(),
		Host:      host,
		Attrs:     make(map[string]any, lr.Attributes().Len()),
	}
	var statusKey, fileKey string // lifted out of attrs
	for _, k := range statusKeys {
		if v, ok := lr.Attributes().Get(k); ok {
			if n, ok := statusCode(v); ok {
				l.Status = &n
				statusKey = k
				break
			}
		}
	}
	for _, k := range fileKeys {
		if v, ok := lr.Attributes().Get(k); ok && v.Str() != "" {
			l.File = v.Str()
			fileKey = k
			break
		}
	}
	lr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k != statusKey && k != fileKey {
			l.Attrs[k] = v.AsRaw()
		}
		return true
	})
	if tid := lr.TraceID(); !tid.IsEmpty() {
		l.Attrs["trace_id"] = tid.String()
	}
	if sid := lr.SpanID(); !sid.IsEmpty() {
		l.Attrs["span_id"] = sid.String()
	}
	return l
}

func level(lr plog.LogRecord) string {
	switch n := lr.SeverityNumber(); {
	case n >= plog.SeverityNumberFatal:
		return "fatal"
	case n >= plog.SeverityNumberError:
		return "error"
	case n >= plog.SeverityNumberWarn:
		return "warn"
	case n >= plog.SeverityNumberInfo:
		return "info"
	case n >= plog.SeverityNumberDebug:
		return "debug"
	case n >= plog.SeverityNumberTrace:
		return "trace"
	}
	return strings.ToLower(lr.SeverityText())
}

func statusCode(v pcommon.Value) (int64, bool) {
	switch v.Type() {
	case pcommon.ValueTypeInt:
		return v.Int(), true
	case pcommon.ValueTypeDouble:
		return int64(v.Double()), true
	case pcommon.ValueTypeStr:
		n, err := strconv.ParseInt(v.Str(), 10, 64)
		return n, err == nil
	}
	return 0, false
}

func firstString(m pcommon.Map, keys []string) string {
	for _, k := range keys {
		if v, ok := m.Get(k); ok && v.Str() != "" {
			return v.Str()
		}
	}
	return ""
}
