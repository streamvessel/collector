package svexporter // import "github.com/streamvessel/collector/exporter/svexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

// uploadRequest is the body of POST {endpoint}/uploads.
type uploadRequest struct {
	Instance string `json:"instance"`
	StartTs  string `json:"startTs"`
	EndTs    string `json:"endTs"`
	// SHA256 is lowercase hex of the gzipped body; its first 8 characters
	// name the object, so a re-sent batch maps to the same key.
	SHA256 string `json:"sha256"`
}

// target is a presigned PUT: the answer to /uploads and one slot of /leases.
// Headers is opaque: the PUT sends exactly these, nothing else that is signed.
type target struct {
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	Key       string            `json:"key"`
	ExpiresAt time.Time         `json:"expiresAt"`
}

// leaseRequest is the body of POST {endpoint}/leases.
type leaseRequest struct {
	Instance string `json:"instance"`
}

// leaseResponse is the assumed answer to /leases (see streamvessel/sv-be#385).
type leaseResponse struct {
	Slots []target `json:"slots"`
	Max   int      `json:"max"`
}

// apiError is a failed sv-be call, classified per the response table.
type apiError struct {
	op     string
	status int    // HTTP status, 0 when sv-be did not answer
	code   string // error code from the body or the token endpoint
	desc   string // error_description, for the log
	retry  time.Duration
	err    error
	// local: the request never left this host, e.g. the oauth2client
	// extension could not read client_secret_file. Not an outage.
	local bool
}

func (e *apiError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "sv-be %s", e.op)
	if e.status != 0 {
		fmt.Fprintf(&b, ": HTTP %d", e.status)
	}
	if e.code != "" {
		fmt.Fprintf(&b, " %s", e.code)
	}
	if e.desc != "" {
		fmt.Fprintf(&b, " (%s)", e.desc)
	}
	if e.err != nil {
		fmt.Fprintf(&b, ": %v", e.err)
	}
	return b.String()
}

func (e *apiError) Unwrap() error { return e.err }

// permanent reports errors no retry can fix: the agent is revoked, its secret
// deleted, or the connection no longer reaches its prefix.
func (e *apiError) permanent() bool {
	switch e.code {
	case "invalid_client", "agent_revoked", "scope_changed":
		return true
	}
	// sv-be refusing the request itself; never the token endpoint, whose
	// other refusals may be a misconfiguration the operator fixes.
	return e.op != "token" && (e.status == http.StatusBadRequest || e.status == http.StatusUnprocessableEntity)
}

// unavailable reports that sv-be is down or overloaded: the case for an
// offline slot.
func (e *apiError) unavailable() bool {
	if e.local {
		return false
	}
	return e.status == 0 || e.status == http.StatusTooManyRequests || e.status >= 500
}

// svClient talks to sv-be through the OAuth2-wrapped client.
type svClient struct {
	http     *http.Client
	endpoint string
}

func (c *svClient) presign(ctx context.Context, req uploadRequest) (*target, error) {
	var t target
	if err := c.post(ctx, "uploads", req, &t); err != nil {
		return nil, err
	}
	if t.URL == "" {
		return nil, &apiError{op: "uploads", err: errors.New("response has no url")}
	}
	return &t, nil
}

func (c *svClient) lease(ctx context.Context, instance string) (*leaseResponse, error) {
	var r leaseResponse
	if err := c.post(ctx, "leases", leaseRequest{Instance: instance}, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *svClient) post(ctx context.Context, op string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/"+op, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return transportError(op, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		code, desc := errorCode(data)
		return &apiError{
			op:     op,
			status: resp.StatusCode,
			code:   code,
			desc:   desc,
			retry:  retryAfter(resp.Header.Get("Retry-After")),
		}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return &apiError{op: op, status: resp.StatusCode, err: fmt.Errorf("decoding response: %w", err)}
	}
	return nil
}

// transportError classifies an error from the OAuth2-wrapped client: a token
// endpoint refusal, or sv-be not answering at all.
func transportError(op string, err error) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		ae := &apiError{op: "token", code: re.ErrorCode, desc: re.ErrorDescription, err: err}
		if re.Response != nil {
			ae.status = re.Response.StatusCode
			ae.retry = retryAfter(re.Response.Header.Get("Retry-After"))
		}
		return ae
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	return &apiError{op: op, err: err, local: !networkFailure(err)}
}

// networkFailure reports whether err means sv-be (or its token endpoint) did
// not answer: a refused or reset connection, a DNS failure, a timeout. Every
// error from http.Client is a *url.Error, so look at what it wraps.
func networkFailure(err error) bool {
	var opErr *net.OpError
	var dnsErr *net.DNSError
	var urlErr *url.Error
	switch {
	case errors.As(err, &opErr), errors.As(err, &dnsErr),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.As(err, &urlErr):
		return urlErr.Timeout()
	}
	return false
}

// errorCode reads {"error": "...", "error_description": "..."} from an
// sv-be error body.
func errorCode(body []byte) (code, desc string) {
	var e struct {
		Error       any    `json:"error"`
		Description string `json:"error_description"`
	}
	if json.Unmarshal(body, &e) != nil {
		return "", ""
	}
	code, _ = e.Error.(string)
	return code, e.Description
}

func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0)
	}
	return 0
}
