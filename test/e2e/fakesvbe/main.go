// Command fakesvbe stands in for sv-be's ingest API (streamvessel/sv-be#385)
// in the Compose test stack until an sv-be image exists. It presigns real S3
// PUTs against MiniStack, so the agent's upload path is exercised end to end.
//
// It also generates and verifies test logs:
//
//	fakesvbe serve              the ingest API on :8080
//	fakesvbe gen -n N -dir D    write N numbered lines to D/app.log, rotating
//	fakesvbe verify -n N        check every line 1..N reached the bucket
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fakesvbe serve|gen|verify")
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve()
	case "gen":
		err = gen(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func s3Client() *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(env("S3_ENDPOINT", "http://ministack:4566")),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

var bucket = env("BUCKET", "logs")

const prefix = "agents"

var instanceRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,63}$`)

func validInstance(name string) bool {
	return instanceRe.MatchString(name) && name != "." && name != ".."
}

type server struct {
	presign  *s3.PresignClient
	clientID string
	secret   string
	key      []byte // signs stateless tokens, so a restart keeps them valid like sv-be's database does
	ttl      time.Duration

	mu     sync.Mutex
	minted map[string][]time.Time // instance -> expiry of every slot minted, used or not
}

func serve() error {
	ctx := context.Background()
	c := s3Client()
	for i := 0; ; i++ {
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if err == nil || errors.As(err, &owned) || errors.As(err, &exists) {
			break
		}
		if i == 60 {
			return fmt.Errorf("creating bucket: %w", err)
		}
		time.Sleep(time.Second)
	}
	ttl, _ := strconv.Atoi(env("TOKEN_TTL_SECONDS", "900"))
	secret, err := os.ReadFile(env("CLIENT_SECRET_FILE", "/run/secrets/sv_client_secret"))
	if err != nil {
		return err
	}
	s := &server{
		presign:  s3.NewPresignClient(c),
		clientID: env("CLIENT_ID", "sva_E2eTestClientId0000000"),
		secret:   strings.TrimSpace(string(secret)),
		key:      []byte(env("TOKEN_KEY", "e2e")),
		ttl:      time.Duration(ttl) * time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /be/ingest/oauth/token", s.token)
	mux.HandleFunc("POST /be/ingest/uploads", s.uploads)
	mux.HandleFunc("POST /be/ingest/leases", s.leases)
	log.Printf("fakesvbe listening on :8080, bucket %s", bucket)
	return http.ListenAndServe(":8080", logRequests(mux))
}

func logRequests(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &statusWriter{ResponseWriter: w, status: 200}
		h.ServeHTTP(rw, r)
		log.Printf("%s %s %d", r.Method, r.URL.Path, rw.status)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(s int) { w.status = s; w.ResponseWriter.WriteHeader(s) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if !ok {
		_ = r.ParseForm()
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if r.FormValue("grant_type") != "client_credentials" {
		writeJSON(w, 400, map[string]string{"error": "unsupported_grant_type"})
		return
	}
	if id != s.clientID || !hmac.Equal([]byte(secret), []byte(s.secret)) {
		writeJSON(w, 401, map[string]string{"error": "invalid_client"})
		return
	}
	exp := time.Now().Add(s.ttl).Unix()
	writeJSON(w, 200, map[string]any{
		"access_token": fmt.Sprintf("svla_%d_%s", exp, s.mac(exp)),
		"token_type":   "Bearer",
		"expires_in":   int(s.ttl.Seconds()),
	})
}

func (s *server) mac(exp int64) string {
	m := hmac.New(sha256.New, s.key)
	fmt.Fprint(m, exp)
	return hex.EncodeToString(m.Sum(nil))[:32]
}

func (s *server) authorized(w http.ResponseWriter, r *http.Request) bool {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer svla_")
	expS, mac, _ := strings.Cut(tok, "_")
	exp, err := strconv.ParseInt(expS, 10, 64)
	if !ok || err != nil || time.Now().Unix() > exp || !hmac.Equal([]byte(mac), []byte(s.mac(exp))) {
		writeJSON(w, 401, map[string]string{"error": "invalid_token"})
		return false
	}
	return true
}

type target struct {
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	Key       string            `json:"key"`
	ExpiresAt time.Time         `json:"expiresAt"`
}

func (s *server) uploads(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	var req struct {
		Instance string `json:"instance"`
		StartTs  string `json:"startTs"`
		EndTs    string `json:"endTs"`
		SHA256   string `json:"sha256"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	sum, err := hex.DecodeString(req.SHA256) // names the file; nothing is signed with it
	start, err1 := time.Parse(time.RFC3339Nano, req.StartTs)
	end, err2 := time.Parse(time.RFC3339Nano, req.EndTs)
	if !validInstance(req.Instance) ||
		err != nil || len(sum) != 32 || err1 != nil || err2 != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	const kf = "20060102T150405"
	key := fmt.Sprintf("%s/agent/%s/%s/%sZ_%sZ_%s.log.gz", prefix, req.Instance, start.UTC().Format("20060102"),
		start.UTC().Format(kf), end.UTC().Format(kf), req.SHA256[:8])
	t, err := s.sign(r.Context(), key, 5*time.Minute)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, t)
}

func (s *server) leases(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	var req struct {
		Instance string `json:"instance"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !validInstance(req.Instance) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	mint := time.Now().UTC()
	// Like sv-be: minted, unexpired slots count against max until they expire.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.minted == nil {
		s.minted = map[string][]time.Time{}
	}
	live := slices.DeleteFunc(s.minted[req.Instance], func(exp time.Time) bool { return !exp.After(mint) })
	var slots []target
	for range 8 - len(live) {
		id := make([]byte, 6)
		_, _ = rand.Read(id)
		key := fmt.Sprintf("%s/agent/%s/%s/lease_%s_%s.log.gz", prefix, req.Instance, mint.Format("20060102"),
			mint.Format("20060102T150405"), hex.EncodeToString(id))
		t, err := s.sign(r.Context(), key, time.Hour)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		slots = append(slots, *t)
		live = append(live, t.ExpiresAt)
	}
	s.minted[req.Instance] = live
	writeJSON(w, 200, map[string]any{"slots": slots, "max": 8})
}

// sign presigns a PUT the way sv-be does on AWS: only if-none-match is signed.
func (s *server) sign(ctx context.Context, key string, ttl time.Duration) (*target, error) {
	in := &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String("*"),
	}
	req, err := s.presign.PresignPutObject(ctx, in, s3.WithPresignExpires(ttl))
	if err != nil {
		return nil, err
	}
	h := map[string]string{}
	for k, v := range req.SignedHeader {
		if !strings.EqualFold(k, "host") { // like sv-be: lowercase keys, no Host
			h[strings.ToLower(k)] = strings.Join(v, ",")
		}
	}
	return &target{URL: req.URL, Headers: h, Key: key, ExpiresAt: time.Now().Add(ttl).UTC()}, nil
}

// gen writes n numbered lines in nginx combined format at rate lines/s,
// renaming app.log to app.log.1 every rotate lines.
func gen(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	n := fs.Int("n", 10000, "lines")
	rate := fs.Int("rate", 500, "lines per second")
	rotate := fs.Int("rotate", 4000, "rotate every this many lines")
	dir := fs.String("dir", "/logs", "directory")
	_ = fs.Parse(args)
	path := filepath.Join(*dir, "app.log")
	open := func() (*os.File, error) { return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644) }
	f, err := open()
	if err != nil {
		return err
	}
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	for i := 1; i <= *n; i++ {
		<-tick.C
		status := []int{200, 200, 200, 304, 404, 500}[i%6]
		fmt.Fprintf(f, "10.0.0.%d - - [%s] \"GET /item/%d HTTP/1.1\" %d %d \"-\" \"e2e\"\n",
			i%250, time.Now().Format("02/Jan/2006:15:04:05 -0700"), i, status, i%1000)
		if i%*rotate == 0 {
			f.Close()
			if err := os.Rename(path, path+".1"); err != nil {
				return err
			}
			if f, err = open(); err != nil {
				return err
			}
		}
		if i%1000 == 0 {
			log.Printf("gen: %d lines", i)
		}
	}
	log.Printf("gen: done, %d lines", *n)
	return f.Close()
}

var itemRe = regexp.MustCompile(`GET /item/(\d+) `)

// verify checks that every line 1..n is in the bucket at least once, and
// that each line has the NDJSON shape the log viewer reads.
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	n := fs.Int("n", 10000, "lines expected")
	wantLease := fs.Bool("want-lease", false, "require at least one object uploaded to an offline slot")
	_ = fs.Parse(args)
	ctx := context.Background()
	c := s3Client()
	seen := make([]int, *n+1)
	objects, leases, lines := 0, 0, 0
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: aws.String(prefix + "/")})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, o := range page.Contents {
			objects++
			if strings.Contains(*o.Key, "/lease_") {
				leases++
			}
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: o.Key})
			if err != nil {
				return err
			}
			body, _ := io.ReadAll(out.Body)
			out.Body.Close()
			zr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("%s: not gzip: %w", *o.Key, err)
			}
			sc := bufio.NewScanner(zr)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for sc.Scan() {
				var l struct {
					Timestamp string         `json:"timestamp"`
					Level     string         `json:"level"`
					Status    int            `json:"status"`
					Message   string         `json:"message"`
					File      string         `json:"file"`
					Attrs     map[string]any `json:"attrs"`
				}
				if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
					return fmt.Errorf("%s: bad line %q: %w", *o.Key, sc.Text(), err)
				}
				if _, err := time.Parse(time.RFC3339Nano, l.Timestamp); err != nil || l.Status == 0 || l.Level == "" || l.File == "" || l.Attrs == nil {
					return fmt.Errorf("%s: line lacks fields: %s", *o.Key, sc.Text())
				}
				lines++
				if m := itemRe.FindStringSubmatch(l.Message); m != nil {
					if i, _ := strconv.Atoi(m[1]); i >= 1 && i <= *n {
						seen[i]++
					}
				}
			}
			if err := sc.Err(); err != nil {
				return fmt.Errorf("%s: %w", *o.Key, err)
			}
		}
	}
	var missing []int
	dups := 0
	for i := 1; i <= *n; i++ {
		if seen[i] == 0 {
			missing = append(missing, i)
		}
		if seen[i] > 1 {
			dups++
		}
	}
	log.Printf("verify: %d objects (%d via offline slots), %d lines, %d of %d expected present, %d duplicated",
		objects, leases, lines, *n-len(missing), *n, dups)
	if len(missing) > 0 {
		if len(missing) > 20 {
			missing = missing[:20]
		}
		return fmt.Errorf("missing lines, first: %v", missing)
	}
	if *wantLease && leases == 0 {
		return fmt.Errorf("no object was uploaded to an offline slot")
	}
	return nil
}
