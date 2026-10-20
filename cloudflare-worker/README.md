# StreamVessel log agent as a Cloudflare Worker

A Worker you deploy in your own Cloudflare account. DigitalOcean App Platform
can forward logs to it (destination type **OpenSearch**). The Worker
queues them, then uploads them as gzipped NDJSON files into your own bucket.
It acts as a StreamVessel log agent: it holds the agent's client id and
secret, gets a short-lived token and a presigned URL from StreamVessel, and
PUTs each file straight to the bucket. Log bytes never pass through
StreamVessel.

```
App Platform ──POST /_bulk (Basic auth)──► fetch()  ──sendBatch──► Queue "sv-logs"
                                                                     │ ≤100 msgs / 60 s
                                                                     ▼
            customer bucket ◄──PUT (presigned, no Authorization)── queue()
                                     ▲                               │
                                     └── POST /be/ingest/oauth/token ┤ (client_credentials, cached)
                                         POST /be/ingest/uploads ────┘ → {url, headers, key}
```

Part of [streamvessel/sv-be#384](https://github.com/streamvessel/sv-be/issues/384); backend contract in
[streamvessel/sv-be#385](https://github.com/streamvessel/sv-be/issues/385).

## What it does

**Receive** (`fetch`)

- Every request needs HTTP Basic auth. The password (and the user, if
  `BULK_USER` is set) is compared in constant time: both sides are hashed
  with SHA-256, then compared with `crypto.subtle.timingSafeEqual`. If
  `BULK_PASSWORD` is unset or shorter than 16 characters, the Worker answers
  500 to everything instead of running open.
- `GET /` answers an OpenSearch-style version document, and `HEAD /` answers 200.
- `POST|PUT /_bulk` and `/<index>/_bulk` parse action/document pairs. Each
  `index` or `create` document becomes one NDJSON line:
  `{"timestamp", "level", "message", "attrs"}`.
  - `timestamp` comes from `@timestamp`, `timestamp`, `time`, `ts`,
    `datetime` or `date`. ISO strings and epoch s/ms/µs/ns are understood. It
    is written as RFC 3339 UTC with milliseconds. If there is no usable value,
    the receive time is used, and an unparsable value is kept as
    `attrs.timestamp_raw`.
  - `message` comes from `message`, `msg`, `log` or `text`.
  - `level` comes from `level`, `severity`, `log.level`, `levelname`,
    `loglevel` or `severity_text`, lower-cased.
  - Every other field goes into `attrs` (App Platform's app and component
    fields, for example), plus `attrs["opensearch.index"]`.
  - A line over 100 KB keeps its timestamp and level and has its message
    cut. Its `attrs` is replaced by `{"truncated": true}`.
- `delete` and `update` items, and documents that are not JSON objects, get a
  per-item 400 in the bulk response (`errors: true`). A malformed action line
  or an empty body fails the whole request with 400, as OpenSearch does.
- Lines are packed into queue messages of at most 120 KB (the limit is 128 KB),
  sent with `contentType: "text"` so their size is exactly their UTF-8 length.
  The messages go out in `sendBatch` calls of at most 100 messages and 240 KB
  (the limits are 100 and 256 KB). **The bulk response is only sent after
  every `sendBatch` has resolved.** If one fails, the answer is 503 with
  `Retry-After`, and the sender retries the whole request.
- Size caps: `MAX_BODY_BYTES` (default 10 MiB) applies to the body as sent,
  checked against `Content-Length` and again while streaming.
  `MAX_DECOMPRESSED_BYTES` (default 32 MiB) applies after
  `Content-Encoding: gzip|deflate` is removed. Exceeding either gives 413.
  Unknown encodings get 415.
- Rate limits use [rate-limit bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/),
  keyed by client IP. `REQUEST_LIMITER` (1200/min) covers all requests and
  `AUTH_FAIL_LIMITER` (10/min) covers failed logins. Both answer 429 when
  exceeded. App Platform sends from a few shared IPs, so keep
  `REQUEST_LIMITER` generous.

**Batch** (the queue): `max_batch_size = 100`, `max_batch_timeout = 60`. One
object therefore holds at most about 12 MB of uncompressed lines.

**Upload** (`queue` consumer)

1. Joins the messages' lines, gzips them with `CompressionStream`, and hashes
   the gzip bytes with SHA-256 (`crypto.subtle`). It reads the time window
   from the lines' `timestamp`.
2. Gets a token: `POST {SV_SERVER}/be/ingest/oauth/token`,
   `grant_type=client_credentials`, client id and secret as HTTP Basic. The
   token is cached per isolate until 60 s before `expires_in` runs out.
3. Calls `POST {SV_SERVER}/be/ingest/uploads` with
   `{instance, startTs, endTs, size, sha256}` and gets
   `{url, headers, key, expiresAt}`. On a 401 it gets a new token, once.
4. `PUT url` with exactly the returned headers and **no `Authorization`
   header**, even if one were listed. The bucket would refuse it, and it
   would leak the token to the storage provider.
5. Acks the batch. A 412 on the PUT means the key, which contains the hash,
   already holds this batch, so it counts as success. A 403 means a new URL
   is requested, at most 3 times per attempt.

| sv-be or bucket answers | Consumer does |
|---|---|
| `invalid_client` from the token route, 403 `agent_revoked` / `scope_changed`, any other 4xx, or a missing/invalid setting | **Permanent.** Logs `upload_refused_permanently` with the code at error level, and retries the batch with a 1 h delay. The isolate also stops calling sv-be for 10 minutes. It does not spin. After `max_retries`, the batch moves to the dead-letter queue. |
| 429, 408, 5xx, network error; PUT 400/409/501/5xx; PUT 403 three times | **Transient.** Retries the batch after 30 s × 2^(attempt−1), capped at 1 h, or after `Retry-After` if that is longer. |
| A message that is not a non-empty string (nothing this Worker produces) | Acked and logged as `malformed_message_dropped`. |

The `sha256` sent to `/ingest/uploads` is lowercase hex. `startTs`/`endTs`
are RFC 3339 strings. Both are this Worker's reading of sv-be#385, which
does not fix the encodings yet.

## Delivery semantics

- **At-least-once, with possible duplicates.** A batch can be delivered again
  after a timeout, a crash between the PUT and the ack, or a 503 from the
  receiver after some `sendBatch` calls already succeeded. Cloudflare may also
  regroup redelivered messages into a different batch. That batch then has a
  different hash and therefore a different key, so the same lines can end up
  in two objects. Deduplicate downstream if that matters.
- **Unconsumed messages** are bounded by the queue's retention period: 4 days
  by default, at most 14 days on the paid plan, 24 h on the free plan.
- **A failing consumer** is bounded by `max_retries` (here 100, the maximum)
  and the delays above. With the backoff capped at 1 h, 100 attempts cover
  roughly **four days** of sv-be or bucket outage. After that, a batch goes
  to the dead-letter queue `sv-logs-dlq`. Set the queue's retention to at
  least the outage you want to cover:
  `wrangler queues update sv-logs --message-retention-period-secs 1209600`.
- **A permanent refusal** (revoked agent, deleted secret) does not drop logs.
  Batches keep retrying hourly until they reach the DLQ. Fix the credential,
  then re-drive.

### Alerting

Every outcome is logged as one JSON line (`uploaded`, `upload_failed`,
`upload_refused_permanently`, `upload_blocked`, `enqueue_failed`), and
`[observability]` is enabled. Set up an alert in Workers Logs on
`upload_refused_permanently`. Also watch the DLQ's backlog in the Queues
dashboard.

### Re-driving the dead-letter queue

The DLQ has no consumer, so its messages wait there for its retention period.
The consumer does not care which queue a batch came from. To re-drive, fix
the cause, then attach the same Worker to the DLQ temporarily:

```toml
[[queues.consumers]]
queue = "sv-logs-dlq"
max_batch_size = 100
max_batch_timeout = 60
max_retries = 10
```

Run `wrangler deploy`, wait until the DLQ backlog reaches 0, then remove the
block and deploy again. Batches that fail again during the re-drive are
lost, because the DLQ itself has no dead-letter queue. Keep `max_retries`
high enough, or re-drive only once sv-be answers.

## Deploy

Requirements: a Cloudflare account with Queues (the Workers paid plan for real
volume, since free accounts have tighter limits on Queues operations and
subrequests), Node 22+, and an agent created in StreamVessel (client id
`sva_…` and secret `svas_…`).

```sh
cd cloudflare-worker
npm ci
npx wrangler login
npx wrangler queues create sv-logs
npx wrangler queues create sv-logs-dlq
# edit wrangler.toml: SV_SERVER (and SV_INSTANCE / BULK_USER if you like)
npx wrangler secret put SV_CLIENT_ID       # sva_…
npx wrangler secret put SV_CLIENT_SECRET   # svas_…
npx wrangler secret put BULK_PASSWORD      # e.g. `openssl rand -base64 32`
npx wrangler deploy
```

`wrangler deploy` prints the URL, e.g.
`https://streamvessel-log-worker.<account>.workers.dev`. Check it:

```sh
curl -u streamvessel:$BULK_PASSWORD https://streamvessel-log-worker.<account>.workers.dev/
curl -u streamvessel:$BULK_PASSWORD -H 'content-type: application/x-ndjson' \
  --data-binary $'{"index":{}}\n{"message":"hello","level":"info"}\n' \
  https://streamvessel-log-worker.<account>.workers.dev/_bulk
```

Within 60 s, a file appears under
`<prefix>/<agentId>/<SV_INSTANCE>/YYYYMMDD/…log.gz` in the bucket.

Secrets rotate without downtime. Add a second secret in StreamVessel, run
`wrangler secret put SV_CLIENT_SECRET`, then delete the old secret. Cached
tokens stay valid until they expire. A token minted from a deleted secret
answers 401, and the consumer then fetches a new one.

### App Platform

Add a log destination to the **app's own components** in the app spec, or
under *Settings → Log Forwarding*:

```yaml
services:
  - name: web
    # …
    log_destinations:
      - name: streamvessel
        open_search:
          endpoint: https://streamvessel-log-worker.<account>.workers.dev
          basic_auth:
            user: streamvessel
            password: ${BULK_PASSWORD}   # an encrypted app-level env var
          index_name: logs
```

Set `log_destinations` only on components whose logs you want. Never set it
on anything whose own logs flow back into this pipeline, or you create a
loop. That rule matters if you later run the Go agent as an App Platform
service. The Worker itself is not an App Platform component, so it cannot
loop.

## What App Platform actually sends: not yet captured

The issue asks to record App Platform's real requests. That needs an App
Platform app pointed at a deployed Worker, which was not available while
writing this. To capture them:

1. Deploy with `DEBUG_LOG_REQUESTS = "true"` in `wrangler.toml`.
2. Run `npx wrangler tail --format json` while an app forwards logs.
3. Record what you see here:

| Question | Finding |
|---|---|
| Does it probe `GET /` / `HEAD /` or create the index (`PUT /<index>`) first? | _pending_ (an unknown path answers an OpenSearch 404; add a route if it insists) |
| Is the body gzip (`Content-Encoding`)? | _pending_ (gzip and deflate are handled) |
| Which fields does each document carry (timestamp, message, app, component…)? | _pending_ (mapping is by common names; unknown fields land in `attrs`) |
| Does it read per-item results, or only `errors`? | _pending_ |
| Request sizes and rate | _pending_ (check against the caps and `REQUEST_LIMITER`) |

The debug log prints method, path, query and headers (Authorization
redacted), plus a per-request item count. It never prints document contents.

## Configuration

| Name | Kind | Default | Meaning |
|---|---|---|---|
| `SV_SERVER` | var | — | sv-be origin; routes are `/be/ingest/…` |
| `SV_CLIENT_ID` | secret | — | agent client id (`sva_…`) |
| `SV_CLIENT_SECRET` | secret | — | agent client secret (`svas_…`) |
| `BULK_PASSWORD` | secret | — | Basic auth password, ≥ 16 chars |
| `BULK_USER` | var | `streamvessel` | Basic auth user; unset to accept any user |
| `SV_INSTANCE` | var | `cloudflare-worker` | instance folder, `[A-Za-z0-9._-]{1,63}` |
| `MAX_BODY_BYTES` | var | 10485760 | body cap as sent |
| `MAX_DECOMPRESSED_BYTES` | var | 33554432 | body cap after decompression |
| `DEBUG_LOG_REQUESTS` | var | `false` | log request summaries to `wrangler tail` |
| `LOG_QUEUE` | queue | `sv-logs` | producer binding |
| `REQUEST_LIMITER`, `AUTH_FAIL_LIMITER` | rate limit | 1200/min, 10/min | per client IP; optional |

A 32 MiB body makes about 140 `sendBatch` calls, each one a subrequest. That
fits the paid plan's limit of 1000 subrequests per request.

## Develop

```sh
npm ci
npm test          # vitest in the Workers runtime (@cloudflare/vitest-plugin), fake sv-be + bucket
npm run typecheck
npx wrangler dev  # local, with a local queue; put secrets in .dev.vars
```

The tests cover:

- auth, including the constant-time comparison and fail-closed;
- the version document, bulk parsing and mapping, and the bulk response;
- that the response waits for `sendBatch`;
- message and batch size limits, against both a fake and Miniflare's real
  queue, which enforces Cloudflare's limits;
- gzip handling and both caps, including a gzip bomb;
- the consumer's token cache and 401 refresh, the presign request, the PUT
  checks (no Authorization, size, checksum, `If-None-Match`), and 412/403
  handling;
- backoff and `Retry-After`, and `invalid_client` / `agent_revoked` as
  permanent.
