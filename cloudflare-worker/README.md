# StreamVessel log Worker for Cloudflare

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/streamvessel/collector/tree/main/cloudflare-worker)

Send your DigitalOcean App Platform logs to your own bucket, with no extra App
Platform component.

You deploy this Worker in **your** Cloudflare account. App Platform forwards
logs to it, and the Worker writes them to your bucket as compressed files.
StreamVessel only signs the upload links; your log data never passes through
StreamVessel.

```
App Platform ──logs──► Worker ──► Cloudflare Queue ──► Worker ──► your bucket
                                                         │
                                       asks StreamVessel for an upload link
```

## What you get

About once a minute, a new file appears in your bucket:

```
<prefix>/<agentId>/cf-worker/20261021/20261021T120000Z_20261021T120100Z_3fa2b1c9.log.gz
```

Each file holds everything that arrived in that minute (up to about 12 MB
before compression, often thousands of log records). It is gzip-compressed
NDJSON: every line is one log record.

```json
{"timestamp":"2026-10-21T12:00:01.123Z","level":"info","message":"GET / 200","attrs":{"app":"shop","component":"web"}}
{"timestamp":"2026-10-21T12:00:01.456Z","level":"warn","message":"slow query 812ms","attrs":{"app":"shop","component":"api"}}
…
```

Open the agent's folder with **View logs** in StreamVessel to browse and
filter them.

## Before you start

You need:

- **A StreamVessel log agent.** Create one on a bucket connection you own.
  Copy its **client id** (`sva_…`) and **secret** (`svas_…`); the secret is
  shown only once.
- **A Cloudflare account** with Workers and Queues. Use the Workers Paid plan
  for real traffic.
- **A GitHub or GitLab account** for the one-click deploy, or **Node.js 22 or
  newer** to deploy by hand.

## Set up

### 1. Deploy the Worker

Pick one way.

**One click (recommended).** Press the button:

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/streamvessel/collector/tree/main/cloudflare-worker)

Cloudflare then:

1. asks you to log in, and copies this Worker into a new repository in your
   GitHub or GitLab account;
2. creates the two queues, `sv-logs` and `sv-logs-dlq`;
3. asks for three secrets:

   | Secret | Value |
   |---|---|
   | `SV_CLIENT_ID` | the agent's client id (`sva_…`) |
   | `SV_CLIENT_SECRET` | the agent's secret (`svas_…`) |
   | `BULK_PASSWORD` | a new password for App Platform to use; at least 16 characters, for example from `openssl rand -base64 32` |

4. deploys, and shows your Worker's address, for example
   `https://streamvessel-log-worker.<your-account>.workers.dev`.

Keep the `BULK_PASSWORD`; you need it in step 2. Every commit to your copied
repository redeploys the Worker. To pick up a newer version of this Worker,
copy the changes from `streamvessel/collector` into your repository.

**By hand.** With Node.js 22 or newer:

```sh
git clone https://github.com/streamvessel/collector.git
cd collector/cloudflare-worker
npm ci
npx wrangler login
npx wrangler queues create sv-logs
npx wrangler queues create sv-logs-dlq
npx wrangler secret put SV_CLIENT_ID       # paste the client id (sva_…)
npx wrangler secret put SV_CLIENT_SECRET   # paste the secret (svas_…)
npx wrangler secret put BULK_PASSWORD      # a new password for App Platform to use
npx wrangler deploy
```

`wrangler deploy` prints your Worker's address.

Either way, the Worker talks to `https://app.streamvessel.com`. If you run your
own StreamVessel, change `SV_SERVER` in `wrangler.toml` (just the origin,
without `/be`) and deploy again.

### 2. Point App Platform at the Worker

In the DigitalOcean control panel, open your app, then **Settings → Log
Forwarding → Add Destination**, and choose **OpenSearch**:

| Field | Value |
|---|---|
| Endpoint | your Worker's address from step 1 |
| User | `streamvessel` |
| Password | the `BULK_PASSWORD` from step 1 |
| Index name | anything, for example `logs` |

Or add it to the app spec of each component whose logs you want:

```yaml
services:
  - name: web
    log_destinations:
      - name: streamvessel
        open_search:
          endpoint: https://streamvessel-log-worker.<your-account>.workers.dev
          basic_auth:
            user: streamvessel
            password: ${BULK_PASSWORD}   # store as an encrypted app env var
          index_name: logs
```

## Check that it works

1. Ask the Worker for its version. You should get a JSON document that
   mentions `opensearch`:

   ```sh
   curl -u streamvessel:YOUR_PASSWORD https://streamvessel-log-worker.<your-account>.workers.dev/
   ```

2. Send a test log line. You should get `"errors":false`:

   ```sh
   curl -u streamvessel:YOUR_PASSWORD \
     -H 'content-type: application/x-ndjson' \
     --data-binary $'{"index":{}}\n{"message":"hello from curl","level":"info"}\n' \
     https://streamvessel-log-worker.<your-account>.workers.dev/_bulk
   ```

3. Within about a minute, a new `.log.gz` file should appear in your bucket
   under `<prefix>/<agentId>/cf-worker/`.

If no file appears, watch the Worker's logs live while you send the test line:

```sh
npx wrangler tail
```

## Troubleshooting

The Worker writes one JSON line per event to its logs (`npx wrangler tail`,
or **Workers Logs** in the Cloudflare dashboard).

| You see | Meaning | What to do |
|---|---|---|
| `401` from the Worker | Wrong user or password | Check the App Platform destination matches `BULK_PASSWORD` |
| `500` "receiver is not configured" | `BULK_PASSWORD` is missing or shorter than 16 characters | Run `wrangler secret put BULK_PASSWORD` again |
| `413` from the Worker | Request larger than 8 MiB | Have the sender send smaller batches. Raising `MAX_BODY_BYTES` / `MAX_DECOMPRESSED_BYTES` risks the Worker's 128 MB memory limit |
| `429` from the Worker | Rate limit reached | Raise the limits in `wrangler.toml` |
| `upload_refused_permanently` with `invalid_client` | Client id or secret is wrong, deleted, or the agent was revoked | Check the agent in StreamVessel and update the secrets |
| `upload_refused_permanently` with `agent_revoked` or `scope_changed` | The agent was revoked, or its bucket connection no longer reaches its folder | Check the agent and its connection in StreamVessel; if revoked, create a new agent and update the secrets |
| `upload_refused_permanently` with `too_many_instances` | The agent already has 50 instance folders | Reuse an existing `SV_INSTANCE`, or use another agent |
| `upload_refused_permanently` with `connection_unreadable` | StreamVessel can no longer read the agent's bucket connection | Fix or recreate the connection in StreamVessel |
| `upload_refused_permanently` with `not_found` | Log agents are not turned on in this StreamVessel, or `SV_SERVER` is wrong | Check `SV_SERVER`; ask StreamVessel support |
| `upload_refused_permanently` with `invalid_request` or `invalid_instance` | StreamVessel refused the request; the log line says why | Fix what it names (for `invalid_instance`, `SV_INSTANCE`) |
| `upload_failed` | StreamVessel or the bucket is unreachable or busy for now (including `storage_unavailable`, and `rate_limited` above 120 uploads a minute per instance) | Nothing; the Worker keeps retrying |
| `uploaded` | A file was written | All good |

After a permanent error, the Worker retries each batch once an hour instead
of hammering StreamVessel. Once you fix the cause, uploads resume on their
own. Batches that ran out of retries are in the dead-letter queue; see
[Recover from the dead-letter queue](#recover-from-the-dead-letter-queue).

## Running it

### Delivery guarantees

- **No logs are lost while the Worker answers App Platform.** It replies only
  after the logs are safely in the queue. If queueing fails, it replies with
  an error (503), so the sender knows to send them again.
- **Duplicates are possible.** After a retry, the same lines can end up in
  two files. Deduplicate when you read the logs if that matters to you.
- **Outages are covered for about three days.** If StreamVessel or your
  bucket is down, each batch is retried with growing delays (30 s, 1 min,
  2 min, … up to 1 hour), 72 times in total. After that, it moves to the
  dead-letter queue.

These numbers fit the queue's default retention of 4 days: a batch reaches
the dead-letter queue before Cloudflare would delete it. To cover longer
outages, raise both together: first the retention (up to 14 days on the
Paid plan), then `max_retries` in `wrangler.toml` (at most 100). Every 24 extra
retries need one more day of retention.

StreamVessel refuses a file whose time window starts more than 30 days back.
The window the Worker reports (and that names the file) is limited to 7 days
before the logs arrived, so even the longest retry schedule and a full 14 days
in the dead-letter queue stay inside that limit. Each log line keeps its own
time, even if the sending app's clock was off.

```sh
npx wrangler queues update sv-logs --message-retention-period-secs 1209600
```

### Alerts

In **Workers Logs**, set up an alert on `upload_refused_permanently`. In the
**Queues** dashboard, watch the backlog of `sv-logs-dlq`; it should stay at 0.

### Rotate the secret

1. Add a second secret to the agent in StreamVessel.
2. Put the new secret in `SV_CLIENT_SECRET` (Cloudflare dashboard → your
   Worker → **Settings → Variables and Secrets**, or
   `npx wrangler secret put SV_CLIENT_SECRET`).
3. Delete the old secret in StreamVessel.

There is no downtime.

### Recover from the dead-letter queue

Fix the cause first (for example, update the secret). Then add this block to
`wrangler.toml` and deploy (with the one-click setup, commit it to your copied
repository):

```toml
[[queues.consumers]]
queue = "sv-logs-dlq"
max_batch_size = 100
max_batch_timeout = 60
max_retries = 10
```

When the `sv-logs-dlq` backlog reaches 0, remove the block and deploy again.

> Batches that fail again during this recovery are lost, because the
> dead-letter queue has no dead-letter queue of its own. Only recover once
> uploads work again.

## Settings

Change variables in `wrangler.toml` and deploy again (with the one-click setup,
commit the change to your copied repository). Change secrets in the Cloudflare
dashboard (your Worker → **Settings → Variables and Secrets**) or with
`npx wrangler secret put`.

| Name | Type | Default | What it does |
|---|---|---|---|
| `SV_SERVER` | variable | `https://app.streamvessel.com` | Your StreamVessel address |
| `SV_CLIENT_ID` | secret | — | Agent client id (`sva_…`) |
| `SV_CLIENT_SECRET` | secret | — | Agent secret (`svas_…`) |
| `BULK_PASSWORD` | secret | — | Password App Platform uses; at least 16 characters |
| `BULK_USER` | variable | `streamvessel` | User App Platform uses. Remove it to accept any user name |
| `SV_INSTANCE` | variable | `cf-worker` | Folder name for this Worker's files. Letters, digits, `.`, `_`, `-`; up to 63 characters |
| `MAX_BODY_BYTES` | variable | 8 MiB | Largest request accepted |
| `MAX_DECOMPRESSED_BYTES` | variable | 8 MiB | Largest request accepted after unzipping |
| `DEBUG_LOG_REQUESTS` | variable | `false` | `true` logs each request's method, path and headers (password hidden, log content never shown) |
| `REQUEST_LIMITER` | rate limit | 1200 per minute per IP | Limit on all requests. App Platform sends from a few shared IPs, so keep it generous |
| `AUTH_FAIL_LIMITER` | rate limit | 10 per minute per IP | Answers `429` instead of `401` once an IP keeps sending wrong passwords. It cuts noise; it does not stop guessing, so the password's length is what protects you |

Running more than one Worker on the same agent? Give each one its own
`SV_INSTANCE` so their files land in separate folders.

## How it works

**Receiving.** The Worker accepts the OpenSearch API that App Platform
speaks:

- `GET /` returns a version document.
- `POST /_bulk` and `POST /<index>/_bulk` accept logs.

It checks the password in constant time and accepts gzip-compressed requests.
Each log record becomes one line with these fields:

| Field | Taken from (first one present) |
|---|---|
| `timestamp` | `@timestamp`, `timestamp`, `time`, `ts`, `datetime`, `date`; if missing or not a real log time (before 2000, or over a day ahead), the time it arrived, with the original kept as `attrs.timestamp_raw` |
| `level` | `level`, `severity`, `log.level`, `levelname`, `loglevel`, `severity_text` |
| `message` | `message`, `msg`, `log`, `text` |
| `attrs` | every other field, such as App Platform's app and component names |

A record larger than 100 KB keeps its timestamp and level, and its message is
shortened.

**Queueing.** Lines go into the `sv-logs` queue in chunks that respect
Cloudflare's limits: 128 KB per message, and 100 messages or 256 KB per send.

**Uploading.** About once a minute (or every 100 messages), the Worker:

1. Joins and compresses a batch.
2. Gets an access token from StreamVessel (OAuth 2.0 client credentials,
   reused until it expires).
3. Asks StreamVessel for a short-lived upload link for that file.
4. Uploads the file straight to your bucket as `application/gzip`. Your
   StreamVessel token is never sent to the bucket.

The StreamVessel side is described in `docs/design-notes/log-ingest.md` in
sv-be (added in [sv-be#386](https://github.com/streamvessel/sv-be/pull/386)).

## Not verified yet

This Worker has not yet been run against a real App Platform app. The field
mapping above is based on common field names. To confirm it:

1. Set `DEBUG_LOG_REQUESTS = "true"` and deploy.
2. Run `npx wrangler tail --format json` while the app sends logs.
3. Fill in this table:

| Question | Finding |
|---|---|
| Does App Platform call `GET /` or create an index before sending logs? | _pending_ |
| Are requests gzip-compressed? | _pending_ |
| Which fields does each log record have? | _pending_ |
| Does App Platform read per-record results? | _pending_ |
| Typical request size and rate | _pending_ |

## Development

```sh
npm ci
npm test            # runs the tests inside the Workers runtime
npm run typecheck
npx wrangler dev    # runs locally; put secrets in .dev.vars
```

The tests use a fake StreamVessel and a fake bucket. Like a real S3 bucket,
the fake refuses an upload that carries an `Authorization` header, and answers
`412` to a second upload of the same file when `If-None-Match` is set (AWS).
