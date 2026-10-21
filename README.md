# StreamVessel log collector

StreamVessel log collector: OTel Collector distribution (issue #1) and Cloudflare Worker (issue #2).

## Log agent (`sv-log-agent`)

An [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) distribution that tails log files or receives OTLP, batches the records into gzipped NDJSON, and uploads each batch straight into the customer's own bucket through a URL that StreamVessel presigns. Log bytes never pass through StreamVessel's servers. The files land where the StreamVessel log viewer opens them as a folder. Tracking issue: #1, part of streamvessel/sv-be#384.

### Quick start

You need an agent created in StreamVessel: its client id (`sva_…`) and secret (`svas_…`, shown once).

**Linux with systemd** (the `.deb` / `.rpm` from a release):

```sh
sudo install -m 600 /dev/null /etc/sv-log-agent/client-secret
sudo editor /etc/sv-log-agent/client-secret # paste the secret (keeps it out of shell history)
sudo editor /etc/sv-log-agent/agent.env     # set SV_CLIENT_ID (and SV_SERVER if not the default)
sudo editor /etc/sv-log-agent/agent.yaml    # point the filelog receiver at your logs
sudo systemctl enable --now sv-log-agent
```

The unit runs as a dynamic user in group `adm` (read access to `/var/log/nginx` and similar) and keeps its state in `/var/lib/sv-log-agent`.

**Docker Compose:** see [`examples/compose/compose.yaml`](examples/compose/compose.yaml) (agent next to nginx, secret as a Compose secret).

**Kubernetes:** see [`examples/kubernetes/`](examples/kubernetes/): a DaemonSet that tails `/var/log/pods` on every node, or a sidecar next to one workload.

**By hand:**

```sh
SV_SERVER=https://app.streamvessel.com SV_CLIENT_ID=sva_… \
SV_CLIENT_SECRET_FILE=/etc/sv-log-agent/secret sv-log-agent --config examples/agent.yaml
```

### Configuration

The example configs read these environment variables:

| Variable | Meaning |
|---|---|
| `SV_SERVER` | `https://app.streamvessel.com`, no trailing slash |
| `SV_CLIENT_ID` | the agent's client id (`sva_…`) |
| `SV_CLIENT_SECRET_FILE` | path to the file holding the secret (recommended; see [Supplying the secret](#supplying-the-secret)) |
| `SV_INSTANCE` | folder name for this host or workload, `[A-Za-z0-9._-]{1,63}`; defaults to the host name. DaemonSet: the node name; sidecar: the workload name; never the pod name, which changes on every rollout |
| `SV_STORAGE_DIR` | read offsets, the persistent queue and the offline slots (default `/var/lib/sv-log-agent`) |

[`examples/agent.yaml`](examples/agent.yaml) is a complete config for an nginx access log, parsed for time and status. Options of the `sv` exporter, with their defaults:

| Option | Default | Meaning |
|---|---|---|
| `endpoint` | (required) | sv-be ingest base, `${env:SV_SERVER}/be/ingest` |
| `auth.authenticator` | (required) | the `oauth2client` extension |
| `instance` | host name | see `SV_INSTANCE` |
| `timeout` | `30s` | one call to sv-be |
| `export_timeout` | `2m` | one export attempt: asking sv-be for a URL plus the upload |
| `upload.timeout` | `60s` | one upload to the bucket |
| `sending_queue` | persistent, 512 MiB | set `storage: file_storage`; batches of 16 MiB uncompressed or 5 minutes, whichever comes first |
| `retry_on_failure.max_elapsed_time` | `0` | retry until the batch lands; never drop it for taking too long |
| `offline_slots.pool_size` | `8` | pre-minted upload slots for sv-be outages; `0` turns them off |
| `hold_interval` | `15m` | how long a batch waits after the storage provider refused it three times |

### Supplying the secret

The OpenTelemetry Collector accepts the secret from a file or from the environment; which to use is a deployment choice, not a collector rule. **We recommend a file**, and every example here uses one:

- **Rotation without a restart.** The `oauth2client` extension re-reads `client_secret_file` each time it fetches a token (about every 10 minutes). A value taken from the environment is fixed when the agent starts.
- **Less exposure.** A file stays out of `docker inspect`, the process environment (`/proc/<pid>/environ`) and crash or support dumps that include the environment.
- **Native platform support.** systemd credentials, Docker Compose `secrets:` and Kubernetes Secret volumes all deliver a file.

A file is not encrypted on disk: protect it with permissions (`chmod 600`, or the platform's secret store).

**Environment variable.** Where files are not available (for example DigitalOcean App Platform, which offers only encrypted environment variables), replace `client_secret_file` in the config with:

```yaml
extensions:
  oauth2client:
    client_secret: ${env:SV_CLIENT_SECRET}
```

The collector prints the value as `[REDACTED]` in its own output. Rotating the secret then needs a restart.

**Avoid the command line** (`--set extensions::oauth2client::client_secret=…`): arguments are visible to every user on the host through `ps`.

**Rotating the secret.** An agent can have two live secrets, so rotation needs no downtime:

1. Create a second secret for the agent in StreamVessel.
2. Put it where the agent reads it:
   - Kubernetes: update the Secret; the mounted file follows within a minute or two.
   - Docker Compose: overwrite the secret file in place (`cat new > sv_client_secret.txt`); an editor that replaces the file leaves the container on the old copy until it restarts.
   - systemd: replace `/etc/sv-log-agent/client-secret`, then `systemctl restart sv-log-agent`. The unit's `LoadCredential` copies the file when the service starts.
   - Environment variable: restart with the new value.
3. Wait about 15 minutes, until every agent has fetched a token with the new secret, then delete the old one. Deleting it earlier only makes uploads retry until the next token; nothing is lost.

### How it works

```
filelog / otlp → memory_limiter → resourcedetection → sv exporter
                                                       └ persistent sending_queue (file_storage)
                                                         → batch of 16 MiB uncompressed or 5 min
                                                         → NDJSON + gzip + SHA-256
                                                         → POST /be/ingest/uploads → PUT to the bucket
```

- **Nothing read is lost.** There is no batch processor: batching happens in the exporter's queue after the records are on disk, so a crash or `SIGKILL` loses nothing the receivers handed over, and a batch is retried until it lands. The binary turns on the `stanza.synchronousLogEmitter` feature gate, so filelog cannot save a read offset before its records reach the queue. Delivery is at least once.
- **Authentication** is the stock `oauth2client` extension from collector-contrib (OAuth 2.0 client credentials against `${SV_SERVER}/be/ingest/oauth/token`, 15-minute tokens); the agent has no auth code of its own.
- **Uploads.** For each batch the agent asks sv-be for a presigned URL (`POST /be/ingest/uploads {instance, startTs, endTs, sha256}`, contract: streamvessel/sv-be#386) and PUTs the batch with exactly the headers sv-be returned, plus `Content-Type: application/gzip`. The PUT uses a separate plain HTTP client: no `Authorization` header ever reaches the storage provider.
- **Object names** are chosen by sv-be from the batch's time range and hash, so a retried batch lands on the same key and a 412 means it is already stored. The range comes from the records' times, clamped to [read time − 7 days, read time + 1 min] and to [now − 29 days, now + 4 min]: one stale line or a producer with a bad clock cannot get the batch refused, which sv-be does for good when the range is over 30 days old or more than 5 minutes ahead.
- **Offline slots.** The agent holds up to 8 pre-minted upload slots (`POST /be/ingest/leases`, valid 1 hour) and uses one per batch while sv-be does not answer, so delivery continues through an sv-be outage. sv-be counts every slot it minted until it expires, used or not, so the agent asks for more only at startup or once one has expired. The slots are kept in `file_storage`, so a restart on the same volume keeps them. Slot URLs are credentials and are never logged.

Each record becomes one NDJSON line:

```json
{"timestamp":"2026-10-20T12:00:00.123456Z","level":"warn","status":404,"message":"GET /a HTTP/1.1 …","host":"web-1","file":"/var/log/nginx/access.log","attrs":{"method":"GET"}}
```

`timestamp` is RFC 3339 UTC with at most 6 fractional digits (the parsed time, else the observed time) and is never altered; `level` comes from the severity; `status` appears when a parser extracted one; `attrs` holds the remaining attributes. These are the field names the StreamVessel log viewer recognises.

### When something fails

| What happens | The agent |
|---|---|
| `client_secret_file` missing, empty or unreadable | logs an error naming the file; keeps the batch and retries; uses no offline slot |
| token request refused with `invalid_client` (wrong or not yet rotated secret, secret deleted, agent revoked) | logs an error, stops uploading and keeps the batches, tries again every 10 minutes. The extension sends each failed token exchange twice (it auto-detects how to pass the credentials), and sv-be limits failures to 10 per 5 minutes per client id and IP; one attempt per queue consumer every 10 minutes stays under that. Fixing the secret takes effect within 10 minutes |
| 401 `invalid_token` | retries; succeeds once the extension has a new token (it keeps its cached token until near expiry, up to about 10 minutes) |
| 403 `agent_revoked` / `scope_changed`, 400 `invalid_request` / `invalid_instance` | drops the batch (retrying cannot help) and logs the reason; on revocation discards its offline slots |
| 404 `not_found` (ingest disabled on sv-be) | backs off and keeps the batch |
| 409 `too_many_instances` / `connection_unreadable` | logs an error with sv-be's explanation; keeps the batch and retries after an hour |
| 429, 5xx (incl. 502 `storage_unavailable`), no answer, TLS failure | backs off (honouring `Retry-After`); uploads to an offline slot if it holds one |
| 412 on the upload | already stored: success |
| 403 on the upload | asks for a new URL up to 3 times, then logs a signing or clock problem and backs off |
| 400 / 409 / 501 on the upload | logs the provider's error code; after 3 refusals holds the batch for `hold_interval` until an operator fixes the connection |

### Limitations

- A restarted agent **without** a persistent volume has no offline slots until those leased by its previous run expire (up to an hour). The persistent queue still holds its data through an sv-be outage.
- Records with neither a timestamp nor an observed time (only from OTLP producers that set neither) are stamped with the time of each upload attempt, so a retry can produce a second object with the same lines.
- The binary is about 117 MB, mostly cloud SDKs pulled in by `resourcedetection`.

### Development

| Path | What |
|---|---|
| `builder-config.yaml` | `ocb` manifest; every component pinned to collector v0.163.0 / v1.69.0 |
| `cmd/sv-log-agent/` | distribution generated by `ocb` and committed (CI checks it is current); `featuregates.go` is hand-written |
| `exporter/svexporter/` | the one custom component: exporter type `sv` |
| `examples/` | `agent.yaml` (nginx), `compose/`, `kubernetes/` (DaemonSet, sidecar) |
| `packaging/` | systemd unit and environment file |
| `test/e2e/` | Docker Compose stack with a fake sv-be and MiniStack |

Requires Go 1.26.10 (the Makefile pins it through `GOTOOLCHAIN`) and Docker for the e2e test.

```sh
make test              # exporter unit tests against httptest fakes of sv-be and the bucket
make generate          # regenerate cmd/sv-log-agent with ocb v0.163.0
make check-generated   # fail if the committed distribution is stale
make build             # bin/sv-log-agent: static, -trimpath, empty build id (reproducible)
make e2e               # Compose: two sv-be outages and a SIGKILL; every line must arrive
```

The e2e test runs against `test/e2e/fakesvbe`, a stand-in for sv-be's ingest API that presigns real S3 PUTs against MiniStack, until sv-be publishes an image. Releases (`v*` tags) run goreleaser: static Linux amd64/arm64 archives, deb/rpm packages with the systemd unit, and the image `ghcr.io/streamvessel/sv-log-agent`.

### License

This repository is licensed under the [Apache License 2.0](LICENSE), the same license as the OpenTelemetry Collector the agent is built from.
