# [Ecosyste.ms: Live](https://live.ecosyste.ms)

A live SSE feed of new packages and versions across open source software ecosystems.

Internal ecosyste.ms services post events to `/ingest` and connected clients receive them on `/events` as a `text/event-stream`. The service holds a short in-memory ring buffer so reconnecting clients can replay anything they missed via `Last-Event-ID`. Events are not persisted; this is a live ticker, not a durable log.

This project is part of [Ecosyste.ms](https://ecosyste.ms): Tools and open datasets to support, sustain, and secure critical digital infrastructure.

## Endpoints

- `GET /` — HTML page that subscribes to the feed and shows events as they arrive
- `GET /events` — SSE stream. Optional query params `event`, `registry`, `ecosystem` filter the stream. Send `Last-Event-ID` (header or `?last_id=`) to replay from the ring buffer.
- `GET /status` — JSON with subscriber and event counters
- `POST /ingest` — accepts `{"events":[...]}` from internal services. Requires `Authorization: Bearer $LIVE_WEBHOOK_TOKEN` when the token is set.

Each event object is passed through unchanged as the SSE `data:` line. The server only reads `event`, `registry` and `ecosystem` from each object to support filtering; any other fields a posting service includes are forwarded as-is, so services beyond [packages](https://github.com/ecosyste-ms/packages/blob/main/docs/live-events.md) can post their own event shapes.

## Configuration

| Variable | Purpose |
|---|---|
| `PORT` | Listen port. Defaults to 3000. Dokku sets this. |
| `LIVE_WEBHOOK_TOKEN` | Shared secret for `/ingest`. If unset, ingest is open (only do that in development). |

## Development

```sh
go run .
curl http://localhost:3000/events &
curl -X POST http://localhost:3000/ingest \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"event":"version.created","registry":"rubygems.org","package":{"name":"rails"},"version":{"number":"8.1.1"}}]}'
```

```sh
go test -race ./...
```

## Deployment

Dokku builds from the `Dockerfile`. The static index page is embedded in the binary so only the single executable is needed at runtime.

The `/events` response sets `Cache-Control: no-cache` and `X-Accel-Buffering: no` and writes a heartbeat comment every 25 seconds, which keeps Cloudflare and any nginx in front from buffering or timing out the stream.
