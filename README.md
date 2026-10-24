# Post Publishing Scheduler

A beginner-friendly Node.js demo that finds due posts with `node-cron`, sends
them to Kafka's `publish-requests` topic, and consumes those requests. Failed
publishes are retried with exponential backoff, up to five attempts, before
being sent to `publish-dlq`. Event IDs and their status are saved in
`data/events.json` so a restart or duplicate Kafka delivery does not repeat a
completed post.

> **Demo publisher:** `src/consumer/consumer.js` currently logs successful
> publishing instead of calling a social-media API. Its optional `demoFailures`
> post field simulates a failed publish for retry/DLQ testing. Replace
> `publishPost` with your platform API before using this with real posts.

## Requirements

- Node.js 18 or newer (the project has been checked with Node.js 22).
- Docker Desktop for Windows, running with the Linux container engine, and its
  included Docker Compose v2.
- VS Code with the `post-publishing-scheduler` project folder opened.

The JavaScript tests use a mock Kafka client, so `npm test` can run without
Docker or a Kafka server. Sending real events and running the app requires
Kafka.

## Start Kafka and the application in VS Code PowerShell

Start Docker Desktop from the Windows Start menu and wait until it reports that
the engine is running. Open VS Code's integrated PowerShell terminal and run
these commands in order:

```powershell
Set-Location "C:\Users\Rishi Rana\OneDrive\Documents\Semester-5\Full stack\8TH EXPERIMENT\post-publishing-scheduler"
if (-not (Test-Path .\.env)) { Copy-Item .\.env.example .\.env }
docker info
docker --version
docker compose version
docker compose up -d
docker compose ps
Test-NetConnection -ComputerName localhost -Port 9092
npm install
npm test
npm start
```

The `.env.example` file contains local defaults. The conditional copy
preserves an existing `.env`; edit that local file only if different settings
are needed. Wait until `docker compose ps` shows Kafka running and
`Test-NetConnection` reports `TcpTestSucceeded : True` before running the app.
`npm install` is only needed once per checkout (or after dependency changes).
For automatic Node.js restarts while editing, use `npm run dev` instead of
`npm start`.

The included due post is queued during the first scan. Expected application
output includes lines similar to:

```text
Kafka topics ready: publish-requests, publish-dlq.
Kafka producer connected.
Kafka consumer connected; listening to publish-requests.
Post scheduler started with cron schedule "* * * * *".
Queued due post "demo-post-1" (event <event-id>).
Publishing post "demo-post-1": Hello from the Post Publishing Scheduler demo!
Successfully processed event <event-id>.
```

The event ID is a generated SHA-256 value, so its exact text will vary. Stop
the application with `Ctrl+C`; then stop Kafka:

```powershell
docker compose down
```

## Run the automated tests

From the project directory:

```powershell
npm test
```

The tests exercise Kafka topic routing with a mock client, consumer
subscription, due-post scheduling and duplicate queue prevention, exponential
delays and the five-attempt limit, DLQ routing, and event-state persistence
across both module reload and a real child-process restart. They do not connect
to Kafka. Live Kafka integration has not been verified in this environment.
A successful run ends with `# pass 8` and `# fail 0`; this verifies workflow
behavior against mocks, not a live Kafka integration.

## Test each behavior with the running app

### Successful publish

The included `demo-post-1` is already due and publishes once when first
processed. Watch the terminal for `Queued due post`, `Publishing post`, and
`Successfully processed event`.

### Retry and eventual success

Stop the app with `Ctrl+C`. Add this object to the array in `data/posts.json`
(keep the surrounding JSON commas valid), then start the app again with
`npm start`:

```json
{
  "id": "demo-retry-success",
  "content": "This demo succeeds after two simulated failures.",
  "platform": "demo",
  "scheduledAt": "2026-10-22T09:00:00.000Z",
  "demoFailures": 2
}
```

The terminal should show retry warnings for attempts 1 and 2, with waits of
1,000 ms and 2,000 ms, followed by a successful publish on attempt 3.

### Permanent failure and dead-letter routing

Add a second post with a **different ID** and `demoFailures` set to 5:

```json
{
  "id": "demo-permanent-failure",
  "content": "This demo fails all five attempts and goes to the DLQ.",
  "platform": "demo",
  "scheduledAt": "2026-10-22T09:00:00.000Z",
  "demoFailures": 5
}
```

It should fail five times, wait 1, 2, 4, and 8 seconds between attempts, then
log that it was sent to `publish-dlq`. You can inspect the topic in another
PowerShell terminal:

```powershell
Set-Location "C:\Users\Rishi Rana\OneDrive\Documents\Semester-5\Full stack\8TH EXPERIMENT\post-publishing-scheduler"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic publish-dlq --from-beginning
```

To inspect publishing requests instead, replace the topic with
`publish-requests`. These commands keep running while reading; stop them with
`Ctrl+C`.

### Duplicate prevention and restart persistence

After a post logs `Successfully processed event`, stop the app with `Ctrl+C`,
then run `npm start` again without changing that post's ID or scheduled time.
The scheduler should not queue it again. The processed status remains in
`data/events.json`, which is intentionally excluded from version control.
`npm test` additionally replays an identical request after reloading the event
tracker and verifies that the publish function is not called twice.

Changing the post's `scheduledAt` creates a new event ID; use a new post ID for
independent manual tests. To reset the local event history, stop the app and
remove only its generated event file:

```powershell
Remove-Item .\data\events.json
```

## Posts and configuration

Edit `data/posts.json` as a JSON array. Each post must include:

- `id`: a non-empty string.
- `content`: non-empty text.
- `scheduledAt`: a valid ISO-8601 date/time.

Optional post fields are passed through to the consumer. Due posts are checked
at startup and on the cron schedule. Defaults and configuration are in `.env`:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker; comma-separated for multiple brokers |
| `KAFKA_CLIENT_ID` | `post-publishing-scheduler` | Kafka client name |
| `KAFKA_GROUP_ID` | `post-publishing-scheduler` | Consumer group |
| `CRON_SCHEDULE` | `* * * * *` | Check for due posts once a minute |
| `POSTS_FILE` | `./data/posts.json` | Scheduled posts file |
| `EVENT_STORE_FILE` | `./data/events.json` | Persistent event tracking file |
| `MAX_ATTEMPTS` | `5` | Retry limit; values above five are rejected |
| `RETRY_BASE_DELAY_MS` | `1000` | First retry delay; each next delay doubles |

Do not put real platform credentials in a committed `.env` file. The current
publisher does not use credentials.

## Troubleshooting

- **`docker` is not recognized:** Install Docker Desktop for Windows from
  <https://www.docker.com/products/docker-desktop/>. If WSL is not installed,
  run `wsl --install` in an elevated PowerShell and restart Windows if
  prompted. Then run `wsl --update` and `wsl --set-default-version 2`. Install
  Docker Desktop, enable **Use the WSL 2 based engine** in **Settings →
  General**, and start Docker Desktop. Open a new VS Code PowerShell terminal
  and verify `docker --version`, `docker compose version`, and `docker info`.
- **Docker daemon is unavailable:** Start/restart Docker Desktop and wait until
  its engine is running. Then check `docker info` and retry `docker compose up
  -d`.
- **Port 9092 is unavailable:** Check for another local Kafka service with
  `Test-NetConnection -ComputerName localhost -Port 9092` and inspect
  `docker compose logs kafka`. Stop the conflicting service or change the
  Compose port and `KAFKA_BROKER` together.
- **The app cannot connect:** Check `docker compose ps`, then
  `docker compose logs kafka`. Wait for Kafka startup before running `npm
  start`; the app creates `publish-requests` and `publish-dlq` automatically.
- **A post is not queued:** Confirm valid JSON, a due `scheduledAt`, and that
  its event is not already marked in `data/events.json`. Check terminal output
  for scheduler errors.
- **A post does not publish again after an edit:** Its event identity uses the
  post ID and scheduled time. Change the scheduled time or use a new ID; do not
  delete the event store unless you intend to reset idempotency history.
