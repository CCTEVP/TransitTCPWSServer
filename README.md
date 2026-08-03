# Transit TCP WebSocket Server

This project bridges GOVI/NDOV realtime ZeroMQ feeds to browser clients over WebSocket.

## What it does

- Connects to an external ZeroMQ TCP feed (example: `tcp://pubsub.besteffort.ndovloket.nl:7658`)
- Subscribes to one or more topic prefixes (example: `/RIG/KV15messages,/RIG/KV17cvlinfo,/RIG/KV6posinfo`)
- Broadcasts only derived RET state commands to connected content WebSocket clients
- Serves the status dashboard at `/` (legacy `/status` and `/dashboard/status` redirect here)
- Serves the display client at `/content`
- Serves status JSON at `/api/status`

## Source specs used

From GOVI realtime info:

- BISON feed endpoint: `tcp://pubsub.besteffort.ndovloket.nl:7658`
- Subscription is topic-prefix based (envelope prefix), e.g. `/ARR/` or `/ARR/KV6posinfo`
- The transport is ZeroMQ PUB/SUB over TCP

Reference: https://govi.nu/realtime.html

## Setup

1. Install dependencies:

```bash
npm install
```

2. Create your env file:

```bash
copy .env.example .env
```

3. Edit `.env` values, especially `ZMQ_TOPICS`.

4. Start server:

```bash
npm start
```

5. Open:

- Status dashboard: http://localhost:8080
- Display client: http://localhost:8080/content

## Environment variables

- `ZMQ_ENDPOINT`: ZeroMQ source endpoint
- `ZMQ_TOPICS`: comma-separated topic prefixes
- `ROTTERDAM_TOPIC_PREFIX`: topic prefix for RET command derivation
- `PORT`: HTTP/WebSocket server port
- `MAX_PAYLOAD_BYTES`: drops very large payloads before broadcasting

## Status and monitoring

- Status page: `http://localhost:8080`
- Display client: `http://localhost:8080/content`
- Status API: `http://localhost:8080/api/status`

The status API includes:

- ZeroMQ upstream state (`initializing`, `connected`, `error`)
- last message time and total/dropped message counters
- list of connected WebSocket clients and client metadata

## Message shape sent to browser

**Content channel** (`/content`) commands are minimal — display clients only need
`type` and `command`. `vehiclenumber` is included when known for deduplication:

```json
{
  "type": "transit-command",
  "protocol": "RET",
  "command": "RET_TRAIN_ARRIVED",
  "receivedAt": "2026-07-23T10:00:00.000Z",
  "vehiclenumber": "2142"
}
```

**Control channel** (dashboard TCP activity) still receives full command payloads
including `entity` and `data`, plus `transit-update` feed rows.

For Rotterdam topics (`/RIG/`), only these derived train-state commands
are broadcast (`protocol: "RET"`):

- `RET_NO_TRAIN`
- `RET_TRAIN_ARRIVING_15S`
- `RET_TRAIN_ARRIVED`
- `RET_TRAIN_DEPARTED`

## Notes

- GOVI fair-use indicates a limit of one connection per datastream per consumer.
- If `payloadText` is not useful for your message type, decode `payloadBase64` in your client.

## Google Cloud Run Auto-Deploy (main branch)

This repository now includes:

- `Dockerfile` for Cloud Run container builds
- `cloudbuild.yaml` for build + push + deploy
- `.dockerignore` to keep image context clean

### One-time Google Cloud setup

1. Ensure APIs are enabled:

```bash
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
```

2. Create an Artifact Registry repository (if not already present):

```bash
gcloud artifacts repositories create cloud-run-source-deploy \
  --repository-format=docker \
  --location=europe-west4
```

3. Grant Cloud Build permission to deploy Cloud Run:

```bash
PROJECT_NUMBER=$(gcloud projects describe "$GOOGLE_CLOUD_PROJECT" --format='value(projectNumber)')
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

### Create the auto-deploy trigger

Create a Cloud Build trigger tied to your repository branch `main` using `cloudbuild.yaml`.

Console path:

- Cloud Build -> Triggers -> Create trigger
- Event: push to a branch
- Branch regex: `^main$`
- Configuration: Cloud Build configuration file
- Location: repository root
- File: `cloudbuild.yaml`

When new commits reach `main`, Cloud Build will:

1. Build the image
2. Push to Artifact Registry
3. Deploy to Cloud Run service `${_SERVICE}`

### Tuning deployment values

Edit `substitutions` in `cloudbuild.yaml`:

- `_SERVICE`: Cloud Run service name
- `_REGION`: deployment region
- `_REPOSITORY`: Artifact Registry repo
- `_ZMQ_ENDPOINT`: external feed endpoint
- `_ZMQ_TOPICS`: comma-separated topic prefixes
- `_MAX_PAYLOAD_BYTES`: message payload size cap
