package observe

// Base ingest URL, shared by logs, traces and metrics — each appends its own
// suffix (/logs, /v1/traces, /v1/metrics). One override (Options.Endpoint or
// SEENTICS_INGEST_ENDPOINT) points every signal at a self-hosted deployment
// instead of three.
const defaultEndpoint = "https://api.seentics.com/api/v1/observability/ingest"
