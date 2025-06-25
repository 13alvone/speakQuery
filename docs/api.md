# API Reference

This document describes the public API endpoints served by the Flask
application. Example requests use `curl` and assume the service is running
locally on port `5000`.

The query syntax accepted by `/api/query` and saved search routes is
derived from [`lexers/speakQuery.g4`](../lexers/speakQuery.g4).

## Query

### `POST /api/query`
Execute a SpeakQuery search and return the results. Timing metadata is
included in the response.

**Parameters**
- `query` – string containing a valid query.

**Example**
```bash
curl -X POST http://localhost:5000/api/query \
     -H 'Content-Type: application/json' \
     -d '{"query": "index=logs | head 10"}'
```

The JSON reply provides `time_sent`, `time_received` and `duration_ms`
fields so clients can measure processing latency.

## Saved Searches

### `POST /api/saved_search`
Create a saved search.

Required JSON fields include `request_id` (or `id`), `title`,
`description`, `query` and schedule information such as
`cron_schedule` and `trigger`.

```bash
curl -X POST http://localhost:5000/api/saved_search \
     -H 'Content-Type: application/json' \
     -d '{"request_id": "abc123", "title": "My search", "query": "index=logs"}'
```

### `PATCH /api/saved_search/<id>`
Update a search. All fields from the create request must be supplied.

### `DELETE /api/saved_search/<id>`
Delete a search. Repeated failed deletions from the same IP can trigger
an automatic ban when `BAN_DELETIONS_ENABLED` is true.

### `GET /api/saved_search/<id>`
Retrieve search metadata.

### `GET /api/saved_search/<id>/settings`
Return metadata plus the next scheduled runtime.

## Lookup Files

### `DELETE /api/lookup/<name>`
Remove a lookup file. Invalid attempts count toward the ban threshold
when deletion banning is enabled.

## Utility Endpoints

The following helper routes are primarily used by the web interface but
are available for API use as well.

- `POST /check_title_unique` – body: `{ "title": "name" }`.
- `POST /fetch_api_data` – body: `{ "api_url": "https://..." }`.
- `POST /run_query` – body: `{ "query": "..." }`.
- `GET /get_query_for_loadjob/<filename>` – return original query.
- `POST /save_results` – save a result set to JSON or CSV.
- `GET /get_lookup_files` – list lookup tables.
- `GET /get_loadjob_files` – list load job output files.
- `POST /upload_file` – multipart upload for lookup tables.
- `GET /view_lookup?file=<path>` – return HTML view of a lookup.
- `POST /delete_lookup_file` – body: `{ "filepath": "..." }`.
- `POST /clone_lookup_file` – body: `{ "filepath": "...", "new_name": "copy.csv" }`.
- `GET /get_settings` – fetch application settings (admin only).
- `POST /update_settings` – update settings with a `settings` object (admin only).

## Security Features

- **Rate limiting** – Controlled by the `THROTTLE_ENABLED`, `QUEUE_SIZE`
  and `PROCESSING_LIMIT` settings. When enabled, requests from one IP are
  throttled using Flask‑Limiter.
- **Ban support** – If `BAN_DELETIONS_ENABLED` is true, repeated failed
  DELETE requests cause the IP to be banned for `BAN_DURATION`
  seconds. The thresholds are defined in `routes/api.py`.
- **Timing metadata** – `/api/query` returns the timestamps `time_sent`
  and `time_received` along with `duration_ms` so consumers can audit
  response times.

