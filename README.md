# Pumping base classes

Allows fetching external data via API and store it in database.
Supports full and partial modes.

See [`pumpe/pumps/model_test.py`](https://github.com/shkarupa-alex/pumpe/blob/master/pumpe/pumps/model_test.py) for example.

## Datetimes

Datetime fields are timezone-aware and stored in UTC (SQLModel's `UTCDateTime`).
Pass aware values to `datetime` fields: they are normalized to UTC, and naive values are rejected when written.
Annotate a field with pydantic's `NaiveDatetime` to store naive values as they are.
`_fetch` receives `modified_since` and `created_after` as aware UTC datetimes.

## Concurrent runs

One run of a pump writes at a time, across sessions and processes: a run takes a lease in the `pump_lock` table,
and `run()` returns `None` while another run holds it.
Every write transaction renews the lease, and a run that loses it fails before writing anything more.
A lease older than `lease_timeout` (a class attribute, 10 minutes by default) is taken over, so keep it above
the longest time a run spends between two renewals: waiting for the next batch of `_fetch` plus writing a batch.
A competing run that finds the lease expired while its holder is still writing waits for the database's lock
timeout, then skips.

## Running

`start_pump(pump_task)` runs the task until it finishes or the process gets SIGINT or SIGTERM,
and meanwhile serves `GET /health` on `0.0.0.0:8000`.
Pass `health_host` and `health_port` to listen elsewhere, or `health_port=None` to serve nothing.
