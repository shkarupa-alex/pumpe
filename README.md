# Pumping base classes

Allows fetching external data via API and store it in database.
Supports full and partial modes.

See [`pumpe/pumps/model_test.py`](https://github.com/shkarupa-alex/pumpe/blob/master/pumpe/pumps/model_test.py) for example.

## Datetimes

Datetime fields are timezone-aware and stored in UTC (SQLModel's `UTCDateTime`).
Pass aware values to `datetime` fields: they are normalized to UTC, and naive values are rejected when written.
Annotate a field with pydantic's `NaiveDatetime` to store naive values as they are.
pumpe's own timestamps keep microseconds on MySQL/MariaDB too (`DATETIME(6)`, via `PreciseUTCDateTime`).
`_fetch` receives `modified_since` and `created_after` as aware UTC datetimes.

## Primary keys

Source records must carry the model's primary key, and rows are matched by it as Python values.
If the source can send string keys that differ only in case, accents or trailing spaces, give the key column a binary
collation (MySQL/MariaDB compare strings case-insensitively by default). Otherwise such records fail the run:
with `ValueError` when a stored key matches a source key only under the collation, or with the database's
`IntegrityError` when two such spellings arrive in one batch.

## Concurrent runs

One run of a pump writes at a time, across sessions and processes: a run takes a lease in the `pump_lock` table,
and `run()` returns `None` while another run holds it.
Every write transaction renews the lease, and a run that loses it fails before writing anything more.
A lease older than `lease_timeout` (a class attribute, 10 minutes by default) is taken over, so keep it above
the longest time a run spends between two renewals: waiting for the next batch of `_fetch` plus writing a batch.
A competing run that finds the lease expired while its holder is still writing waits for the database's lock
timeout, then skips on PostgreSQL and MySQL/MariaDB. SQLite locks the whole database for a write, so there it cannot
tell that holder from any other writer and raises `database is locked` instead.

## Running

`start_pump(pump_task)` runs the task until it finishes or the process gets SIGINT or SIGTERM,
and meanwhile serves `GET /health` on `0.0.0.0:8000`.
Pass `health_host` and `health_port` to listen elsewhere, or `health_port=None` to serve nothing.
