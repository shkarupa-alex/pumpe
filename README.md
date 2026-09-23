# Pumping base classes

Allows fetching external data via API and store it in database.
Supports full and partial modes.

See `pumpe/pumps/model_test.py` for example.

## Datetimes

Datetime fields are timezone-aware and stored in UTC (SQLModel's `UTCDateTime`).
Pass aware values to `datetime` fields: they are normalized to UTC, and naive values are rejected when written.
Annotate a field with pydantic's `NaiveDatetime` to store naive values as they are.
