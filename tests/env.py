import os

dsn = os.environ.get("POSTGRES_DSN")

if dsn is None:
    raise ValueError("Cannot run tests without POSTGRES_DSN")

POSTGRES_DSN = dsn
