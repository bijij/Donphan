import os

POSTGRES_DSN: str = os.environ.get('POSTGRES_DSN')  # type: ignore

if POSTGRES_DSN is None:
    raise ValueError('Cannot run tests without POSTGRES_DSN')
