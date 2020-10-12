import os

POSTGRES_DSN = os.environ.get('POSTGRES_DSN')

if POSTGRES_DSN is None:
    raise ValueError('Cannot run tests without POSTGRES_DSN')
