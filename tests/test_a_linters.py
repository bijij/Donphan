# Based on flake8 test by asyncpg authors and contributors

import subprocess
import sys

from pathlib import Path
from unittest import TestCase, SkipTest


def find_root():
    return str(Path(__file__).parent.parent)


class TestLinters(TestCase):

    def test_flake8(self):
        try:
            import flake8  # NoQa
        except ImportError:
            raise SkipTest('flake8 module not installed')

        try:
            subprocess.run(
                (sys.executable, '-m', 'flake8'),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=find_root()
            )
        except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise AssertionError('flake8 file validation failed:\n{}'.format(output))

    def test_mypy(self):
        try:
            import mypy  # NoQa
        except ImportError:
            raise SkipTest('mypy module not installed')

        try:
            subprocess.run(
                (sys.executable, '-m', 'mypy', 'donphan'),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=find_root()
            )
        except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise AssertionError('mypy file validation failed:\n{}'.format(output))
