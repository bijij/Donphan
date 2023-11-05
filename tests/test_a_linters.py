# Based on flake8 test by asyncpg authors and contributors

import subprocess
import sys

from pathlib import Path
from unittest import TestCase, SkipTest


def find_root():
    return str(Path(__file__).parent.parent)


WINDOWS = sys.platform == "win32"


class LinterTest(TestCase):
    def test_black(self):
        try:
            import black  # type: ignore
        except ImportError:
            raise SkipTest("black module not installed")

        try:
            subprocess.run(
                (sys.executable, "-m", "black", "--check", "."),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=find_root(),
            )
        except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise AssertionError("black file validation failed:\n{}".format(output))

    def test_pyright(self):
        try:
            subprocess.run(
                ("pyright.cmd" if WINDOWS else "pyright"),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=find_root(),
            )
        except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise AssertionError("pyright type checking failed:\n{}".format(output))
        except FileNotFoundError:
            raise SkipTest("pyright not installed")
