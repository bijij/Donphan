import sys

from pathlib import Path
from unittest import TestLoader
from unittest.runner import TextTestRunner


def load_tests():
    loader = TestLoader()
    path = Path(__file__).parent
    return loader.discover(str(path), pattern='test_*.py')


if __name__ == '__main__':
    runner = TextTestRunner()
    tests = load_tests()
    result = runner.run(tests)

    sys.exit(not result.wasSuccessful())
