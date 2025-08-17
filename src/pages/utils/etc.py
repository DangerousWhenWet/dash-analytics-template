#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from typing import Callable


def make_prefixer(prefix: str) -> Callable[[str], str]:
    return lambda s: prefix + s