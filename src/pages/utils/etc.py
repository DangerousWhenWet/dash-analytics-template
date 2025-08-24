#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import itertools
from typing import List, Iterable
from typing import Callable

from dash.development.base_component import Component
import dash_mantine_components as dmc


def make_prefixer(prefix: str) -> Callable[[str], str]:
    return lambda s: prefix + s


def interleave_with_dividers(items:Iterable[Component], divider:dmc.Divider = dmc.Divider(size="xs", color="lightgrey", my="xs")) -> List[Component]: #type:ignore
    #interleave dividers between items e.g. [item1, divider, item2, divider, item3, ...]
    return list(itertools.chain.from_iterable(zip(items, itertools.repeat(divider))))[:-1]