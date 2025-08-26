#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import itertools
import json
import pathlib as pl
from typing import List, Dict, Iterable, Any, Callable

from dash.development.base_component import Component
import dash_mantine_components as dmc
import hjson
import jsonschema


THIS_DIR = pl.Path(__file__).parent.resolve()
BASE_DIR = THIS_DIR.parent.parent.resolve()


def load_config() -> Dict[str, Any]:
    with open(BASE_DIR/'config.schema.json', 'r', encoding='utf8') as f:
        config_schema = jsonschema.Draft7Validator(json.load(f))
    with open(BASE_DIR/'config.hjson', 'r', encoding='utf8') as f:
        config = hjson.load(f)
        config_schema.validate(config)
    return config


def make_prefixer(prefix: str) -> Callable[[str], str]:
    return lambda s: prefix + s


def interleave_with_dividers(items:Iterable[Component], divider:dmc.Divider = dmc.Divider(size="xs", color="lightgrey", my="xs")) -> List[Component]: #type:ignore
    #interleave dividers between items e.g. [item1, divider, item2, divider, item3, ...]
    return list(itertools.chain.from_iterable(zip(items, itertools.repeat(divider))))[:-1]