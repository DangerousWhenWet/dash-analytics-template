#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import functools
from typing import cast, Iterable, Optional, List, Literal, TypedDict

import dash
from dash_iconify import DashIconify
import pandas as pd


epr = pd.DataFrame() #pylint: disable=invalid-name


def compile_extended_page_registry() -> pd.DataFrame:
    global epr #pylint: disable=global-statement
    sanitized = ( {k:v for k,v in dic.items() if k not in ('layout', 'supplied_layout')} for dic in dash.page_registry.values() )
    epr = pd.DataFrame.from_records(sanitized) \
        .sort_values('path')
        #.set_index('module')
    return epr

compile_registry = compile_extended_page_registry #alias


class PageRegistryEntry(TypedDict):
    module: str
    supplied_path: Optional[str]
    path_template: Optional[str]
    path: str
    supplied_name: Optional[str]
    name: str
    supplied_title: Optional[str]
    title: str
    description: str
    order: float
    supplied_order: Optional[float]
    tags: List[str]
    supplied_image: Optional[str]
    image: Optional[str]
    image_url: Optional[str]
    redirect_from: Optional[str]
    relative_path: str
    icon: Optional[str]


def get_entry(*boolean_masks:pd.Series, logic:Literal['any', 'all']='all') -> PageRegistryEntry:
    mask = functools.reduce(lambda l,r: (l | r) if logic == 'any' else (l & r), boolean_masks)
    entry = epr.loc[mask].iloc[0].to_dict()
    # ensure has keys for expected custom metadata fields if not present
    entry.setdefault('tags', [])
    entry.setdefault('icon', None)
    return cast(PageRegistryEntry, entry)



def with_tags(tags:Iterable[str], logic:Literal['any', 'all']='any') -> pd.DataFrame:
    """
    Return DataFrame of entries in Dash's page registry with tags matching `tags`
    """
    tags_ser = epr['tags'].explode().dropna() # explode = you get one row for each tag duplicated on the value column of the series
    match_pool = tags_ser[tags_ser.isin(tags)]
    if logic == 'any': # return results that have any one or more of the queried tags
        matching_modules = match_pool.index.unique()
        return epr.loc[matching_modules]
    else: # return only results that have *all* of the queried tags
          # i.e., those where count occurence in match_pool == len(tags)
        tag_counts = match_pool.groupby(level=0).size()
        has_all_tags = tag_counts[tag_counts == len(list(tags))]
        return epr.loc[has_all_tags.index]
