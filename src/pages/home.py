#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import datetime as dt
from typing import Union, Optional, List

import dash
from dash import dcc
from dash.development.base_component import Component
from dash_iconify import DashIconify
import dash_mantine_components as dmc


dash.register_page(__name__, path='/', tags=['meta'], icon='radix-icons:home')


def timeline_entry(title: str, date: Optional[Union[str, dt.date]]=None, message: Optional[str]=None, **kwargs) -> Component:
    if date is None:
        date = dt.date.today()
    return dmc.TimelineItem(
        title=title,
        children=[x for x in [
            dmc.Text(
                date if isinstance(date, str) else date.strftime("%d %b %Y"),
                size='xs', c='dimmed', #type:ignore
            ),
            (dmc.Text(message, size='md', c='dimmed') if message else None), #type:ignore

        ] if x is not None],
        **kwargs
    )


layout =[
    dmc.TypographyStylesProvider(dmc.Timeline(
        active=1,
        bulletSize=15,
        lineWidth=2,
        
        children=[
            timeline_entry(title="Dashboard Launched!"),
        ]
    ))
]
