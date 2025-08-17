#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from dataclasses import dataclass
import itertools
from typing import cast, Optional, Any, List, Iterable

import dash
from dash import html, Input, Output, State
from dash.development.base_component import Component
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import pandas as pd

from pages.utils import extended_page_registry as epr



def is_none_or_nan(x:Optional[Any]) -> bool:
    return x is None or pd.isna(x)

def interleave_with_dividers(items:Iterable[Component], divider:dmc.Divider = dmc.Divider(size="xs", color="lightgrey", my="xs")) -> List[Component]: #type:ignore
    #interleave dividers between items e.g. [item1, divider, item2, divider, item3, ...]
    return list(itertools.chain.from_iterable(zip(items, itertools.repeat(divider))))[:-1]



@dataclass
class NavbarSection:
    """
    outer-most grouping for navbar -- "super-category" of categories of pages
    """
    id: str
    title: str
    icon: str


@dataclass
class NavbarAccordion:
    """
    mid-level container for broad navbar groups/categories, expands to show individual items in NavbarLinks
    """
    id: str
    title: str
    description: str
    image: Optional[str] = None
    icon: Optional[str] = None

    def __post_init__(self):
        # one or the other
        assert not all([self.image, self.icon]), "Only one of `image` or `icon` should be set"
        # placeholder when none
        if all([is_none_or_nan(self.image), is_none_or_nan(self.icon)]):
            self.icon = 'streamline-plump-color:bug-flat'
        #print(self)
    
    def layout(self, content: List[Component]):
        avatar_common = dict(radius='xl', size='lg')
        avatar_display = dict(src=self.image) if self.image else dict(children=DashIconify(icon=self.icon, width=50, height=50))
        return dmc.AccordionItem(
            value=self.id,
            children=[
                dmc.AccordionControl(
                    dmc.Group([
                        dmc.Avatar(**(avatar_display | avatar_common)), #type: ignore
                        html.Div([
                            dmc.Text(self.title),
                            dmc.Text(self.description, size="sm", fw="lighter", c="gray"),
                        ]),
                    ])
                ),
                dmc.AccordionPanel(content),
            ]
        )


@dataclass
class NavbarLink:
    """
    lower-most level container for individual navbar items; these are links to pages
    """
    id: str
    title: str
    description: Optional[str]
    path: str
    image: Optional[str] = None
    icon: Optional[str] = None


    def __post_init__(self):
        # one or the other
        assert not all([self.image, self.icon]), "Only one of `image` or `icon` should be set"
        # placeholder when none
        if all([is_none_or_nan(self.image), is_none_or_nan(self.icon)]):
            self.icon = 'carbon:unknown'
        
        if self.description=='':
            self.description = None
        
        #print(self)
        
    
    @staticmethod
    def from_page(page_registry_entry:epr.PageRegistryEntry):
        return NavbarLink(
            id=page_registry_entry['path'].replace('/', '-'),
            title=page_registry_entry['title'],
            description=page_registry_entry['description'],
            path=page_registry_entry['path'],
            image=page_registry_entry['image'],
            icon=page_registry_entry['icon'],
        )
    
    @property
    def layout(self) -> Component:
        avatar_common = dict(radius='xl', size='md')
        avatar_display = dict(src=self.image) if self.image else dict(children=DashIconify(icon=self.icon, width=36, height=36))
        return dmc.NavLink(
            leftSection=dmc.Avatar(**(avatar_display | avatar_common)), #type: ignore
            label=self.title,
            href=self.path,
            description=self.description,
            styles={
                "root": {"padding": "1px 2px"},
                "description": {"lineHeight": "1.2"},
            }
        )


NAVBAR_SECTIONS: List[NavbarSection] = []
NAVBAR_CATEGORIES: dict[str, Component] = {}
NAVBAR_PRODUCTS: dict[str, Component] = {}


def initialize():
    global NAVBAR_SECTIONS, NAVBAR_CATEGORIES, NAVBAR_PRODUCTS #pylint: disable=global-statement
    NAVBAR_SECTIONS = [
        NavbarSection('navbar-segmented-category', 'By Category', 'ic:twotone-category'),
        NavbarSection('navbar-segmented-product', 'By Product', 'fluent-emoji-high-contrast:money-bag'),
    ]
    NAVBAR_CATEGORIES = dict(
        meta = NavbarAccordion(
            id='navbar-category-meta',
            title='Meta',
            description='Miscellaneous pages and tools.',
            icon='unjs:image-meta'
        ).layout(cast(
            List[Component],
            interleave_with_dividers(
                [NavbarLink.from_page(epr.get_entry(epr.epr.index == idx)).layout   for idx in epr.with_tags(tags=['meta'], blacklist=['suppressed']).index]
            )
        )),
    )
    NAVBAR_PRODUCTS = dict(
        widget1 = NavbarAccordion(
            id='navbar-product-widget1',
            title='Widget 1',
            description='The first kind of widget ever invented.',
            icon='streamline-emojis:robot-face-3'
        ).layout(cast(List[Component], [
            "foo bar"
        ])),

        widget2 = NavbarAccordion(
            id='navbar-product-widget2',
            title='Widget 2',
            description='The new hotness of widgets.',
            icon='streamline-emojis:robot-face-2'
        ).layout(cast(List[Component], [
            "baz bang"
        ]))
    )


def get_navbar():
    return dmc.AppShellNavbar(
        id='navbar',
        children=[
            dmc.ScrollArea([
                dmc.Center(dmc.SegmentedControl(
                    id="navbar-segmented-control",
                    value=NAVBAR_SECTIONS[0].id,
                    data=[
                        {
                            "value": nb.id,
                            "label": dmc.Center(
                                [DashIconify(icon=nb.icon, width=16), html.Span(nb.title)],
                                style={"gap": 10},
                            ),
                        }
                        for nb in NAVBAR_SECTIONS
                    ], #type: ignore
                    mb=10,
                )),

                dmc.Box(id="navbar-content"),
            ]),
        ],
        p='md',
    )


@dash.callback(
    Output("appshell", "navbar"),
    Input("mobile-burger", "opened"),
    Input("desktop-burger", "opened"),
    State("appshell", "navbar"),
)
def toggle_navbar_visible(mobile_opened, desktop_opened, navbar):
    navbar["collapsed"] = {
        "mobile": not mobile_opened,
        "desktop": not desktop_opened,
    }
    return navbar


@dash.callback(
    Output("navbar-content", "children"),
    Input("navbar-segmented-control", "value"),
)
def update_navbar_content(value):
    #print(f"update_navbar_content({value=})")
    match value:
        case "navbar-segmented-category":
            content = NAVBAR_CATEGORIES.values()
        case "navbar-segmented-product":
            content = NAVBAR_PRODUCTS.values()
        case _:
            return dash.no_update
    return dmc.Accordion(
        chevronPosition='right',
        variant='contained',
        children=list(content),
        className='no-bg-accordion',
    )
