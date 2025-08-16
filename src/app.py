#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from dataclasses import dataclass
import itertools
from typing import cast, Optional, Any, List, Iterable

import dash
from dash import html, Input, Output, State
from dash.development.base_component import Component
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import hjson
import pandas as pd

from pages.utils import extended_page_registry as epr


with open('dense.mantine-theme.hjson', 'r', encoding='utf8') as f:
    theme = hjson.load(f)


# NOTE: you cannot import this module from any other module. if ever you need `app`, use `dash.app` to get a reference to it.
app = dash.Dash(__name__, use_pages=True)
reg = epr.compile_registry()


def is_none_or_nan(x:Optional[Any]) -> bool:
    return x is None or pd.isna(x)

def interleave_with_dividers(items:Iterable[Component], divider:dmc.Divider = dmc.Divider(size="xs", color="gray", my="xs")) -> List[Component]:
    #interleave dividers between items e.g. [item1, divider, item2, divider, item3, ...]
    return list(itertools.chain.from_iterable(itertools.zip_longest(items, [divider])))


@dataclass
class NavbarSection:
    """
    outer-most grouping for navbar -- "super-category" of categories of pages
    """
    id: str
    title: str
    icon: str

NAVBAR_SECTIONS = [
    NavbarSection('navbar-segmented-category', 'By Category', 'ic:twotone-category'),
    NavbarSection('navbar-segmented-product', 'By Product', 'fluent-emoji-high-contrast:money-bag'),
]


app.layout = dmc.MantineProvider(
    theme=cast(dmc.MantineProvider.Theme, theme),
    children=dmc.AppShell(
        id='appshell',
        header={"height": 35},
        padding="md",

        navbar={
            "width": 300,
            "breakpoint": "sm",
            "collapsed": {
                "mobile": True,
                "desktop": False  # Add this - controls desktop collapse state
            },
        },

        children=[

            #top titlebar
            dmc.AppShellHeader(
                children=[
                    dmc.Group(
                        justify='space-between',
                        children=[
                            dmc.Group(
                                children=[
                                    dmc.Burger(
                                        id="mobile-burger",
                                        size="sm",
                                        hiddenFrom="sm",
                                        opened=False,
                                    ),
                                    dmc.Burger(
                                        id="desktop-burger",
                                        size="sm",
                                        visibleFrom="sm",
                                        opened=True,
                                    ),

                                    DashIconify(
                                        icon='arcticons:testy',
                                        width=30,
                                        height=30,
                                    ),

                                    dmc.Title('Demo App', c='dark', my=0, lh=1.0),

                                ],
                                h='100%',
                                p='xs',
                                m=0
                            ),

                            dmc.Switch(
                                offLabel=DashIconify(icon="radix-icons:sun", width=15, color=dmc.DEFAULT_THEME["colors"]["yellow"][8]),
                                onLabel=DashIconify(icon="radix-icons:moon", width=15, color=dmc.DEFAULT_THEME["colors"]["yellow"][6]),
                                id="color-scheme-switch",
                                persistence=True,
                                color="dark",
                            )
                        ],
                    )
                ],
            ),

            #side navbar
            dmc.AppShellNavbar(
                id='navbar',
                children=[
                    dmc.ScrollArea([
                        dmc.SegmentedControl(
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
                        ),

                        dmc.Box(id="navbar-content"),
                    ])
                ],
                p='md',

            ),

            dmc.AppShellMain(dash.page_container),
        ]
    )
)


# dark mode toggle 😎
dash.clientside_callback(
    """
    (switchOn) => {
       document.documentElement.setAttribute('data-mantine-color-scheme', switchOn ? 'dark' : 'light');
       return window.dash_clientside.no_update
    }
    """,
    Output("color-scheme-switch", "id"),
    Input("color-scheme-switch", "checked"),
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



NAVBAR_CATEGORIES = dict(
    meta = NavbarAccordion(
        id='navbar-category-meta',
        title='Meta',
        description='Miscellaneous pages and tools.',
        icon='unjs:image-meta'
    ).layout(cast(
        List[Component],
        interleave_with_dividers(
            [NavbarLink.from_page(epr.get_entry(reg.index == idx)).layout   for idx in epr.with_tags(['meta']).index]
        )
    )),

    # TODO: add more categories
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


if __name__ == '__main__':
    app.run(debug=True)
