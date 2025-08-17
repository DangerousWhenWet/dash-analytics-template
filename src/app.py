#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from typing import cast

import dash
from dash import Input, Output, dcc, html
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import hjson

from pages.utils import navbar as nbar, extended_page_registry as epr


with open('dense.mantine-theme.hjson', 'r', encoding='utf8') as f:
    theme = hjson.load(f)


# NOTE: you cannot import this module from any other module. if ever you need `app`, use `dash.app` to get a reference to it.
app = dash.Dash(__name__, use_pages=True)
reg = epr.compile_registry()
nbar.initialize()

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
                "desktop": False
            },
        },

        aside={
            "width": 300,
            "breakpoint": "sm",
            "collapsed": {
                "desktop": True,
                "mobile": True
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

            nbar.get_navbar(), #type:ignore

            dmc.AppShellMain(dash.page_container),

            dmc.AppShellAside(dmc.Text("EMPTY!!!"), id="appshell-aside"),
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


if __name__ == '__main__':
    app.run(debug=True)
