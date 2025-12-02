#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from typing import cast

import dash
from dash import Input, Output, State, dcc, html
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import hjson
import plotly.io as pio

from backend.sql import base
from pages.utils import navbar as nbar, extended_page_registry as epr


with open('dense.mantine-theme.hjson', 'r', encoding='utf8') as f:
    theme = hjson.load(f)
base.init()

# set up chart theming
dmc.add_figure_templates()

mantine_light_with_grid = pio.templates['mantine_light'].to_plotly_json()
mantine_light_with_grid['layout']['xaxis'].update(showgrid=True)
mantine_light_with_grid['layout']['yaxis'].update(showgrid=True)
pio.templates['mantine_light_with_grid'] = mantine_light_with_grid
pio.templates.default = 'mantine_light_with_grid'

mantine_dark_with_grid = pio.templates['mantine_dark'].to_plotly_json()
mantine_dark_with_grid['layout']['xaxis'].update(showgrid=True)
mantine_dark_with_grid['layout']['yaxis'].update(showgrid=True)
pio.templates['mantine_dark_with_grid'] = mantine_dark_with_grid
pio.templates.default = 'mantine_dark_with_grid'

# NOTE: you cannot import this module from any other module. if ever you need `app`, use `dash.app` to get a reference to it.
app = dash.Dash(__name__, use_pages=True, suppress_callback_exceptions=True)
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
            dcc.Location(id='url', refresh='callback-nav'),

            #top titlebar
            dmc.AppShellHeader(
                children=[
                    dmc.Group(
                        justify='space-between',
                        children=[
                            dmc.Group(
                                children=[

                                    dmc.ActionIcon(
                                        nbar.BURGER_CLOSED,
                                        id='mobile-burger',
                                        variant='transparent',
                                        size=30,
                                        hiddenFrom="sm"
                                    ),

                                    dmc.ActionIcon(
                                        nbar.BURGER_OPEN,
                                        id='desktop-burger',
                                        variant='transparent',
                                        size=30,
                                        visibleFrom="sm"
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
    app.run(debug=True, host='0.0.0.0')
