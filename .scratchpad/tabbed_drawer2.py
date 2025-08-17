#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import pathlib as pl
from typing import cast

import dash
from dash import html, Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import hjson


THIS_DIR = pl.Path(__file__).parent


with open(THIS_DIR/'..'/'src'/'dense.mantine-theme.hjson', 'r', encoding='utf8') as f:
    theme = hjson.load(f)

app = dash.Dash(__name__)
app.layout = dmc.MantineProvider(
    theme=cast(dmc.MantineProvider.Theme, theme),
    children=dmc.AppShell(
        id='appshell',
        header={"height": 35},
        padding="md",
        aside={
            "width": 300,
            "breakpoint": "md",
            "collapsed": {"desktop": False, "mobile": True},
        },

        children=[
            #top titlebar
            dmc.AppShellHeader(
                children=[
                    dmc.Group(
                        children=[
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
                ],
            ),


            dmc.AppShellMain(
                children = [
                    dmc.Group(
                        children = [
                            dmc.Center(dmc.Text("Main page stuff goes here!"),),

                            dmc.Tabs(
                                children=[dmc.TabsList([
                                    dmc.TabsTab(
                                        dmc.Group(
                                                children = [
                                                DashIconify(
                                                    icon='streamline-ultimate:analytics-graph-lines-2',
                                                    width=20,
                                                    height=20,
                                                ),
                                                dmc.Text("Plots")
                                            ],
                                            style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                        ),
                                        px=2,
                                        value="tab-plots",
                                    ),


                                    dmc.TabsTab(
                                        dmc.Group(
                                                children = [
                                                DashIconify(
                                                    icon='mdi:filter-outline',
                                                    width=20,
                                                    height=20,
                                                ),
                                                dmc.Text("Filters")
                                            ],
                                            style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                        ),
                                        px=2,
                                        value="tab-filters",
                                    ),


                                ])],
                                variant='outline',
                                orientation='vertical',
                                placement='left',
                                mr="calc(-1 * var(--app-shell-padding) - 1px)",
                            ),
                        ],
                        h='100%', w='100%',
                        justify='space-between',
                        pr=0, mr=0
                    )
                ],
            ),


            dmc.AppShellAside([
                dmc.Text("EMPTY!!!")
            ])
        ],
    )
)


# @dash.callback(
#     Output("appshell", "aside"),
#     Input("mobile-burger", "opened"),
#     Input("desktop-burger", "opened"),
#     State("appshell", "aside"),
# )
# def toggle_navbar_visible(mobile_opened, desktop_opened, aside):
#     aside["collapsed"] = {
#         "mobile": not mobile_opened,
#         "desktop": not desktop_opened,
#     }
#     return aside


if __name__ == '__main__':
    app.run(debug=True)