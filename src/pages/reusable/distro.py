#pylint: disable=missing-docstring
from typing import cast, Optional, Callable, List

import dash
from dash import Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import pandas as pd

from pages.utils.etc import make_prefixer
from pages.utils.extended_page_registry import PageRegistryInput


class Distro:
    def __init__(   self,
                    id_prefix:str,
                    page_registry:PageRegistryInput,
                ):
        self._p = make_prefixer(id_prefix)
        self._tab_values: List[str] = []

        dash.register_page(**(page_registry|{'layout': self.layout()}))
        self._register_callbacks()
    
    def _get_datasource(self) -> Optional[pd.DataFrame]:
        return None

    def layout(self):
        self._tab_values.extend(['tab-plots', 'tab-filters'])
        return [
            dmc.Group(
                children = [
                    dmc.Center(dmc.Text("Main page stuff goes here!"),),

                    dmc.Tabs(
                        id=self._p("tabs"),
                        allowTabDeactivation=True,
                        color="dark",
                        autoContrast=True,

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
                                bd="1px solid var(--mantine-color-default-border)",
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
                                bd="1px solid var(--mantine-color-default-border)",
                                value="tab-filters",
                            ),


                        ])],
                        variant='pills',
                        orientation='vertical',
                        placement='left',
                        mr="calc(-1 * var(--app-shell-padding) - 1px)",
                    ),
                ],
                h='100%', w='100%',
                justify='space-between',
                pr=0, mr=0
            )
        ]


    def _register_callbacks(self):
        dash.callback(
            Output('appshell-aside', 'children'),
            Output('appshell', 'aside'),
            Input(self._p('tabs'), 'value'),
            State('appshell', 'aside'),
            suppress_callback_exceptions=True
        )(self._render_tab_aside_content)


    def _render_tab_aside_content(self, active_tab, aside):
        aside_hidden = active_tab not in self._tab_values
        aside["collapsed"] = {"mobile": aside_hidden, "desktop": aside_hidden}
        if active_tab == "tab-plots":
            return "Plots", aside
        elif active_tab == "tab-filters":
            return "Filters", aside
        else:
            return "No tab selected", aside


distro_demo_with_dataset = Distro(
    id_prefix='distro-demo_set-',
    page_registry=cast(PageRegistryInput, dict(
        module=__name__,
        name='Distro Demo, Baked-in Dataset',
        path='/demos/distro',
        description='Demonstration of a versatile scatterplot distribution visualizer.',
        tags=['meta', 'demo', 'reusable', 'distribution', 'scatter'],
        icon='flat-color-icons:scatter-plot',
    ))
)
