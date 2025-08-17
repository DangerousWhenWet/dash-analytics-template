#pylint: disable=missing-docstring, trailing-whitespace, line-too-long
import datetime as dt
from typing import cast, Optional, List, Dict, Tuple, Literal, Any, Callable

import dash
from dash import dcc, Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import pandas as pd
from pydantic import BaseModel

from backend.jobqueue import tasks
from backend.sql.duck import DuckDBMonitorMiddleware
from pages.utils.etc import make_prefixer
from pages.utils.extended_page_registry import PageRegistryInput


class Column(BaseModel):
    key: str
    dtype: Literal['str', 'int', 'float', 'bool', 'date', 'datetime']

class DatasourceSchema(BaseModel):
    columns: List[Column]
    name: str

    @staticmethod
    def from_df(data_name:str, data_df:pd.DataFrame) -> 'DatasourceSchema':
        columns = [
            Column(key=col, dtype='float' if pd.api.types.is_numeric_dtype(data_df[col]) else 'str')
            for col in data_df.columns
        ]
        return DatasourceSchema(columns=columns, name=data_name)

class PlotSettings(BaseModel):
    x_column: Optional[str] = None
    y_column: Optional[str] = None

class AppState(BaseModel):
    plot_settings: PlotSettings = PlotSettings()
    has_data: bool = False


def make_tab_close_button(tab_id:Dict[str, Any]):
    return dmc.ActionIcon(
        DashIconify(
            icon='material-symbols:tab-close-right-outline-sharp',
            width=20,
            height=20,
        ),
        id=tab_id,
        variant='transparent',
        style={
            "position": "absolute",
            "top": "0px",
            "left": "0px",
            "zIndex": 10
        }
    )


def set_visibility(style_dict:Dict[str, Any], visible:bool):
    if visible:
        style_dict['display'] = 'block'
    else:
        style_dict['display'] = 'none'
    return style_dict


class Distro:
    def __init__(   self,
                    id_prefix:str,
                    page_registry:PageRegistryInput,
                    datasource_getter:Optional[Callable[  [], Tuple[str, pd.DataFrame]  ]] = None,
                ):
        self._p = make_prefixer(id_prefix)
        self._tab_values: List[str] = ['tab-plots', 'tab-filters', 'tab-overlays']
        self._datasource_getter = datasource_getter

        dash.register_page(**(page_registry|{'layout': self.layout()}))
        self._register_callbacks()


    def layout(self):
        return [
            dcc.Store(id=self._p('datasource-schema')),
            dcc.Store(id=self._p('app-state')),

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
                                            icon='bitcoin-icons:gear-outline',
                                            width=20,
                                            height=20,
                                        ),
                                        dmc.Text("Plot Settings", fw=500) #type: ignore
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
                                            icon='stash:filter-light',
                                            width=20,
                                            height=20,
                                        ),
                                        dmc.Text("Filters", fw=500) #type: ignore
                                    ],
                                    style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                ),
                                px=2,
                                bd="1px solid var(--mantine-color-default-border)",
                                value="tab-filters",
                            ),


                            dmc.TabsTab(
                                dmc.Group(
                                        children = [
                                        DashIconify(
                                            icon='fluent:arrow-trending-lines-20-regular',
                                            width=20,
                                            height=20,
                                        ),
                                        dmc.Text("Overlays", fw=500) #type: ignore
                                    ],
                                    style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                ),
                                px=2,
                                bd="1px solid var(--mantine-color-default-border)",
                                value="tab-overlays",
                            )


                        ])],
                        variant='pills',
                        orientation='vertical',
                        placement='left',
                        mr="calc(-1 * var(--app-shell-padding))", #type: ignore
                    ),
                ],
                h='100%', w='100%',
                justify='space-between',
                pr=0, mr=0
            )
        ]


    def _tab_content_plot_settings(self, schema:DatasourceSchema):
        col_keys = [col.key for col in schema.columns]
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-plots')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-plots')),
                dmc.Stack([
                    dmc.Select(
                        #label='X:',
                        leftSection=DashIconify(icon='emojione-monotone:letter-x'),
                        id=self._p('x-column-select'),
                        data=col_keys,
                        value=col_keys[0] if len(col_keys) > 1 else None,
                        clearable=False,
                    ),
                    dmc.Select(
                        #label='Y:',
                        leftSection=DashIconify(icon='emojione-monotone:letter-y'),
                        id=self._p('y-column-select'),
                        data=col_keys,
                        value=col_keys[1] if len(col_keys) > 1 else None,
                        clearable=False,
                    )
                ])
            ]
        )


    def _tab_content_filters(self, schema:DatasourceSchema):
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-filters')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-filters')),
                dmc.Text("Filters")
            ]
        )


    def _tab_content_overlays(self, schema:DatasourceSchema):
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-overlays')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-overlays')),
                dmc.Text("Overlays")
            ]
        )



    # CALLBACK, triggered by page load or by modification of the DatasourceSchema
    #           sets the DatasourceSchema and AppState stores
    def _initialize(self, _):
        if self._datasource_getter: # developer already declared which datasource to use
            data_name, data_df = self._datasource_getter()
            schema = DatasourceSchema.from_df(data_name, data_df)
            state = AppState(
                plot_settings=PlotSettings(
                    x_column=schema.columns[0].key,
                    y_column=schema.columns[1].key if len(schema.columns) > 1 else schema.columns[0].key
                ),
                has_data=True
            )
        else:
            schema = DatasourceSchema(columns=[], name="No data")
            state = AppState(has_data=False)
        #print(f"_initialize({schema=}, {state=})")
        return schema.model_dump(), state.model_dump()



    # CALLBACK, triggered by modification of DatasourceSchema
    #           modifies the contents of the tab aside
    def _populate_aside(self, schema):
        #print(f"_populate_aside({schema=})")
        schema = DatasourceSchema(**schema) if schema is not None else DatasourceSchema(columns=[], name="No data")
        return (
            self._tab_content_plot_settings(schema),
            self._tab_content_filters(schema),
            self._tab_content_overlays(schema)
        )
        


    # CALLBACK, triggered by clicking any tab
    #           modifies collapsed-state of the tab aside as well as visibility of the various tab contents in it (via style {'display': 'none'})
    def _manage_tab_aside_content(self, active_tab, aside, tab_content_styles):
        #NOTE: tab_content_styles is indexed-alike to self._tab_values due to how we initialized the tab-content dmc.Box's id's in the layout
        print(f"_manage_tab_aside_content({active_tab=}, {aside=}, {tab_content_styles=})")
        if tab_content_styles:
            for idx, tab_value in enumerate(self._tab_values):
                tab_content_styles[idx] = set_visibility(tab_content_styles[idx] or {}, active_tab == tab_value)
            aside_hidden = active_tab not in self._tab_values
            aside["collapsed"] = {"mobile": aside_hidden, "desktop": aside_hidden}
            print(f"_manage_tab_aside_content() -> {aside=}, {tab_content_styles=}")
            return aside, tab_content_styles
        else:
            return dash.no_update, [dash.no_update]*len(tab_content_styles or [])


    # CALLBACK, triggered by click the "close" actionicon button in any tab aside on a mobile device
    #           deactivates the tab
    def _deactivate_tabs(self, any_actionicon_nclicks):
        return None if any(any_actionicon_nclicks) else dash.no_update


    def _register_callbacks(self):
        dash.callback(
            Output(self._p('datasource-schema'), 'data', allow_duplicate=True),
            Output(self._p('app-state'), 'data', allow_duplicate=True),
            Input('url', 'pathname'), # it's just here to trigger on load, we don't care about the value
            suppress_callback_exceptions=True,
            prevent_initial_call='initial-duplicate'
        )(self._initialize)

        dash.callback(
            Output('appshell-aside', 'children', allow_duplicate=True),
            Input(self._p('datasource-schema'), 'data'),
            suppress_callback_exceptions=True,
            prevent_initial_call=True
        )(self._populate_aside)

        dash.callback(
            Output('appshell', 'aside', allow_duplicate=True),
            Output(dict(type=self._p('tab-content'), index=dash.ALL), 'style', allow_duplicate=True),
            Input(self._p('tabs'), 'value'),
            State('appshell', 'aside'),
            State(dict(type=self._p('tab-content'), index=dash.ALL), 'style'),
            suppress_callback_exceptions=True,
            prevent_initial_call=True
        )(self._manage_tab_aside_content)

        dash.callback(
            Output(self._p('tabs'), 'value', allow_duplicate=True),
            Input(
                dict(type=self._p('close-tab'), index=dash.ALL),
                'n_clicks',
            ),
            suppress_callback_exceptions=True,
            prevent_initial_call=True,
        )(self._deactivate_tabs)


@tasks.queue.task(name='fetch_iris')
def _fetch_iris():
    return ("iris",   DuckDBMonitorMiddleware.get_dataframe("SELECT * FROM iris;"))

distro_demo_with_dataset = Distro(
    id_prefix='distro-demo_set-',
    page_registry=cast(PageRegistryInput, dict(
        module=__name__,
        name='Distro Demo, Baked-in Dataset',
        path='/demos/distro',
        description='Demonstration of a versatile scatterplot distribution visualizer.',
        tags=['meta', 'demo', 'reusable', 'distribution', 'scatter'],
        icon='flat-color-icons:scatter-plot',
    )),
    datasource_getter=_fetch_iris
)
