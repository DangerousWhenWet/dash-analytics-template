#pylint: disable=missing-docstring, trailing-whitespace, line-too-long
import datetime as dt
from typing import cast, Optional, List, Dict, Tuple, Literal, Any, Callable

import dash
from dash import dcc, html, Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import pandas as pd
from plotly.colors import qualitative as qualitative_color_scales
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

    @property
    def has_data(self) -> bool:
        return bool(self.columns)

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



def make_tab_close_button(tab_id:Dict[str, Any]):
    return dmc.ActionIcon(
        DashIconify(
            icon='material-symbols:tab-close-right-outline-sharp',
            width=20,
            height=20,
        ),
        id=tab_id,
        variant='transparent',
        hiddenFrom="sm",
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


def error_figure(use_dark_mode:bool, err_text:str) -> go.Figure:
    err_fig = go.Figure(layout_margin=dict(l=0, r=0, t=0, b=0))
    err_fig.add_annotation(x=0.5, xref='paper', y=0.5, yref='paper', text=err_text, showarrow=False)
    err_fig.update_layout(template=f"mantine_{'dark' if use_dark_mode else 'light'}_with_grid")
    return err_fig


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
            dcc.Store(id=self._p('plot-settings')),


            dmc.Group(
                wrap='nowrap',
                children = [
                    #dcc.Graph(id=self._p('graph')),
                    dmc.Box(
                        dcc.Graph(
                            id=self._p('graph'),
                            style={"width": "100%", "height": "100%"}
                        ),
                        style={"flex": "1", "minWidth": "0"},
                        h="100%"
                    ),

                    dmc.Tabs(
                        id=self._p("tabs"),
                        allowTabDeactivation=True,
                        color="dark",
                        autoContrast=True,
                        h='100%',

                        children=[dmc.TabsList(
                            justify='flex-start',
                            children = [
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
                            ]
                        )],
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

                dmc.ScrollArea([
                    dmc.Center(dmc.SegmentedControl(
                        id=self._p('dimensionality'),
                        value='2D',
                        data=[
                            {
                                "value": '2D',
                                "label": dmc.Center(
                                    [DashIconify(icon='gis:coord-system', width=16), html.Span('2D')],
                                    style={"gap": 10},
                                ),
                            },
                            {
                                "value": '3D',
                                "label": dmc.Center(
                                    [DashIconify(icon='gis:coord-system-3d', width=16), html.Span('3D')],
                                    style={"gap": 10},
                                ),
                            }
                        ],#type: ignore
                    )),
                    dmc.Select(
                        leftSection=DashIconify(icon='emojione-monotone:letter-x', width=20,height=20),
                        leftSectionPointerEvents='none',
                        id=self._p('x-column-select'),
                        data=col_keys,
                        value=col_keys[0] if len(col_keys) > 1 else None,
                        clearable=False,
                    ),
                    dmc.Select(
                        leftSection=DashIconify(icon='emojione-monotone:letter-y', width=20,height=20),
                        leftSectionPointerEvents='none',
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
    #           sets initial contents of all dcc.Store's
    def _initialize(self, _):
        if self._datasource_getter: # developer already declared which datasource to use
            data_name, data_df = self._datasource_getter()
            schema = DatasourceSchema.from_df(data_name, data_df)
            plot_settings = PlotSettings(
                x_column=schema.columns[0].key,
                y_column=schema.columns[1].key if len(schema.columns) > 1 else schema.columns[0].key
            )
        else:
            schema = DatasourceSchema(columns=[], name="No data")
            plot_settings = PlotSettings()
        #print(f"_initialize({schema=}, {state=})")
        return schema.model_dump(), plot_settings.model_dump()



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
        #print(f"_manage_tab_aside_content({active_tab=}, {aside=}, {tab_content_styles=})")
        if tab_content_styles:
            for idx, tab_value in enumerate(self._tab_values):
                tab_content_styles[idx] = set_visibility(tab_content_styles[idx] or {}, active_tab == tab_value)
            aside_hidden = active_tab not in self._tab_values
            aside["collapsed"] = {"mobile": aside_hidden, "desktop": aside_hidden}
            #print(f"_manage_tab_aside_content() -> {aside=}, {tab_content_styles=}")
            return aside, tab_content_styles
        else:
            return dash.no_update, [dash.no_update]*len(tab_content_styles or [])


    # CALLBACK, triggered by click the "close" actionicon button in any tab aside on a mobile device
    #           deactivates the tab
    def _deactivate_tabs(self, any_actionicon_nclicks):
        return None if any(any_actionicon_nclicks) else dash.no_update
    

    #CALLBACK, triggered by interaction with anything in the Plot Settings tab
    #          mutates the plot-settings Store
    def _plot_settings_changed(self, columns, plot_settings):
        plot_settings = PlotSettings(**plot_settings) if plot_settings else PlotSettings()
        plot_settings.x_column = columns['x']
        plot_settings.y_column = columns['y']
        return plot_settings.model_dump()

    #CALLBACK, interaction with any of PlotSettings, ..., or the theme switcher
    #          updates the graph
    def _update_graph(self, use_dark_mode, plot_settings):
        try:
            plot_settings = PlotSettings(**plot_settings) if plot_settings else PlotSettings()
            data_name, df = self._datasource_getter() #type:ignore
            fig = make_subplots(
                rows=2, row_heights=[0.1, 0.9],
                cols=2, column_widths=[0.9, 0.1],
                shared_yaxes=True, shared_xaxes=True,
                vertical_spacing=0.02, horizontal_spacing=0.01
            )
            ser_x = df[plot_settings.x_column]
            ser_y = df[plot_settings.y_column]
            fig.add_trace(go.Scatter(x=ser_x, y=ser_y, mode='markers'), row=2, col=1)
            fig.add_trace(
                go.Histogram(
                    x=ser_x, nbinsx=50, marker=dict(opacity=0.5), bingroup=1,
                    showlegend=False, name=plot_settings.x_column
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Histogram(
                    y=ser_y, nbinsy=50, marker=dict(opacity=0.5), bingroup=2,
                    showlegend=False, name=plot_settings.y_column
                ),
                row=2, col=2
            )
        # fig.update_layout(
        #     title=f"<b>{page.title}:</b> {page.plottables[range].name} vs. {page.plottables[domain].name}",
        #     margin=dict(l=0, r=0, t=40, b=10),
        #     barmode='overlay',
        #     showlegend=show_legend,
        #     legend=(legend_config|{'bgcolor':hex_to_rgba('FFFFFF', 0.500), 'orientation':legend_orientation}),
        #     boxgap=0.1,
        #     uirevision=True, # prevent automatic resize
        # )
            fig.update_layout(
                title=f"<b>{data_name}:</b> {plot_settings.y_column} vs. {plot_settings.x_column}",
                margin=dict(l=0, r=0, t=40, b=10),
                barmode='overlay',
                showlegend=True,
                legend=dict(orientation='h'),
                boxgap=0.1,
                uirevision=True, # prevent automatic resize
                template=f"mantine_{'dark' if use_dark_mode else 'light'}_with_grid",
            )
            return fig
        
        except Exception as e: #pylint: disable=broad-except
            err_text = "An error has occurred.<br><br>" + str(e.__class__.__name__) + ': ' + str(e).replace('\n', '<br>')
            err_fig = error_figure(use_dark_mode, err_text)
            return err_fig



    def _register_callbacks(self):
        dash.callback(
            Output(self._p('datasource-schema'), 'data', allow_duplicate=True),
            Output(self._p('plot-settings'), 'data', allow_duplicate=True),
            Input('url', 'pathname'), # it's just here to trigger on load, we don't care about the value
            prevent_initial_call='initial-duplicate'
        )(self._initialize)


        dash.callback(
            Output('appshell-aside', 'children', allow_duplicate=True),
            Input(self._p('datasource-schema'), 'data'),
            prevent_initial_call=True
        )(self._populate_aside)


        dash.callback(
            Output('appshell', 'aside', allow_duplicate=True),
            Output(dict(type=self._p('tab-content'), index=dash.ALL), 'style', allow_duplicate=True),
            Input(self._p('tabs'), 'value'),
            State('appshell', 'aside'),
            State(dict(type=self._p('tab-content'), index=dash.ALL), 'style'),
            prevent_initial_call=True
        )(self._manage_tab_aside_content)


        dash.callback(
            Output(self._p('tabs'), 'value', allow_duplicate=True),
            Input(
                dict(type=self._p('close-tab'), index=dash.ALL),
                'n_clicks',
            ),
            prevent_initial_call=True,
        )(self._deactivate_tabs)


        dash.callback(
            Output(self._p('plot-settings'), 'data', allow_duplicate=True),
            inputs={
                'columns': dict(x=Input(self._p('x-column-select'), 'value'), y=Input(self._p('y-column-select'), 'value')),
            },
            state=dict(plot_settings=State(self._p('plot-settings'), 'data')),
            prevent_initial_call=True
        )(self._plot_settings_changed)


        dash.callback(
            Output(self._p('graph'), 'figure', allow_duplicate=True),
            Input("color-scheme-switch", "checked"),
            Input(self._p('plot-settings'), 'data'),
            prevent_initial_call=True
        )(self._update_graph)


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
