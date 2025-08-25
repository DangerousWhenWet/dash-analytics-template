#pylint: disable=missing-docstring, trailing-whitespace, line-too-long
import datetime as dt
import functools
import itertools
import traceback
from typing import cast, get_args, Annotated, Optional, List, Dict, Mapping, Tuple, Literal, Union, Any, Callable, ClassVar, TypedDict

import dash
from dash import dcc, html, Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import pandas as pd
from plotly.colors import qualitative as qualitative_color_scales
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pydantic import BaseModel, Field

from backend.jobqueue import tasks
from backend.sql.duck import DuckDBMonitorMiddleware
from pages.utils.etc import make_prefixer, interleave_with_dividers
from pages.utils.extended_page_registry import PageRegistryInput


class PatternMatchIdType(TypedDict):
    prefix: str
    column: str
    index: int


class Column(BaseModel):
    key: str
    dtype: Literal['str', 'category', 'int', 'float', 'bool', 'date', 'datetime']
    # icon_map: ClassVar[Dict[str,str]] = {
    #     'str': 'radix-icons:text',
    #     'category': 'material-symbols:category-outline',
    #     'int': 'carbon:string-integer',
    #     'float': 'lsicon:decimal-filled',
    #     'bool': 'ix:data-type-boolean',
    #     'date': 'fluent-mdl2:event-date',
    #     'datetime': 'fluent-mdl2:date-time',
    # }

    @property
    def dtyped_key(self) -> str:
        return f"{self.key}<<{self.dtype}>>"


class DatasourceSchema(BaseModel):
    columns: List[Column]
    name: str
    query: Optional[str] = None

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
    z_column: Optional[str] = None
    dimensionality: Literal['2d', '3d'] = '2d'
    filters: List["FilterUnionType"] = []


class FilterPatternMatchIdType(PatternMatchIdType):
    # keys: prefix, column, index, component
    component: Literal['negate', 'operator', 'enable', 'value', 'remove']

class Filter(BaseModel):
    _flag_for_removal: bool = False
    column: str
    enabled: bool = True
    negated: bool = False
    prefix: str # get this from the Distro owner object
    index: int  # get this from `n_click` prop of the "add filter" button
    # the following attributes exist for all Filter subclasses but their exact type depends on the subclass. override in subclasses
    operator: Optional[Any] = None
    value: Optional[Any] = None

    # @property
    # def layout(self):
    #     raise NotImplementedError

    @property
    def dash_ids(self) -> Tuple[FilterPatternMatchIdType, FilterPatternMatchIdType, FilterPatternMatchIdType, FilterPatternMatchIdType, FilterPatternMatchIdType]:
        """returns (
            `id_of_negate_toggle`, 
            `id_of_filter_operator_select`, 
            `id_of_enable_checkbox`,
            `id_of_filter_value_input`,
            `id_of_remove_filter_button`
        )"""
        #return dict(prefix=self.prefix, column=self.column, index=self.index)
        return (
            dict(type='filter', prefix=self.prefix, column=self.column, index=self.index, component='negate'),
            dict(type='filter', prefix=self.prefix, column=self.column, index=self.index, component='operator'),
            dict(type='filter', prefix=self.prefix, column=self.column, index=self.index, component='enable'),
            dict(type='filter', prefix=self.prefix, column=self.column, index=self.index, component='value'),
            dict(type='filter', prefix=self.prefix, column=self.column, index=self.index, component='remove'),
        ) #type: ignore

    def mutate(self, callback_input:Dict[str, Any]):
        component = callback_input['id']['component']
        value = callback_input['value']
        if component == 'negate':
            self.negated = value
        elif component == 'operator':
            self.operator = value
        elif component == 'enable':
            self.enabled = value
        elif component == 'value':
            self.value = value
        elif component == 'remove':
            self._flag_for_removal = True


StringOperatorType = Literal['contains', 'startswith', 'endswith', 'equals', 'regex']
class StringFilter(Filter):
    dtype: Literal['str'] = 'str'
    icon: ClassVar[str] = 'radix-icons:text'
    value: Optional[str] = None
    operator: Optional[StringOperatorType] = 'contains'

    @property
    def layout(self):
        id_neg, id_op, id_enab, id_val, id_del = self.dash_ids
        return dmc.Card(
            withBorder=True,
            children=[
                dmc.CardSection(children=[
                    dmc.Group(
                        wrap='nowrap',
                        children=[
                            DashIconify(icon=self.icon, width=20, height=20),
                            dmc.Tooltip(
                                children=dmc.Text(
                                    children=self.column,
                                    size="sm",
                                    fw="bold",
                                    truncate='end',
                                    flex=1,
                                    # make it look like a dmc.Code
                                    c="var(--mantine-color-text)", #type: ignore
                                    bg="var(--mantine-color-gray-1)", #type: ignore
                                    ff="monospace",
                                    px=1,
                                    py=1,
                                    style={"borderRadius": "4px", "border": "1px solid var(--mantine-color-gray-3)"}
                                ),
                                label=self.column,
                                position='top',
                                radius='xs',
                                withArrow=True,
                                boxWrapperProps={'flex': '1'},
                            ),
                            dmc.Tooltip(
                                children=dmc.Switch(
                                    id=id_neg, #type: ignore
                                    offLabel=DashIconify(icon="mdi:equal", width=15,),
                                    onLabel=DashIconify(icon="ic:baseline-not-equal", width=15,),
                                    checked=self.negated
                                ),
                                label="Logical negation: EQUAL or NOT EQUAL",
                                position='top',
                                radius='xs',
                                withArrow=True,
                            ),
                            dmc.Select(
                                id=id_op, #type: ignore
                                data=get_args(StringOperatorType),
                                value=self.operator,
                                clearable=False,
                                size="xs",
                                w='33%',
                            ),
                            dmc.Tooltip(
                                children=dmc.Checkbox(
                                    id=id_enab, #type: ignore
                                    checked=self.enabled,
                                    size="xs",
                                ),
                                label="Enable/disable filter",
                                position='top',
                                radius='xs',
                                withArrow=True,
                            )
                        ]
                    ),
                    dmc.Group(
                        wrap='nowrap',
                        children=[
                            dmc.TextInput(
                                id=id_val, #type: ignore
                                value=self.value,
                                placeholder="Filter value",
                                size="xs",
                                flex=1,
                            ),
                            dmc.Tooltip(
                                children=dmc.ActionIcon(
                                    DashIconify(icon='material-symbols:close', width=20, height=20),
                                    id=id_del, #type: ignore
                                    variant='transparent',
                                    size='xs',
                                ),
                                label="Remove filter",
                                position='top',
                                radius='xs',
                                withArrow=True,
                            )
                        ]
                    )
                ])
            ]
        )


    def mask(self, df:pd.DataFrame,) -> pd.Series:
        if any((self.value is None, self.value=='', self.enabled is False)): return pd.Series(True, index=df.index)
        match self.operator:
            case 'contains':    mask = df[self.column].str.contains(self.value, na=False) #type: ignore
            case 'startswith':  mask = df[self.column].str.startswith(self.value, na=False) #type: ignore
            case 'endswith':    mask = df[self.column].str.endswith(self.value, na=False) #type: ignore
            case 'equals':      mask = df[self.column] == self.value
            case 'regex':       mask = df[self.column].str.contains(self.value, regex=True, case=False, na=False) #type: ignore
            case _: raise ValueError(f"Unknown operator: {self.operator}")
        return ~mask if self.negated else mask

FilterUnionType = FilterUnion = Annotated[
    Union[StringFilter,],
    Field(discriminator='dtype')
]
FILTER_DTYPE_MAP = {
    'str': StringFilter,
    # 'category': CategoryFilter,
    # 'int': IntFilter,
    # 'float': FloatFilter,
    # 'bool': BoolFilter,
    # 'date': DateFilter,
    # 'datetime': DateTimeFilter
}


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

        dash.register_page(**(page_registry|{'layout': self.layout}))
        self._register_callbacks()


    def layout(self):
        return [
            dcc.Store(id=self._p('datasource-schema'), data=None),
            dcc.Store(id=self._p('plot-settings'), data=None),

            dmc.Modal(
                title=[dmc.Text([
                    "There is no datasource selected for visualization. Please select a built-in source below, or ",
                    dmc.Anchor("upload your own data first at the Data Gallery", size='sm', href="/data-gallery", target="_blank"),
                    "."
                ], size='sm')],
                id=self._p('select-datasource-modal'),
                size='lg',
                centered=True,
                closeOnClickOutside=False,
                children=[
                    dmc.Select(
                        id=self._p('select-datasource'),
                        label='Built-in Datasets',
                        placeholder='Select one...',
                        data=DuckDBMonitorMiddleware.ask_available_tables(),
                    ),
                    dmc.Button('Confirm', id=self._p('confirm-datasource'), mt=2),
                ]
            ),

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
        col_dtyped_keys = [{'value': col.dtyped_key, 'label': col.key} for col in schema.columns]
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-plots')),
            p=2,
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-plots')),

                dmc.ScrollArea([
                    dmc.Center(dmc.SegmentedControl(
                        id=self._p('dimensionality'),
                        value='2d',
                        data=[
                            {
                                "value": '2d',
                                "label": dmc.Center(
                                    [DashIconify(icon='gis:coord-system', width=16), html.Span('2D')],
                                    style={"gap": 10},
                                ),
                            },
                            {
                                "value": '3d',
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
                        data=col_dtyped_keys, #type:ignore
                        value=col_dtyped_keys[0]['value'] if len(col_dtyped_keys) > 1 else None,
                        clearable=False,
                        renderOption={'function': "renderSelectOptionDtypeRight"},
                    ),
                    dmc.Select(
                        leftSection=DashIconify(icon='emojione-monotone:letter-y', width=20,height=20),
                        leftSectionPointerEvents='none',
                        id=self._p('y-column-select'),
                        data=col_dtyped_keys, #type:ignore
                        value=col_dtyped_keys[1]['value'] if len(col_dtyped_keys) > 1 else None,
                        clearable=False,
                        renderOption={'function': "renderSelectOptionDtypeRight"},
                    ),
                    dmc.Select(
                        leftSection=DashIconify(icon='emojione-monotone:letter-z', width=20,height=20),
                        leftSectionPointerEvents='none',
                        id=self._p('z-column-select'),
                        data=col_dtyped_keys, #type:ignore
                        value=col_dtyped_keys[2]['value'] if len(col_dtyped_keys) > 1 else None,
                        clearable=False,
                        renderOption={'function': "renderSelectOptionDtypeRight"},
                    ),
                    dmc.Divider(my=4),
                    dmc.Select(
                        label="Colorize on:",
                        leftSection=DashIconify(icon='ic:outline-palette', width=20,height=20),
                        leftSectionPointerEvents='none',
                        id=self._p('color-column-select'),
                        data=col_dtyped_keys, #type:ignore
                        value=col_dtyped_keys[0]['value'] if len(col_dtyped_keys) > 1 else None,
                        clearable=False,
                        renderOption={'function': "renderSelectOptionDtypeRight"},
                    )
                ])
            ]
        )


    def _tab_content_filters(self, schema:DatasourceSchema):
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-filters')),
            p=2,
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-filters')),
                dmc.Group(
                    children=[
                        dmc.Select(
                            id=self._p('select-filter-field'),
                            data=[{'value': f"{col.key}<<{col.dtype}>>", 'label': col.key} for col in schema.columns], #type: ignore
                            value=None,
                            placeholder="Select a field to filter on",
                            clearable=False,
                            searchable=True,
                            renderOption={'function': "renderSelectOptionDtypeRight"},
                            flex=1,
                        ),
                        dmc.ActionIcon(
                            DashIconify(icon='icons8:plus', width=20, height=20),
                            id=self._p('add-filter'),
                            size='sm',
                        ),
                    ],
                ),
                dmc.Divider(my=4),
                dmc.Center(dmc.Button(
                        "Clear Filters",
                        id=self._p('clear-filters'),
                        leftSection=DashIconify(icon='carbon:erase', width=20, height=20),
                        color='red',
                ), mb=2),
                dmc.Box(id=self._p('filters'), children=[], p=2),
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
            show_modal=False
        else:
            schema = DatasourceSchema(columns=[], name="No data")
            plot_settings = PlotSettings()
            show_modal=True
        
        return show_modal, schema.model_dump(), plot_settings.model_dump()


    # CALLBACK, triggered by confirm button in modal
    #           modifies DatasourceSchema according to user's selected datasource (also hides the modal)
    def _confirm_datasource(self, _, data_name):
        if data_name is None:
            #print("_confirm_datasource() -> data_name is None")
            return dash.no_update, dash.no_update
        data_df = DuckDBMonitorMiddleware.get_dataframe(f"SELECT * FROM {data_name};")
        schema = DatasourceSchema.from_df(data_name, data_df)
        plot_settings = PlotSettings(
            x_column=schema.columns[0].key,
            y_column=schema.columns[1].key if len(schema.columns) > 1 else schema.columns[0].key
        )
        return False, schema.model_dump(), plot_settings.model_dump()
        
        


    # CALLBACK, triggered by modification of DatasourceSchema
    #           modifies the contents of the tab aside
    def _populate_aside(self, schema):
        #print(f"_populate_aside({schema=})")
        schema = DatasourceSchema(**schema) if schema is not None else DatasourceSchema(columns=[], name="No data")
        return (
            self._tab_content_plot_settings(schema),
            self._tab_content_filters(schema),
            self._tab_content_overlays(schema),
        ), not schema.has_data
    

    # CALLBACK, triggered by modification of PlotSettings (specifically we care about plot_settings.filters)
    #           modifies the contents of the filters dmc.Box
    def _populate_filters(self, plot_settings):
        plot_settings = PlotSettings(**plot_settings) if plot_settings is not None else PlotSettings()
        #print(f"_populate_filters({plot_settings=})") #<-- plot_settings.filters now contains only base Filter objects
        return interleave_with_dividers([f.layout for f in plot_settings.filters])
        


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
    #          mutates the plot-settings Store and mutates visibility of 2D-/3D-specific controls
    def _plot_settings_changed(
                self,
                columns,
                dimensionality,
                global_filter_control,
                individual_filter_controls,
                plot_settings,
                schema,
                dimensionality_controls
            ):
        print('='*80)
        print(f"_plot_settings_changed({columns=}, {dimensionality=}, {global_filter_control=}, {individual_filter_controls=}, {plot_settings=}, {schema=}, {dimensionality_controls=})")
        # print(f"{dash.callback_context.triggered=}")
        # print(f"{dash.callback_context.triggered_id=}")
        # print(f"{type(dash.callback_context.triggered_id)=}")
        #print(f"{dash.callback_context.inputs_list=}")
        #print(f"{dash.callback_context.states_list=}")
        print(f"{dash.callback_context.states=}")
        # print(f"{individual_filter_controls=}")
        
        trig_id = dash.callback_context.triggered_id
        plot_settings = PlotSettings(**plot_settings) if plot_settings else PlotSettings()

        # ===== "Plot Settings" tab ======
        plot_settings.dimensionality = dimensionality
        # modify visibility; set {display: none} or {display: block} as necessary
        ids_visible_only_3d = [self._p(x)+'.style' for x in ['z-column-select',]]
        #print(f"{ids_visible_only_3d=}")
        def visibility_criteria(k): return (True) if dimensionality == '3d' else (k not in ids_visible_only_3d) #pylint: disable=multiple-statements
        dimensionality_styles = {
            k: set_visibility(v or {}, visibility_criteria(k))
                for k,v in dash.callback_context.states.items()
                if k.endswith('.style')
        }
        #print(f"{dimensionality_styles=}")
        plot_settings.x_column = columns['x'].split('<<')[0] if columns['x'] else None
        plot_settings.y_column = columns['y'].split('<<')[0] if columns['y'] else None

        # ===== "Filters" tab ======
        # global filter controls
        if trig_id == self._p('add-filter') and global_filter_control['selected_filter_field'] is not None:
            field, dtype = global_filter_control['selected_filter_field'].split('<<')
            dtype = dtype[:-2] if dtype.endswith('>>') else dtype
            FilterType = FILTER_DTYPE_MAP[dtype] #pylint: disable=invalid-name
            plot_settings.filters.append(FilterType(
                column=field,
                prefix=self._p(''),
                index=global_filter_control['add_filter'] #n-clicks
            ))
        # individual filter controls
        elif not isinstance(trig_id, str):
            trig_id = cast(Mapping[str, Any], trig_id)
            if trig_id.get('type') == 'filter':
                # dash.callback_context.inputs_list is List[Union[Dict[str, Any], List[Dict[str, Any]]]]
                # we need to flatten it in order to seek the input that triggered the callback
                trig_input: Optional[Dict[str, Any]] = next(
                    (x for x in itertools.chain.from_iterable(
                        item if isinstance(item, list) else [item] 
                        for item in dash.callback_context.inputs_list
                    ) if x['id'] == trig_id),
                    None
                )
                #print(f"{trig_input=}")
                corresponding_filter: Optional[FilterUnionType] = next(
                    (f for f in plot_settings.filters if all((
                        f.column == trig_id['column'],
                        f.index == trig_id['index'],
                    ))),
                    None
                )
                #print(f"{corresponding_filter=}")
                if trig_input is not None and corresponding_filter is not None:
                    corresponding_filter.mutate(trig_input)
                    for f in plot_settings.filters:
                        if f._flag_for_removal: #pylint: disable=protected-access
                            print(f"DESTROYING {f=}")
                            plot_settings.filters.remove(f)


        return {
            'plot_settings': plot_settings.model_dump(),
            'dimensionality_controls': list(dimensionality_styles.values()),
        }


    #CALLBACK, interaction with any of PlotSettings, ..., or the theme switcher
    #          updates the graph
    def _update_graph(self, use_dark_mode, plot_settings, schema):
        #print(f"_update_graph({use_dark_mode=},   {plot_settings=},   {schema=})")
        try:
            plot_settings = PlotSettings(**plot_settings) if plot_settings else PlotSettings()
            schema = DatasourceSchema(**schema) if schema else DatasourceSchema(columns=[], name="No data")
            if schema.has_data is False:
                #print("_update_graph() -> schema.has_data is False")
                #raise ValueError("No data to plot.")
                err_text = "An error has occurred.<br><br>ValueError: No data to plot."
                err_fig = error_figure(use_dark_mode, err_text)
                return err_fig
            if self._datasource_getter:
                data_name, df = self._datasource_getter()
            else:
                data_name = schema.name
                df = DuckDBMonitorMiddleware.get_dataframe(f"SELECT * FROM {data_name};")
            
            # ===== Filters =====
            boolmasks = [f.mask(df) for f in plot_settings.filters]
            df = df[functools.reduce(lambda l,r: (l & r), boolmasks, pd.Series(True, index=df.index))]

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
            # import json
            # print(len(json.dumps(fig.to_dict(), indent=2)))
            return fig
        
        except Exception as e: #pylint: disable=broad-except
            #print(f"Exception in _update_graph: {e.__class__.__name__}: {e}")
            traceback_text = traceback.format_exc().replace('\n', '<br>')
            err_text = "An error has occurred.<br><br>" + str(e.__class__.__name__) + ': ' + str(e).replace('\n', '<br>')
            err_text += '<br><br>' + traceback_text
            err_fig = error_figure(use_dark_mode, err_text)
            return err_fig



    def _register_callbacks(self):
        dash.callback(
            Output(self._p('select-datasource-modal'), 'opened', allow_duplicate=True),
            Output(self._p('datasource-schema'), 'data', allow_duplicate=True),
            Output(self._p('plot-settings'), 'data', allow_duplicate=True),
            Input('url', 'pathname'), # it's just here to trigger on load, we don't care about the value

            # background=True,
            # manager=tasks.manager,
            prevent_initial_call='initial-duplicate'
        )(self._initialize)


        dash.callback(
            Output(self._p('select-datasource-modal'), 'opened', allow_duplicate=True),
            Output(self._p('datasource-schema'), 'data', allow_duplicate=True),
            Output(self._p('plot-settings'), 'data', allow_duplicate=True),
            Input(self._p('confirm-datasource'), 'n_clicks'),
            State(self._p('select-datasource'), 'value'),

            # background=True,
            # manager=tasks.manager,
            prevent_initial_call=True
        )(self._confirm_datasource)


        dash.callback(
            Output('appshell-aside', 'children', allow_duplicate=True),
            Output(self._p('select-datasource-modal'), 'opened', allow_duplicate=True),
            Input(self._p('datasource-schema'), 'data'),

            prevent_initial_call=True
        )(self._populate_aside)


        dash.callback(
            Output(self._p('filters'), 'children', allow_duplicate=True),
            Input(self._p('plot-settings'), 'data'),

            prevent_initial_call=True
        )(self._populate_filters)


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
            output = {
                'plot_settings': Output(self._p('plot-settings'), 'data', allow_duplicate=True),
                'dimensionality_controls': [
                    Output(self._p('x-column-select'), 'style'),
                    Output(self._p('y-column-select'), 'style'),
                    Output(self._p('z-column-select'), 'style'),
                ]
            },
            inputs={
                'columns': dict(x=Input(self._p('x-column-select'), 'value'), y=Input(self._p('y-column-select'), 'value')),
                'dimensionality': Input(self._p('dimensionality'), 'value'),
                'global_filter_control': {
                    'selected_filter_field': Input(self._p('select-filter-field'), 'value'),
                    'add_filter': Input(self._p('add-filter'), 'n_clicks'),
                    'clear_filters': Input(self._p('clear-filters'), 'n_clicks'),
                },
                'individual_filter_controls': {
                    'negate': Input(dict(type='filter',  prefix=self._p(''), component='negate', column=dash.ALL, index=dash.ALL), 'checked'),
                    'operator': Input(dict(type='filter', prefix=self._p(''), component='operator', column=dash.ALL, index=dash.ALL), 'value'),
                    'enable': Input(dict(type='filter',  prefix=self._p(''), component='enable', column=dash.ALL, index=dash.ALL), 'checked'),
                    'value': Input(dict(type='filter',  prefix=self._p(''), component='value', column=dash.ALL, index=dash.ALL), 'value'),
                    'remove': Input(dict(type='filter',  prefix=self._p(''), component='remove', column=dash.ALL, index=dash.ALL), 'n_clicks'),
                },
            },
            state=dict(
                plot_settings=State(self._p('plot-settings'), 'data'),
                schema=State(self._p('datasource-schema'), 'data'),
                dimensionality_controls=[
                    State(self._p('x-column-select'), 'style'),
                    State(self._p('y-column-select'), 'style'),
                    State(self._p('z-column-select'), 'style'),
                ]
            ),

            prevent_initial_call=True
        )(self._plot_settings_changed)


        dash.callback(
            Output(self._p('graph'), 'figure'),
            Input("color-scheme-switch", "checked"),
            Input(self._p('plot-settings'), 'data'),
            Input(self._p('datasource-schema'), 'data'),

            background=True,
            manager=tasks.manager,
            prevent_initial_call=True,
            #cache_args_to_ignore=[0, 1, 2]  # ignore plot-settings and datasource-schema
            #cache_by=[] # disable cache
        )(self._update_graph)


distro_demo_with_dataset = Distro(
    id_prefix='distro-demo_set-',
    page_registry=cast(PageRegistryInput, dict(
        module="distro_bakedin",
        name='Distro Demo, Baked-in Dataset',
        path='/demos/distro/baked-in',
        description='Demonstration of a versatile scatterplot distribution visualizer.',
        tags=['meta', 'demo', 'reusable', 'distribution', 'scatter'],
        icon='flat-color-icons:scatter-plot',
    )),
    datasource_getter=lambda: ("iris",   DuckDBMonitorMiddleware.get_dataframe("SELECT * FROM iris;"))
)


distro_demo_without_dataset = Distro(
    id_prefix='distro-demo_byod-',
    page_registry=cast(PageRegistryInput, dict(
        module="distro_byod",
        name='Distro Demo, BYO-Dataset',
        path='/demos/distro/byod',
        description='Demonstration of a versatile scatterplot distribution visualizer.',
        tags=['meta', 'demo', 'reusable', 'distribution', 'scatter'],
        icon='flat-color-icons:scatter-plot',
    ))
)