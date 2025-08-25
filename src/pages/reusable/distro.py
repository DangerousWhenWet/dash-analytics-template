#pylint: disable=missing-docstring, trailing-whitespace, line-too-long, multiple-statements, use-dict-literal
import datetime as dt
import functools
import itertools
import traceback
from typing import cast, get_args, Annotated, Optional, List, Dict, Mapping, Tuple, Literal, Union, Any, Callable, ClassVar, TypedDict, Generic, Iterable, TypeVar, Type

import dash
from dash import dcc, html, Input, Output, State
from dash_iconify import DashIconify
import dash_mantine_components as dmc
import numpy as np
import pandas as pd
from plotly.colors import qualitative as qualitative_color_scales, sequential as continuous_color_scales
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pydantic import BaseModel, Field, ValidationError

from backend.jobqueue import tasks
from backend.sql.duck import DuckDBMonitorMiddleware
from pages.utils.etc import make_prefixer
from pages.utils.extended_page_registry import PageRegistryInput


DtypeType = Literal['str', 'category', 'int', 'float', 'bool', 'date', 'datetime']
DiscreteColorScale = Literal[
    'Plotly', 'D3', 'G10', 'T10', 'Alphabet',
    'Dark24', 'Light24', 'Set1', 'Pastel1', 'Dark2',
    'Set2', 'Pastel2', 'Set3', 'Antique', 'Bold',
    'Pastel', 'Prism', 'Safe', 'Vivid'
]
ContinuousColorScale = Literal[
    'Plotly3', 'Viridis', 'Cividis', 'Inferno', 'Magma', 'Plasma', 'Turbo',
    'Blackbody', 'Bluered', 'Electric', 'Hot', 'Jet', 'Rainbow', 'Blues',
    'BuGn', 'BuPu', 'GnBu', 'Greens', 'Greys', 'OrRd', 'Oranges',
    'PuBu', 'PuBuGn', 'PuRd', 'Purples', 'RdBu', 'RdPu', 'Reds', 'YlGn',
    'YlGnBu', 'YlOrBr', 'turbid', 'thermal', 'haline', 'solar', 'ice',
    'gray', 'deep', 'dense', 'algae', 'matter', 'speed', 'amp', 'tempo',
    'Burg', 'Burgyl', 'Redor', 'Oryel', 'Peach', 'Pinkyl', 'Mint', 'Blugrn',
    'Darkmint', 'Emrld', 'Aggrnyl', 'Bluyl', 'Teal', 'Tealgrn', 'Purp',
    'Purpor', 'Sunset', 'Magenta', 'Sunsetdark', 'Agsunset', 'Brwnyl'
]
AnyColorScale = Union[DiscreteColorScale, ContinuousColorScale]
colorscale_map: Dict[str, List[str]] = \
    {k: getattr(qualitative_color_scales, k) for k in get_args(DiscreteColorScale)} | \
    {k: getattr(continuous_color_scales, k) for k in get_args(ContinuousColorScale)}


X = TypeVar('X')
class SubscriptableCycle(Generic[X]):
    '''a cycle that can be subscripted to get elements at arbitrary position within the cycle wrapping around "toroidally"'''
    def __init__(self, iterable: Iterable[X]):
        self.iterable: List[X] = list(iterable)
    
    def __getitem__(self, key) -> X:
        return self.iterable[key % len(self)]
    
    def __len__(self) -> int:
        return len(self.iterable)
    
    def __iter__(self) -> Iterable[X]:
        return itertools.cycle(self.iterable)


class PatternMatchIdType(TypedDict):
    prefix: str
    column: str
    index: int


class Column(BaseModel):
    key: str
    dtype: DtypeType
    etc: Dict[str, Any] = {}


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
        print(f"DatasourceSchema.from_df({data_name=}, {data_df.dtypes=})")
        #which_type = lambda col: str(data_df[col].dtype) if pd.api.types.is_numeric_dtype(data_df[col]) else ('category' if isinstance(data_df[col].dtype, pd.CategoricalDtype) else 'str')
        def which_type(col, data_df=data_df) -> DtypeType:
            if isinstance(data_df[col].dtype, pd.CategoricalDtype):
                return 'category'
            elif pd.api.types.is_integer_dtype(data_df[col]):
                return 'int'
            elif pd.api.types.is_float_dtype(data_df[col]):
                return 'float'
            elif pd.api.types.is_bool_dtype(data_df[col]):
                return 'bool'
            elif pd.api.types.is_datetime64_any_dtype(data_df[col]):
                return 'datetime'
            else:
                return 'str'
        def etc(col, dtype, data_df=data_df) -> Dict[str, Any]:
            if dtype == 'category':
                return {'choices': data_df[col].cat.categories.tolist()}
            return {}
        columns = [Column(key=col, dtype=which_type(col), etc=etc(col, which_type(col))) for col in data_df.columns]
        print(f"  -> {columns=}")
        return DatasourceSchema(columns=columns, name=data_name)
    
    def get_column_dtype(self, key:str) -> DtypeType:
        column = self.get_column(key)
        return column.dtype if column else 'str'

    def get_column(self, key:str) -> Optional[Column]:
        key = key.split('<<')[0]
        return next((col for col in self.columns if col.key == key), None)

class PlotSettings(BaseModel):
    x_column: Optional[str] = None
    y_column: Optional[str] = None
    z_column: Optional[str] = None
    dimensionality: Literal['2d', '3d'] = '2d'
    color_enabled: bool = False
    color_column: Optional[str] = None
    color_column_type: Optional[Literal['discrete', 'continuous']] = 'discrete'
    color_scale_name_discrete: Optional[DiscreteColorScale] = 'T10'
    color_scale_name_continuous: Optional[ContinuousColorScale] = 'thermal'
    filters: List["FilterUnionType"] = []

    @property
    def color_scale_name(self) -> Optional[str]:
        if self.color_column_type == 'discrete':
            return self.color_scale_name_discrete
        elif self.color_column_type == 'continuous':
            return self.color_scale_name_continuous
        return self.color_scale_name_discrete
    
    @color_scale_name.setter
    def color_scale_name(self, value: Optional[str]):
        if self.color_column_type == 'discrete':
            self.color_scale_name_discrete = cast(DiscreteColorScale, value)
        elif self.color_column_type == 'continuous':
            self.color_scale_name_continuous = cast(ContinuousColorScale, value)

    def get_color_scale(self, name:Optional[str]=None) -> Optional[SubscriptableCycle[str]]:
        name = name or self.color_scale_name
        return SubscriptableCycle(colorscale_map[name]) if name else None


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

    def cast(self, x: Any) -> Optional[Any]:
        return x

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
            self.value = self.cast(value)
        elif component == 'remove':
            self._flag_for_removal = True


StringOperatorType = Literal['contains', 'startswith', 'endswith', 'equals', 'regex']
class StringFilter(Filter):
    dtype: Literal['str'] = 'str'
    icon: ClassVar[str] = 'radix-icons:text'
    value: Optional[str] = None
    operator: StringOperatorType = 'contains'

    def cast(self, x: Any) -> Optional[str]:
        try:
            return str(x)
        except:
            return None

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
                                label='Logical negation: "IS" or "IS NOT"',
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
                                placeholder="Filter value...",
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


CategoryOperatorType = Literal['in']
class CategoryFilter(Filter):
    dtype: Literal['category'] = 'category'
    icon: ClassVar[str] = 'material-symbols:category-outline'
    value: List[str] = []
    choices: List[str] = []
    operator: CategoryOperatorType = 'in'

    def cast(self, x: Any) -> Optional[str]:
        try:
            return str(x)
        except:
            return None

    @property
    def layout(self):
        id_neg, _id_op, id_enab, id_val, id_del = self.dash_ids
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
                                label='Logical negation: "IS" or "IS NOT"',
                                position='top',
                                radius='xs',
                                withArrow=True,
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
                            dmc.TagsInput(
                                placeholder="Select values...",
                                id=id_val, #type:ignore
                                data=self.choices,
                                value=self.value,
                                clearable=False,
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

    def mask(self, df:pd.DataFrame) -> pd.Series:
        if any((self.value is None, self.enabled is False)): return pd.Series(True, index=df.index)
        mask = df[self.column].isin(self.value)
        return ~mask if self.negated else mask

NumericOperatorType = Literal['equal', 'greater', 'greater-equal', 'less', 'less-equal']
class _NumericFilter(Filter):
    dtype: Any = None
    icon: ClassVar[str] = "carbon:string-integer"
    value: Optional[Any] = None
    operator: NumericOperatorType = 'equal'
    allowDecimal: ClassVar[bool] = False

    def cast(self, x: Any) -> Optional[Any]:
        raise NotImplementedError("abstract method")

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
                                label='Logical negation: "IS" or "IS NOT"',
                                position='top',
                                radius='xs',
                                withArrow=True,
                            ),
                            dmc.Select(
                                id=id_op, #type: ignore
                                data=get_args(NumericOperatorType),
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
                            dmc.NumberInput(
                                id=id_val, #type:ignore
                                placeholder="Filter value...",
                                variant='default',
                                size='xs',
                                hideControls=True,
                                allowDecimal=self.allowDecimal,
                                flex=1
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

    def mask(self, df:pd.DataFrame) -> pd.Series:
        if any((self.value is None, self.value=='', self.enabled is False)): return pd.Series(True, index=df.index)
        match self.operator:
            case 'equal':           mask = df[self.column] == self.value
            case 'greater':         mask = df[self.column] > self.value
            case 'less':            mask = df[self.column] < self.value
            case 'greater-equal':   mask = df[self.column] >= self.value
            case 'less-equal':      mask = df[self.column] <= self.value
            case _: raise ValueError(f"Unknown operator: {self.operator}")
        return ~mask if self.negated else mask


class IntFilter(_NumericFilter):
    dtype: Literal['int'] = 'int'
    value: Optional[int] = None
    icon: ClassVar[str] = "carbon:string-integer"
    allowDecimal: ClassVar[bool] = False

    def cast(self, x: Any) -> Optional[int]:
        try:
            return int(x)
        except ValueError:
            return None


class FloatFilter(_NumericFilter):
    dtype: Literal['float'] = 'float'
    value: Optional[float] = None
    icon: ClassVar[str] = "lsicon:decimal-filled"
    allowDecimal: ClassVar[bool] = True
    

    def cast(self, x: Any) -> Optional[float]:
        try:
            return float(x)
        except ValueError:
            return None

    # icon_map: ClassVar[Dict[str,str]] = {
    #     'str': 'radix-icons:text',
    #     'category': 'material-symbols:category-outline',
    #     'int': 'carbon:string-integer',
    #     'float': 'lsicon:decimal-filled',
    #     'bool': 'ix:data-type-boolean',
    #     'date': 'fluent-mdl2:event-date',
    #     'datetime': 'fluent-mdl2:date-time',
    # }
FilterUnionType = FilterUnion = Annotated[
    Union[StringFilter, CategoryFilter, IntFilter, FloatFilter],
    Field(discriminator='dtype')
]
FILTER_DTYPE_MAP = {
    'str': StringFilter,
    'category': CategoryFilter,
    'int': IntFilter,
    'float': FloatFilter,
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


def set_visibility(style_dict:Dict[str, Any], visible:bool, visible_style:str='block') -> Dict[str, Any]:
    if visible:
        style_dict['display'] = visible_style
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
        self._tab_values: List[str] = ['tab-plots', 'tab-filters', 'tab-overlays', 'tab-stats', 'tab-table']
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
                                ),


                                dmc.TabsTab(
                                    dmc.Group(
                                        children=[
                                            DashIconify(
                                                icon='material-symbols-light:query-stats',
                                                width=20,
                                                height=20,
                                            ),
                                            dmc.Text("Statistics", fw=500) #type: ignore
                                        ],
                                        style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                    ),
                                    px=2,
                                    bd="1px solid var(--mantine-color-default-border)",
                                    value="tab-stats",
                                ),


                                dmc.TabsTab(
                                    dmc.Group(
                                        children=[
                                            DashIconify(
                                                icon='ph:table-thin',
                                                width=20,
                                                height=20,
                                            ),
                                            dmc.Text("Table", fw=500) #type: ignore
                                        ],
                                        style={"writingMode": "vertical-rl", "textOrientation": "mixed", 'min-width': '30px'},
                                    ),
                                    px=2,
                                    bd="1px solid var(--mantine-color-default-border)",
                                    value="tab-table",
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
                    dmc.Fieldset(
                        legend=dmc.Text('Axes:', fw=700), #type:ignore
                        variant='filled',
                        radius='xs',
                        children=[
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
                        ]
                    ),
                    dmc.Fieldset(
                        legend=dmc.Text('Colorization:', fw=700), #type:ignore
                        variant='filled',
                        radius='xs',
                        children=[
                            dmc.Group(
                                align='center',
                                children=[
                                    dmc.Select(
                                        placeholder="Pick a column...",
                                        leftSection=DashIconify(icon='ic:outline-palette', width=20,height=20),
                                        leftSectionPointerEvents='none',
                                        id=self._p('color-column-select'),
                                        data=col_dtyped_keys, #type:ignore
                                        value=None,
                                        clearable=False,
                                        renderOption={'function': "renderSelectOptionDtypeRight"},
                                        flex=1,
                                    ),
                                    dmc.Switch(
                                        id=self._p('color-enabled'),
                                        onLabel='ON', offLabel='OFF',
                                        size='sm',
                                    ),
                                ]
                            ),
                            dmc.Group(
                                id=self._p('color-scale-discrete-group'),
                                align='end', justify='space-between',
                                wrap='nowrap',
                                children=[
                                    dmc.Select(
                                        label=dmc.Text("Discrete Color Scale:", size='xs', fw=700),#type:ignore
                                        id=self._p('color-scale-discrete-select'),
                                        data=get_args(DiscreteColorScale),
                                        value='T10',
                                        clearable=False,
                                        flex=1
                                    ),
                                    dmc.Tooltip(
                                        children=[
                                            dmc.ActionIcon(
                                                DashIconify(icon='material-symbols-light:preview-sharp', width=20, height=20),
                                                id=self._p('color-scale-discrete-preview'),
                                                size='sm',
                                            )
                                        ],
                                        position='bottom',
                                        withArrow=True,
                                        label='Preview colorscales',
                                    )
                                ]
                            ),
                            dmc.Group(
                                id=self._p('color-scale-continuous-group'),
                                align='end', justify='space-between',
                                wrap='nowrap',
                                children=[
                                    dmc.Select(
                                        label=dmc.Text("Continuous Color Scale:", size='xs', fw=700),#type:ignore
                                        id=self._p('color-scale-continuous-select'),
                                        data=get_args(ContinuousColorScale),
                                        value='thermal',
                                        clearable=False,
                                        flex=1
                                    ),
                                    dmc.Tooltip(
                                        children=[
                                            dmc.ActionIcon(
                                                DashIconify(icon='material-symbols-light:preview-sharp', width=20, height=20),
                                                id=self._p('color-scale-continuous-preview'),
                                                size='sm',
                                            )
                                        ],
                                        position='bottom',
                                        withArrow=True,
                                        label='Preview colorscales',
                                    )
                                ]
                            ),
                            dmc.Modal(
                                id=self._p('color-scale-preview-modal'),
                                title="??? Color Scale Preview",
                                size='xl',
                                children=[
                                    dcc.Graph(id=self._p('color-scale-preview-graph')),
                                ]
                            )
                        ],
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


    def _tab_content_overlays(self, schema:DatasourceSchema): #pylint: disable=unused-argument
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-overlays')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-overlays')),
                dmc.Text("Overlays")
            ]
        )


    def _tab_content_statistics(self, schema:DatasourceSchema): #pylint: disable=unused-argument
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-stats')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-stats')),
                dmc.Text("Statistics")
            ]
        )


    def _tab_content_table(self, schema:DatasourceSchema): #pylint: disable=unused-argument
        return dmc.Box(
            id=dict(type=self._p('tab-content'), index=self._tab_values.index('tab-table')),
            children=[
                make_tab_close_button(dict(type=self._p('close-tab'), index='tab-table')),
                dmc.Text("Table")
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
            self._tab_content_statistics(schema),
            self._tab_content_table(schema),
        ), not schema.has_data
    

    # CALLBACK, triggered by modification of PlotSettings (specifically we care about plot_settings.filters)
    #           modifies the contents of the filters dmc.Box
    def _populate_filters(self, plot_settings):
        plot_settings = PlotSettings(**plot_settings) if plot_settings is not None else PlotSettings()
        #print(f"_populate_filters({plot_settings=})") #<-- plot_settings.filters now contains only base Filter objects
        return [f.layout for f in plot_settings.filters]
        


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
            #print(f"_manage_tab_aside_content() -> {aside=}, {tab_content_styles=}")
            return aside, tab_content_styles
        else:
            return dash.no_update, [dash.no_update]*len(tab_content_styles or [])


    # CALLBACK, triggered by click the "close" actionicon button in any tab aside on a mobile device
    #           deactivates the tab
    def _deactivate_tabs(self, any_actionicon_nclicks):
        return None if any(any_actionicon_nclicks) else dash.no_update
    

    # CALLBACK, triggered by changing the colorize column select
    #           manages visibility of either discrete/continuous color scale selects
    def _colorize_column_changed(self, colorize_column, schema):
        #print(f"_colorize_column_changed({colorize_column=}, {schema=})")
        schema = DatasourceSchema(**schema) if schema is not None else DatasourceSchema(columns=[], name="No data")
        if colorize_column is None:
            return dict(
                color_scale_discrete=set_visibility({}, False, visible_style='flex'),
                color_scale_continuous=set_visibility({}, False, visible_style='flex')
            )
        is_discrete = schema.get_column_dtype(colorize_column) in ['str', 'category', 'bool']
        return dict(
            color_scale_discrete=set_visibility({}, is_discrete, visible_style='flex'),
            color_scale_continuous=set_visibility({}, not is_discrete, visible_style='flex')
        )


    # CALLBACK, triggered by either of discrete/continuous color scale "preview" button
    #           populates the color scale preview graph and opens the modal for viewing
    def _color_scale_preview(self, nclicks, colorize_column, schema, use_dark_mode): #pylint: disable=unused-argument
        #print(f"_color_scale_preview({nclicks=}, {colorize_column=}, {schema=})")
        schema = DatasourceSchema(**schema) if schema is not None else DatasourceSchema(columns=[], name="No data")
        if colorize_column is None:
            return dict(
                color_scale_fig=dash.no_update,
                color_scale_modal=False,
                color_scale_modal_title=dash.no_update,
            )
        is_discrete = schema.get_column_dtype(colorize_column) in ['str', 'category', 'bool']
        fig = qualitative_color_scales.swatches() if is_discrete else continuous_color_scales.swatches_continuous()
        fig.update_layout(template=f"mantine_{'dark' if use_dark_mode else 'light'}_with_grid")
        return dict(
            color_scale_fig=fig,
            color_scale_modal=True,
            color_scale_modal_title=f"{'Discrete' if is_discrete else 'Continuous'} Color Scale Preview"
        )



    #CALLBACK, triggered by interaction with anything in the Plot Settings tab
    #          mutates the plot-settings Store and mutates visibility of 2D-/3D-specific controls
    def _plot_settings_changed(
                self,
                columns,
                dimensionality,
                colorization,
                global_filter_control,
                individual_filter_controls, #pylint: disable=unused-argument
                plot_settings,
                schema,
                dimensionality_controls #pylint: disable=unused-argument
            ):
        # print('='*80)
        # print(f"_plot_settings_changed({columns=}, {dimensionality=}, {global_filter_control=}, {individual_filter_controls=}, {plot_settings=}, {schema=}, {dimensionality_controls=})")
        # print(f"{dash.callback_context.triggered=}")
        # print(f"{dash.callback_context.triggered_id=}")
        # print(f"{type(dash.callback_context.triggered_id)=}")
        # print(f"{dash.callback_context.inputs_list=}")
        # print(f"{dash.callback_context.states_list=}")
        # print(f"{dash.callback_context.states=}")
        # print(f"{individual_filter_controls=}")
        
        trig_id = dash.callback_context.triggered_id
        plot_settings = PlotSettings(**plot_settings) if plot_settings else PlotSettings()
        schema = DatasourceSchema(**schema) if schema is not None else DatasourceSchema(columns=[], name="No data")

        # ===== "Plot Settings" tab ======
        plot_settings.dimensionality = dimensionality
        ids_visible_only_3d = [self._p(x)+'.style' for x in ['z-column-select',]]
        def visibility_criteria(k): return (True) if dimensionality == '3d' else (k not in ids_visible_only_3d) #pylint: disable=multiple-statements
        dimensionality_styles = {
            k: set_visibility(v or {}, visibility_criteria(k))
                for k,v in dash.callback_context.states.items()
                if k.endswith('.style')
        }
        plot_settings.x_column = columns['x'].split('<<')[0] if columns['x'] else None
        plot_settings.y_column = columns['y'].split('<<')[0] if columns['y'] else None
        if colorization['column'] is None:
            plot_settings.color_enabled = False
            plot_settings.color_column = None
            plot_settings.color_column_type = 'discrete'
            plot_settings.color_scale_name = 'T10'
        else:
            plot_settings.color_enabled = bool(colorization['enabled'])
            plot_settings.color_column = colorization['column']
            plot_settings.color_column_type = 'discrete' if schema.get_column_dtype(colorization['column']) in ['str', 'category', 'bool'] else 'continuous'
            plot_settings.color_scale_name = colorization['discrete_scale' if plot_settings.color_column_type == 'discrete' else 'continuous_scale']

        # ===== "Filters" tab ======
        # global filter controls
        if trig_id == self._p('add-filter') and global_filter_control['selected_filter_field'] is not None:
            field, dtype = global_filter_control['selected_filter_field'].split('<<')
            dtype = dtype[:-2] if dtype.endswith('>>') else dtype
            FilterType: Type[FilterUnionType] = FILTER_DTYPE_MAP[dtype] #pylint: disable=invalid-name
            #print(f"{field=}, {dtype=}, {FilterType=}")
            plot_settings.filters.append(FilterType(
                column=field,
                prefix=self._p(''),
                index=global_filter_control['add_filter'], #n-clicks
                **schema.get_column(field).etc #type: ignore
            ))
        # individual filter controls
        elif not isinstance(trig_id, str):
            trig_id = cast(Mapping[str, Any], trig_id)
            if trig_id.get('type') == 'filter':
                # 1. obtain input dict of the filter who triggered this callback
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
                # 2. obtain corresponding pydantic model for that dict
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
        print(f"_update_graph({use_dark_mode=},   {plot_settings=},   {schema=})")
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
            

            # ===== Assign Colorization Groups =====
            # even if disabled we still assign a "pseudo-group" to bin 100% of data into
            color_column = plot_settings.color_column
            if color_column: color_column = color_column.split('<<')[0]
            ser_color_group = pd.Series('all', index=df.index).astype('category')
            if color_column and plot_settings.color_enabled:
                color_column = color_column.split('<<')[0]
                if plot_settings.color_column_type == 'discrete':
                    # for discrete types, the value itself is a group identifier -- override the pseudo-group
                    ser_color_group = df[color_column].astype('category')
                # for continuous types, there is only the 1 "all" pseudo-group, but we will use the color-column as the value for color scaling


            # ===== Filters =====
            boolmasks = [f.mask(df) for f in plot_settings.filters]
            df = df[functools.reduce(lambda l,r: (l & r), boolmasks, pd.Series(True, index=df.index))]

            fig = make_subplots(
                rows=2, row_heights=[0.1, 0.9],
                cols=2, column_widths=[0.9, 0.1],
                shared_yaxes=True, shared_xaxes=True,
                vertical_spacing=0.02, horizontal_spacing=0.01
            )
            discrete_colorscale = plot_settings.get_color_scale(name=plot_settings.color_scale_name_discrete)
            continuous_color_scale = plot_settings.get_color_scale(name=plot_settings.color_scale_name_continuous).iterable #type:ignore

            for i, category in enumerate(ser_color_group.cat.categories):
                group_df = df[ser_color_group == category]
                group_x = group_df[plot_settings.x_column] if plot_settings.x_column else pd.Series(dtype='float', index=group_df.index)
                group_y = group_df[plot_settings.y_column] if plot_settings.y_column else pd.Series(dtype='float', index=group_df.index)
                group_z = group_df[plot_settings.z_column] if plot_settings.z_column else pd.Series(dtype='float', index=group_df.index)
                group_color_value = group_df[color_column] if color_column else pd.Series(dtype='float', index=group_df.index)

                if plot_settings.dimensionality == '2d':
                    fig.add_trace(
                        go.Scatter(
                            x=group_x,
                            y=group_y,
                            mode='markers',
                            name=str(category), legendgroup=str(category),
                            showlegend=True,
                            marker=functools.reduce(lambda l,r: l|r, [
                                dict(
                                    color=group_color_value,
                                    colorscale=continuous_color_scale,
                                    colorbar=dict(
                                        title=f"<b>{color_column or ''}</b>",
                                        title_font=dict(size=10)
                                    ),
                                ) if all([
                                    category == 'all',
                                    plot_settings.color_enabled,
                                    plot_settings.color_column_type == 'continuous'
                                ]) else dict(),


                                dict(color=discrete_colorscale[i]) if all([ #type:ignore
                                    category != 'all',
                                    plot_settings.color_enabled,
                                    plot_settings.color_column_type == 'discrete'
                                ]) else dict()
                            ])
                        ),
                        row=2, col=1
                    )
                    fig.add_trace(
                        go.Histogram(
                            x=group_x, nbinsx=50, bingroup=1,
                            name=str(category), legendgroup=str(category),
                            marker=dict(
                                opacity=0.5,
                                color=discrete_colorscale[i] if plot_settings.color_enabled and plot_settings.color_column_type == 'discrete' else discrete_colorscale[0] #type:ignore
                            ), 
                            showlegend=False,
                        ),
                        row=1, col=1
                    )
                    fig.add_trace(
                        go.Histogram(
                            y=group_y, nbinsy=50, bingroup=2,
                            name=str(category), legendgroup=str(category),
                            marker=dict(
                                opacity=0.5,
                                color=discrete_colorscale[i] if plot_settings.color_enabled and plot_settings.color_column_type == 'continuous' else discrete_colorscale[0] #type:ignore
                            ), 
                            showlegend=False
                        ),
                        row=2, col=2
                    )
                else:
                    raise NotImplementedError("3D plotting is not yet implemented.")

            fig.update_layout(
                title=f"<b>{data_name}:</b> {plot_settings.y_column} vs. {plot_settings.x_column}",
                margin=dict(l=0, r=0, t=40, b=10),
                barmode='overlay',
                showlegend=len(ser_color_group.cat.categories) > 1,
                legend=dict(
                    orientation='v',
                    yanchor='top', y=1.0,
                    xanchor='right', x=1.0,
                    bgcolor='rgba(255,255,255,0.8)',
                    bordercolor='rgba(0,0,0,1.0)',
                    borderwidth=1
                ),
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
            print(f"An error has occurred.\n\n{e.__class__.__name__}: {e}\n\n{traceback.format_exc()}")
            if isinstance(e, ValidationError):
                print(f"  -> {plot_settings=}")
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
            output={
                'color_scale_discrete': Output(self._p('color-scale-discrete-group'), 'style'),
                'color_scale_continuous': Output(self._p('color-scale-continuous-group'), 'style'),
            },
            inputs={
                'colorize_column': Input(self._p('color-column-select'), 'value'),
            },
            state={
                'schema': State(self._p('datasource-schema'), 'data'),
            },

            #prevent_initial_call=True
        )(self._colorize_column_changed)


        dash.callback(
            output={
                'color_scale_fig': Output(self._p('color-scale-preview-graph'), 'figure'),
                'color_scale_modal': Output(self._p('color-scale-preview-modal'), 'opened'),
                'color_scale_modal_title': Output(self._p('color-scale-preview-modal'), 'title'),
            },
            inputs={
                'nclicks':{
                    'discrete': Input(self._p('color-scale-discrete-preview'), 'n_clicks'),
                    'continuous': Input(self._p('color-scale-continuous-preview'), 'n_clicks'),
                },
            },
            state={
                'colorize_column': State(self._p('color-column-select'), 'value'),
                'schema': State(self._p('datasource-schema'), 'data'),
                'use_dark_mode': State('color-scheme-switch', 'checked')
            },

            prevent_initial_call=True
        )(self._color_scale_preview)


        dash.callback(
            output = {
                'plot_settings': Output(self._p('plot-settings'), 'data', allow_duplicate=True),
                'dimensionality_controls': [
                    Output(self._p('x-column-select'), 'style'),
                    Output(self._p('y-column-select'), 'style'),
                    Output(self._p('z-column-select'), 'style'),
                ],
            },
            inputs={
                'columns': dict(x=Input(self._p('x-column-select'), 'value'), y=Input(self._p('y-column-select'), 'value')),
                'dimensionality': Input(self._p('dimensionality'), 'value'),
                'colorization': dict(
                    enabled=Input(self._p('color-enabled'), 'checked'),
                    column=Input(self._p('color-column-select'), 'value'),
                    discrete_scale=Input(self._p('color-scale-discrete-select'), 'value'),
                    continuous_scale=Input(self._p('color-scale-continuous-select'), 'value'),
                ),
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


def demo_iris_getter() -> Tuple[str, pd.DataFrame]:
    df = DuckDBMonitorMiddleware.get_dataframe("SELECT * FROM iris;")
    df['Species'] = df['Species'].astype('category')
    df['Petal.Width'] = df['Petal.Width'].astype('int')
    return "iris", df


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
    datasource_getter=demo_iris_getter
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
