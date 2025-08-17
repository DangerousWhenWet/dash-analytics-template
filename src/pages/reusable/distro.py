#pylint: disable=missing-docstring
from typing import cast, Optional, Callable

import dash
from itables.dash import ITable, ITableOutputs, updated_itable_outputs
import pandas as pd

from pages.utils.etc import make_prefixer
from pages.utils.extended_page_registry import PageRegistryInput


class Distro:
    def __init__(   self,
                    id_prefix:str,
                    page_registry:PageRegistryInput,
                ):
        self._p = make_prefixer(id_prefix)
        dash.register_page(**(page_registry|{'layout': self.layout}))
    
    def _get_datasource(self) -> Optional[pd.DataFrame]:
        return None

    def layout(self):
        return [
            "Oops! There's nothing here!!!!!"
        ]


distro_demo_with_dataset = Distro(
    id_prefix='distro-demo_set-',
    page_registry=cast(PageRegistryInput, dict(
        module=__name__,
        name='Distro Demo',
        path='/demos/distro',
        description='Demonstration of a versatile scatterplot distribution visualizer.',
        tags=['meta', 'demo', 'reusable', 'distribution', 'scatter'],
        icon='flat-color-icons:scatter-plot',
    ))
)