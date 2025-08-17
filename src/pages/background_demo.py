#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import time

import dash
from dash import dcc, Input, Output
from dash_iconify import DashIconify
import dash_mantine_components as dmc

from backend.jobqueue import tasks


dash.register_page(
    __name__,
    path='/background-demo',
    description='A demo of how to use celery to process long-running tasks in the background.',
    tags=['meta', 'developer', 'demo', 'suppressed']
)

layout = [
    dmc.Button(
        "Fire a blocking callback.",
        leftSection=DashIconify(icon='gravity-ui:snail'),
        id='fire-blocking-callback',
        loading=False,
    ),
    dcc.Store(id='blocking-trigger'),
    dmc.Button(
        'Fire a background callback.',
        leftSection=DashIconify(icon='carbon:message-queue'),
        id='fire-background-callback',
        loading=False,
    ),
    dcc.Store(id='background-trigger'),
    


    dmc.Switch(
        id='dummy-switch',
        label='Do something while you wait...',
        checked=True
    ),
    dmc.Space(h=10),
    dmc.Text(id="dummy-label"),
]


@dash.callback( Output('dummy-label', 'children'),   Input('dummy-switch', 'checked') )
def on_dummy_switch(checked):
    return f'Checked: {checked}'


@dash.callback(
    output={
        'blocking': {
            'loading': Output('fire-blocking-callback', 'loading', allow_duplicate=True),
            'trigger': Output('blocking-trigger', 'data'),
        },
        'background': {
            'loading': Output('fire-background-callback', 'loading', allow_duplicate=True),
            'trigger': Output('background-trigger', 'data'),
        },
    },
    inputs=[
        Input('fire-blocking-callback', 'n_clicks'),
        Input('fire-background-callback', 'n_clicks'),
    ],
    prevent_initial_call=True
)
def on_initiate_either(*_inputs):
    triggered_by = dash.ctx.triggered_id
    return {
        'blocking': {
            'loading': (triggered_by == 'fire-blocking-callback') or dash.no_update,
            'trigger': triggered_by if triggered_by == 'fire-blocking-callback' else dash.no_update,
        },
        'background': {
            'loading': (triggered_by == 'fire-background-callback') or dash.no_update,
            'trigger': triggered_by if triggered_by == 'fire-background-callback' else dash.no_update,
        },
    }


@dash.callback(
    Output('fire-blocking-callback', 'loading', allow_duplicate=True),
    Input('blocking-trigger', 'data'),
    prevent_initial_call=True
)
def on_fire_blocking_callback(_):
    triggered_by = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
    print(f"on_fire_blocking_callback triggered by {triggered_by}")

    # simulate an expensive long blocking task
    time.sleep(10)
    return False


@dash.callback(
    Output('fire-background-callback', 'loading', allow_duplicate=True),
    Input('background-trigger', 'data'),
    background=True,
    manager=tasks.manager,
    prevent_initial_call=True
)
def on_fire_background_callback(_):
    triggered_by = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
    print(f"on_fire_background_callback triggered by {triggered_by}")

    # simulate an expensive long blocking task
    time.sleep(10)
    return False
