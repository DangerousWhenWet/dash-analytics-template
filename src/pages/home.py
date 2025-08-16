#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import dash
from dash_iconify import DashIconify
import dash_mantine_components as dmc


dash.register_page(__name__, path='/', tags=['meta'], icon='radix-icons:home')


layout =[
    dmc.Alert(
    "Welcome to Dash Mantine Components",
    title="Hello!",
    color="violet",
    ),
]
