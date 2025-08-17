#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import dash
from dash import html
import dash_mantine_components as dmc

dash.register_page(__name__)

layout = dmc.Box(
    children=[
        dmc.Stack(
            children=[
                dmc.Text(
                    "404",
                    style={
                        'font-size': '10vh',
                        'font-weight': '600',
                    }
                ),
                dmc.Text(
                    "Not a page in sight...",
                    style={
                        'font-size': '2.5vh',
                        'font-style': 'italic',
                    }
                ),
            ],
            style={
                'position': 'absolute',
                'top': '0',
                'left': '0',
                'margin': '0'
            }
        ),

        dmc.Image(
            src='assets/catlock-holmes.png', 
            w='50%',
            style={
                'mask': 'linear-gradient(to bottom, black 80%, transparent 95%)',
                'WebkitMask': 'linear-gradient(to bottom, black 80%, transparent 95%)',
                'position': 'absolute',
                'bottom': '0',
                'right': '0'
            }
        ),
    ],
    style={
        'position': 'relative',
        'height': 'calc(100vh - var(--app-shell-header-height) - 2*var(--mantine-spacing-md))',
        'width': '100%'
    }
)