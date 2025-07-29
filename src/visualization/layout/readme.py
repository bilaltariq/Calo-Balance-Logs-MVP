from dash import html, dcc
import dash_bootstrap_components as dbc
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from dash import html, dcc
import dash_bootstrap_components as dbc
import os

def readme_layout():
    # Load markdown content
    md_path = "README.md"
    with open(md_path, "r", encoding="utf-8") as f:
        readme_content = f.read()

    return dbc.Container([
        html.Div([
            html.H2("Project: Calo Balance Reconciliation", className="fw-bold mb-3 text-center"),
        ], className="py-3 mb-4", style={
            "backgroundColor": "#EFEFF6",
            "borderBottom": "1px solid #DDD"
        }),

        dbc.Card(
            dbc.CardBody(
                dcc.Markdown(
                    children=readme_content,
                    dangerously_allow_html=False,
                    link_target="_blank",
                    className="p-3"
                )
            ),
            className="mb-4 shadow-sm",
            style={"backgroundColor": "#ffffff", "borderRadius": "10px"}
        )
    ], fluid=True, style={"backgroundColor": "#F8F9FA", "paddingBottom": "30px"})

