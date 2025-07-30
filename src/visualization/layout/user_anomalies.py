from dash import Dash, dcc, html, dash_table, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database

def anomalies_layout():
    return dbc.Container([

        # Page Title
        dbc.Row([
            dbc.Col(html.H2("User-Level Anomalies Dashboard", className="text-center my-3"), width=12)
        ]),

        # Global Filters Card
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Global Filters"),
                    dbc.CardBody([
                        html.Div(
                            "Use these filters to refine anomaly analysis for specific users, countries, mismatch types, or time periods. "
                            "These filters apply to all anomaly visualizations on this page.",
                            className="text-muted mb-3"
                        ),
                        dbc.Row([

                            dbc.Col(
                                dcc.Dropdown(
                                    id='anomaly-country-filter',
                                    placeholder="Filter by Country",
                                    multi=False,
                                    className="mb-2",
                                    value='Bahrain'
                                ),
                                width=3
                            ),

                            dbc.Col(
                                dcc.Dropdown(
                                    id='anomaly-mismatch-type-filter',
                                    placeholder="Filter by Mismatch Type",
                                    multi=True,
                                    className="mb-2",
                                    value=['CALCULATION ISSUE', 'BALANCE SYNC ISSUE'] 

                                ),
                                width=3
                            ),

                            # User filter
                            dbc.Col(
                                dcc.Dropdown(
                                    id='anomaly-user-filter',
                                    placeholder="Filter by User ID",
                                    multi=False,
                                    className="mb-2"
                                ),
                                width=3
                            ),

                            # Date range filter
                            dbc.Col(
                                dcc.DatePickerRange(
                                    id='anomaly-date-filter',
                                    start_date_placeholder_text="Start Date",
                                    end_date_placeholder_text="End Date",
                                    display_format="YYYY-MM-DD",
                                    className="mb-2"
                                ),
                                width=3
                            ),
                        ])
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Cumulative Contribution of Mismatches by User"),
                    dbc.CardBody([
                        # Description
                        html.Div(
                            "Shows the users contributing the highest mismatch counts, with cumulative percentage to highlight key offenders (Pareto principle).",
                            className="text-muted mb-3"
                        ),

                        # Dropdown for Top N Users
                        html.Div([
                            html.Label("Show Top N Users", className="fw-bold mb-2"),
                            dcc.Dropdown(
                                id='top-n-dropdown',
                                options=[
                                    {'label': 'Top 10', 'value': 10},
                                    {'label': 'Top 20', 'value': 20}
                                ],
                                value=20,  # Default selection
                                clearable=False,
                                style={'width': '150px'}
                            )
                        ], className="mb-3"),

                        # Pareto Chart
                        html.Div([
                            dcc.Graph(id='pareto-chart')
                        ])
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),


        # 6. Detailed Anomaly Table
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Detailed Anomaly Table"),
                    dbc.CardBody([
                        html.Div(
                            "Lists all anomalous transactions with user, amount, VAT, and timestamp. "
                            "Provides drill-down details for auditors and accountants.",
                            className="text-muted mb-2"
                        ),
                        dash_table.DataTable(
                            id='anomaly-table',
                            columns=[
                                {'name': 'Timestamp', 'id': 'timestamp'},
                                {'name': 'Mismatch Type', 'id': 'mismatch_type'},
                                # {'name': 'Transaction ID', 'id': 'transaction_id'},
                                {'name': 'User ID', 'id': 'user_id'},
                                {'name': 'Country', 'id': 'country'},
                                {'name': 'Amount', 'id': 'amount'},
                                {'name': 'VAT', 'id': 'vat'},
                                {'name': 'Old Balance', 'id': 'old_balance'},
                                {'name': 'New Balance', 'id': 'new_balance'},
                                {'name': 'Mismatch Amount', 'id': 'mismatch_amount'},
                            ],
                            page_size=40,
                            filter_action="native",
                            sort_action="native",
                            style_table={
                                'overflowX': 'auto',
                                'width': '100%',
                                'minWidth': '100%'
                            },
                            style_cell={
                                'textAlign': 'center',
                                'padding': '8px',
                                'minWidth': '100px',
                                'width': 'auto',
                                'maxWidth': '200px',
                                'whiteSpace': 'normal'
                            },
                            style_header={
                                'backgroundColor': '#f4f4f4',
                                'fontWeight': 'bold'
                            }
                        )
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ])


    ], fluid=True)
