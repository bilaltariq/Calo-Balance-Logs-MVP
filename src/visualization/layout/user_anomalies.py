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

                            # Country filter (replaces currency)
                            dbc.Col(
                                dcc.Dropdown(
                                    id='anomaly-country-filter',
                                    placeholder="Filter by Country",
                                    multi=False,
                                    className="mb-2"
                                ),
                                width=3
                            ),

                            # Mismatch Type filter (new multi-select)
                            dbc.Col(
                                dcc.Dropdown(
                                    id='anomaly-mismatch-type-filter',
                                    placeholder="Filter by Mismatch Type",
                                    multi=True,
                                    className="mb-2"
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

        # 1. Top Anomalous Users
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Top Anomalous Users"),
                    dbc.CardBody([
                        html.Div(
                            "Ranks users by mismatch count or total mismatch amount. "
                            "Helps prioritize investigation into users causing most anomalies.",
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='anomaly-top-users')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # 2. Mismatch Trend by User
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Mismatch Trend by User"),
                    dbc.CardBody([
                        html.Div(
                            "Shows mismatch activity over time for top anomalous users. "
                            "Helps detect recurring anomalies and seasonal patterns.",
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='anomaly-trend-user')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # 3. Transaction Amount Outliers
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Transaction Amount Outliers"),
                    dbc.CardBody([
                        html.Div(
                            "Highlights users with extreme transaction amounts. "
                            "Outliers may indicate fraud or misconfigured processes.",
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='anomaly-amount-outliers')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # 4. User Balance Drift
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("User Balance Drift"),
                    dbc.CardBody([
                        html.Div(
                            "Shows difference between actual and expected balances over time for selected users. "
                            "Helps detect slow-growing systemic mismatches.",
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='anomaly-balance-drift')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # 5. Heatmap of Anomalies
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Anomaly Heatmap"),
                    dbc.CardBody([
                        html.Div(
                            "Visualizes frequency or severity of anomalies across users and dates. "
                            "Helps identify concentrated periods of errors.",
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='anomaly-heatmap')
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
                                {'name': 'User ID', 'id': 'user_id'},
                                {'name': 'Transaction ID', 'id': 'transaction_id'},
                                {'name': 'Amount', 'id': 'amount'},
                                {'name': 'VAT', 'id': 'vat'},
                                {'name': 'Old Balance', 'id': 'old_balance'},
                                {'name': 'New Balance', 'id': 'new_balance'},
                                {'name': 'Mismatch Amount', 'id': 'mismatch_amount'},
                            ],
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left'},
                            page_size=10
                        )
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ])

    ], fluid=True)
