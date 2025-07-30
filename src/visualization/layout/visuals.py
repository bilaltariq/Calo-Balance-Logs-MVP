from dash import Dash, dcc, html, dash_table, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database

def get_filters(df):
    """
    Fetch distinct filter values from the reconcile table.
    """
    # Distinct countries (mapped from currency)
    countries = sorted(df['country'].dropna().unique().tolist())
    user_ids = sorted(df['user_id'].dropna().unique().tolist())

    # Mismatch types
    mismatch_types = sorted(df['mismatch_type'].dropna().unique().tolist())

    # Date range
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    min_date = df['timestamp'].min()
    max_date = df['timestamp'].max()

    return countries, mismatch_types, user_ids, min_date, max_date

def get_data():
    db = Database()
    db.connect()
    reconcile_df = db.select_table('reconcile_events')
    db.close_connection()
    return reconcile_df

def trends_layout():

    df = get_data()
    countries, mismatch_types, user_ids, min_date, max_date = get_filters(df)

    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H2("Accounting Reconciliation Dashboard", className="text-center my-3"), width=12)
        ]),

        # Global Filters Card
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Global Filters"),
                    dbc.CardBody([
                        html.Div(
                            "Use these filters to refine all visualizations on this dashboard. "
                            "Filters apply to country, mismatch type and date range.",
                            className="text-muted mb-3"
                        ),
                        dbc.Row([
                            # Country filter (single select)
                            dbc.Col(
                                dcc.Dropdown(
                                    id='country-filter',
                                    placeholder="Filter by Country",
                                    multi=False,
                                    options=[{'label': c, 'value': c} for c in countries],
                                    className="mb-2",
                                    value='Bahrain'
                                ),
                                width=3
                            ),
                            # Mismatch Type filter (multi-select)
                            dbc.Col(
                                dcc.Dropdown(
                                    id='mismatch-type-filter',
                                    placeholder="Filter by Mismatch Type",
                                    multi=True,
                                    value=['CALCULATION ISSUE'],
                                    options=[{'label': m, 'value': m} for m in mismatch_types],
                                    className="mb-2"
                                ),
                                width=3
                            ),
                            # # User ID filter
                            # dbc.Col(
                            #     dcc.Dropdown(
                            #         id='global-user-filter',
                            #         placeholder="Filter by User ID",
                            #         multi=True,
                            #         options=[{'label': u, 'value': u} for u in user_ids],
                            #         className="mb-2"
                            #     ),
                            #     width=3
                            # ),
                            # Date filter
                            dbc.Col(
                                dcc.DatePickerRange(
                                    id='date-filter',
                                    start_date_placeholder_text="Start Date",
                                    end_date_placeholder_text="End Date",
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date,
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

        # Running Total
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Running Total: Actual vs Expected"),
                    dbc.CardBody([
                        html.Div(
                            """
                            This chart plots two running totals over time: \n
                            1. Cumulative Actual Balance - the sum of all 'new_balance' values in chronological order. \n
                            2. Cumulative Expected Balance** - the sum of all 'expected_new_balance' values in chronological order. \n

                            By comparing these two cumulative lines, you can spot trends in discrepancies: \n
                            - If the lines stay close together, calculations are consistent. \n
                            - If the gap between them widens, it signals systemic calculation issues \n 
                            """,
                            className="text-muted mb-2"
                        ),
                        dcc.Graph(id='running-total-line')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),
    ], fluid=True)
