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
    # Distinct currencies
    currencies = sorted(df['currency'].dropna().unique().tolist())

    user_ids = sorted(df['user_id'].dropna().unique().tolist())

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    min_date = df['timestamp'].min()
    max_date = df['timestamp'].max()

    return currencies, user_ids, min_date, max_date

def get_data():
    db = Database()
    db.connect()
    reconcile_df = db.select_table('reconcile_events')
    db.close_connection()
    return reconcile_df

def trends_layout():

    df = get_data()
    currencies, user_ids, min_date, max_date = get_filters(df)

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
                            "Filters apply to currency, specific users, and date range.",
                            className="text-muted mb-3"
                        ),
                        dbc.Row([
                            dbc.Col(
                                dcc.Dropdown(
                                    id='currency-filter',
                                    placeholder="Filter by Currency",
                                    multi=False,
                                    options=[{'label': u, 'value': u} for u in currencies],
                                    className="mb-2"
                                ),
                                width=3
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id='global-user-filter',
                                    placeholder="Filter by User ID",
                                    multi=True,
                                    options=[{'label': u, 'value': u} for u in user_ids],
                                    className="mb-2"
                                ),
                                width=3
                            ),
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
                                width=6
                            ),
                        ])
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Mismatch Trend
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Mismatch Trend Over Time"),
                    dbc.CardBody([
                        html.Div("This chart shows daily mismatches in balances. It helps identify spikes on certain dates, indicating possible issues in reconciliation or system updates.", className="text-muted mb-2"),
                        dcc.Graph(id='mismatch-trend')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Transaction Volume
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Transaction Volume vs Mismatches"),
                    dbc.CardBody([
                        html.Div("Compares total transactions against mismatched ones to help measure error rates and volume trends across days.", className="text-muted mb-2"),
                        dcc.Graph(id='transaction-volume')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Mismatch Distribution
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Mismatch Amount Distribution"),
                    dbc.CardBody([
                        html.Div("Displays how mismatch amounts are spread out. Helps spot whether errors are minor rounding differences or large anomalies.", className="text-muted mb-2"),
                        dcc.Graph(id='mismatch-distribution')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Source Type Contribution
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Mismatch by Source Type"),
                    dbc.CardBody([
                        html.Div("Shows which transaction sources (e.g., manual deduction, payment) are causing the most mismatches, helping prioritize investigation.", className="text-muted mb-2"),
                        dcc.Graph(id='source-type-pie')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Balance Change per User
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Balance Change per User"),
                    dbc.CardBody([
                        html.Div(
                            "Compares old vs new balances per user to detect unusual spikes or drops in user accounts that may indicate reconciliation issues.",
                            className="text-muted mb-2"
                        ),
                        # User ID dropdown filter
                        dcc.Dropdown(
                            id='user-filter',
                            placeholder="Filter by User ID",
                            multi=True,
                            className="mb-3"
                        ),
                        dcc.Graph(id='balance-change')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),

        # Currency-wise Mismatches
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Mismatches per Currency"),
                    dbc.CardBody([
                        html.Div("Shows mismatches grouped by currency, useful if multiple currencies are handled in the accounting system.", className="text-muted mb-2"),
                        dcc.Graph(id='currency-wise-bar')
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
                        html.Div("Visualizes the cumulative balances (actual vs expected) over time. Any growing gap indicates systemic calculation errors.", className="text-muted mb-2"),
                        dcc.Graph(id='running-total-line')
                    ])
                ], className="mb-4 shadow-sm rounded-3"),
                width=12
            )
        ]),
    ], fluid=True)
