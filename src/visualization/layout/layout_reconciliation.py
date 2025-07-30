from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
import pandas as pd
from dash import Output, Input

def get_reconcile_filters(df):
    """
    Fetch distinct filter values from the reconcile table.
    """
    # Distinct currencies
    countries = sorted(df['country'].dropna().unique().tolist())
    mismatch_type = sorted(df['mismatch_type'].dropna().unique().tolist())

    user_ids = sorted(df['user_id'].dropna().unique().tolist())

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    min_date = df['timestamp'].min()
    max_date = df['timestamp'].max()

    return countries, user_ids, min_date, max_date, mismatch_type

def get_data():
    db = Database()
    db.connect()
    reconcile_df = db.select_table('reconcile_events')
    db.close_connection()
    return reconcile_df

def reconciliation_layout():
    reconcile_df = get_data()

    countries, user_ids, min_date, max_date, mismatch_type = get_reconcile_filters(reconcile_df)

    return dbc.Container([

        # Title
        html.H2("Reconciliation Dashboard", className="text-center my-4 fw-bold"),

        # Filters Card (full width with light blue background)
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(
                        html.H5("Filters", className="mb-0 fw-bold text-dark"),
                        style={"backgroundColor": "#f8f9fa"}
                    ),
                    dbc.CardBody([

                        # User ID Dropdown
                        dbc.Row([
                            dbc.Col([
                                html.Label("User ID", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-user-id',
                                    options=[{'label': u, 'value': u} for u in user_ids],
                                    placeholder='Select User ID',
                                    multi=True
                                )
                            ], width=12, className="mb-3")
                        ]),

                        # Date Range Picker
                        dbc.Row([
                            dbc.Col([
                                html.Label("Date Range", className="fw-bold"),
                                dcc.DatePickerRange(
                                    id='filter-date-range',
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date
                                )
                            ], width=12, className="mb-3")
                        ]),

                        dbc.Row([
                            dbc.Col([
                                html.Label("Country", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-country',
                                    options=[{'label': c, 'value': c} for c in countries],
                                    placeholder="Select Country",
                                    multi=True,  # allow multiple selections
                                    value=['Bahrain']  # default selection
                                ),
                                html.Div(
                                    id='country-warning',
                                    className="text-warning mt-1 fw-bold"
                                )
                            ], width=12, className="mb-3")
                        ]),

                        dbc.Row([
                            dbc.Col([
                                html.Label("Mismatch Type", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-mismatch-type',
                                    options=[{'label': m, 'value': m} for m in mismatch_type],
                                    placeholder="Select Mismatch Type",
                                    value=["CALCULATION ISSUE"],
                                    multi=True
                                ),
                                html.Div(
                                    [
                                        html.P("• CALCULATION ISSUE: New balance doesn’t match expected balance after applying transaction and VAT.", className="text-muted mb-1"),
                                        html.P("• BALANCE SYNC ISSUE: Payment and subscription balances are not aligned.", className="text-muted mb-1"),
                                        html.P("• CALCULATION + BALANCE SYNC ISSUE: Both calculation mismatch and balance sync mismatch are present.", className="text-muted mb-1"),
                                    ],
                                    className="mt-2"
                                )
                            ], width=12, className="mb-3")
                        ]),

                        dbc.Row([
                            dbc.Col([
                                html.Label("Overdraft", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-overdraft',
                                    options=[
                                        {'label': 'Overdraft', 'value': 1},
                                        {'label': 'No Overdraft', 'value': 0}
                                    ],
                                    placeholder="Select Overdraft Status",
                                    multi=True
                                ),
                                html.Div(
                                    [
                                        html.P("OverDraft Values: New Balance < 0", className="text-muted mb-1")
                                    ],
                                    className="mt-2"
                                )

                            ], width=12, className="mb-3")
                        ]),

                        # Buttons Row
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("Apply Filters", id="btn-apply-filters", color="success", className="mt-3 w-100")
                            ], width=6, className="mb-2"),

                            dbc.Col([
                                dbc.Button("Export Transactions", id="btn-export", color="info", className="mt-3 w-100"),
                                dcc.Store(id="store-filtered-data"),
                                dcc.Download(id="download-transactions")
                            ], width=6, className="mb-2"),
                        ]),
                    ])
                ], className="shadow-sm mb-5",
                style={"border": "1px solid #ccc", "borderRadius": "8px"}),
                width=9  # 75% width
            )
        ], justify="center"),

        # Results Card (full width with light gray header)
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Reconciliation Results", className="mb-0 fw-bold text-dark"),
                                   style={"backgroundColor": "#f8f9fa"}),
                    dbc.CardBody([

                        # Summary Stats Row with subtle background
                        dbc.Row([
                            dbc.Col(html.Div([
                                html.H6("Total Users with Discrepancies", className="fw-bold"),
                                html.P(id="summary-total-users", children="0", className="fs-5 text-primary"),
                            ], className="p-2 bg-light rounded"), md=4, className="text-center"),

                            dbc.Col(html.Div([
                                html.H6("Total Amount Mismatch", className="fw-bold"),
                                html.P(id="summary-total-mismatch", children="0 BHD", className="fs-5 text-danger"),
                            ], className="p-2 bg-light rounded"), md=4, className="text-center"),

                            dbc.Col(html.Div([
                                html.H6("Last Sync Date", className="fw-bold"),
                                html.P(id="summary-last-sync", children="--", className="fs-5 text-secondary"),
                            ], className="p-2 bg-light rounded"), md=4, className="text-center"),
                        ], className="mb-4"),

                        # Data Table
                        dash_table.DataTable(
                            id='reconciliation-table',
                            columns=
                            [
                                {'name': 'Timestamp', 'id': 'timestamp'},
                                {'name': 'Mismatch Type', 'id': 'mismatch_type'},
                                {'name': 'Transaction ID', 'id': 'transaction_id'},
                                {'name': 'Transaction Type', 'id': 'type'},
                                {'name': 'User ID', 'id': 'user_id'},
                                {'name': 'Country', 'id': 'country'},
                                {'name': 'Old Balance', 'id': 'old_balance'},
                                {'name': 'Amount', 'id': 'amount'},
                                {'name': 'VAT', 'id': 'vat'},
                                {'name': 'New Balance', 'id': 'new_balance'},
                                # {'name': 'Expected New Balance', 'id': 'expected_new_balance'},
                                {'name': 'Payment Balance', 'id': 'paymentBalance'}, 
                                {'name': 'Subscription Balance', 'id': 'subscriptionBalance'}
                                # {'name': 'Source Type', 'id': 'source_type'},
                                # {'name': 'Event Type', 'id': 'event_type'}
                            ],
                            page_size=40,
                            data=reconcile_df.to_dict('records'),
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
                ], className="shadow-sm", style={"border": "1px solid #ccc", "borderRadius": "8px"}), width=12
            )
        ], justify="center")

    ], fluid=True)
