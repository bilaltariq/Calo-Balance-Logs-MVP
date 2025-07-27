from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
import pandas as pd
from dash import Output, Input

def get_summary_results(reconcile_df):
    count_users = (
        reconcile_df[
            reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat'])
        ]['user_id']
        .nunique()
    )

    # Calculate total mismatch amount
    total_mismatch = (
        (reconcile_df['new_balance'] - (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat']))
        [reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat'])]
        .sum()
    )

    last_sync = reconcile_df['timestamp'].max()

    return count_users, total_mismatch, last_sync
    

def get_reconcile_filters(df):
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

def reconciliation_layout():
    reconcile_df = get_data()

    currencies, user_ids, min_date, max_date = get_reconcile_filters(reconcile_df)

    return dbc.Container([

        # Title
        html.H2("Reconciliation Dashboard", className="text-center my-4 fw-bold"),

        # Filters Card
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Filters", className="mb-0 fw-bold")),
                    dbc.CardBody([

                        # User ID Dropdown (populated from DB)
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

                        #  Date Range (min/max auto-filled)
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


                        # Currency Filter
                        dbc.Row([
                            dbc.Col([
                                html.Label("Currency", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-currency',
                                    options=[{'label': c, 'value': c} for c in currencies],
                                    multi=True,
                                    placeholder="Select Currency"
                                )
                            ], width=12)
                        ]),
                        # Apply Filters Button
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("Apply Filters", id="btn-apply-filters", color="primary", className="mt-3 w-100")
                            ], width=12)
                        ])

                    ])
                ], className="shadow-sm"), width=6
            )
        ], justify="center", className="mb-5"),

# Summary & Table Card
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Reconciliation Results", className="mb-0 fw-bold")),
                    dbc.CardBody([

                        # Summary Stats Row
                        dbc.Row([
                            dbc.Col(html.Div([
                                html.H6("Total Users with Discrepancies", className="fw-bold"),
                                html.P(id="summary-total-users", children="0", className="fs-5"),
                            ]), md=4, className="text-center"),

                            dbc.Col(html.Div([
                                html.H6("Total Amount Mismatch", className="fw-bold"),
                                html.P(id="summary-total-mismatch", children="0 BHD", className="fs-5")
                            ]), md=4, className="text-center"),

                            dbc.Col(html.Div([
                                html.H6("Last Sync Date", className="fw-bold"),
                                html.P(id="summary-last-sync", children="--", className="fs-5")
                            ]), md=4, className="text-center"),
                        ], className="mb-4"),

                        # Export Button
                        html.Div([
                            dbc.Button("Export Transactions", id="btn-export", color="secondary", className="mb-3 float-end"),
                            dcc.Download(id="download-transactions")  # For download callback
                        ]),

                        # Data Table (show all reconcile columns)
                        dash_table.DataTable(
                            id='reconciliation-table',
                            columns=[
                                {'name': 'ID', 'id': 'transaction_id'},
                                {'name': 'User ID', 'id': 'user_id'},
                                {'name': 'Currency', 'id': 'currency'},
                                {'name': 'Amount', 'id': 'amount'},
                                {'name': 'VAT', 'id': 'vat'},
                                {'name': 'Old Balance', 'id': 'old_balance'},
                                {'name': 'New Balance', 'id': 'new_balance'},
                                {'name': 'Payment Balance', 'id': 'payment_balance'},
                                {'name': 'Subscription Balance', 'id': 'subscription_balance'},
                                {'name': 'Event Type', 'id': 'event_type'},
                                {'name': 'Timestamp', 'id': 'timestamp'}
                            ],
                            page_size=20,
                            data=reconcile_df.to_dict('records'),
                            style_table={'overflowX': 'auto' },
                            style_cell={'textAlign': 'center', 'padding': '8px'},
                            style_header={'backgroundColor': '#f4f4f4', 'fontWeight': 'bold'}
                        )
                    ])
                ], className="shadow-sm"), width=10
            )
        ], justify="center")

    ], fluid=True)


from dash import callback
@callback(
    [
        Output("summary-total-users", "children"),
        Output("summary-total-mismatch", "children"),
        Output("summary-last-sync", "children"),
    ],
    Input("btn-apply-filters", "n_clicks"),
    prevent_initial_call=False
)
def update_summary(n_clicks):
    reconcile_df = get_data()
    count_users, total_mismatch, last_sync = get_summary_results(reconcile_df)

    return str(count_users), str(total_mismatch), last_sync