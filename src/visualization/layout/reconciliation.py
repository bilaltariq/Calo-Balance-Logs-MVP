from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc

def reconciliation_layout():
    """
    Layout for Reconciliation tab using Bootstrap cards.
    Includes export button for transactional data.
    """
    return dbc.Container([

        # Title
        html.H2("Reconciliation Dashboard", className="text-center my-4 fw-bold"),

        # Filters Card
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Filters", className="mb-0 fw-bold")),
                    dbc.CardBody([
                        # User ID Filter
                        dbc.Row([
                            dbc.Col([
                                html.Label("User ID", className="fw-bold"),
                                dcc.Input(id='filter-user-id', type='text', placeholder='Enter User ID',
                                          className="form-control")
                            ], width=12, className="mb-3")
                        ]),

                        # Date Range Filter
                        dbc.Row([
                            dbc.Col([
                                html.Label("Date Range", className="fw-bold"),
                                dcc.DatePickerRange(
                                    id='filter-date-range',
                                    start_date_placeholder_text="Start Date",
                                    end_date_placeholder_text="End Date"
                                )
                            ], width=12, className="mb-3")
                        ]),

                        # Currency Filter
                        dbc.Row([
                            dbc.Col([
                                html.Label("Currency", className="fw-bold"),
                                dcc.Dropdown(
                                    id='filter-currency',
                                    options=[
                                        {'label': 'BHD', 'value': 'BHD'},
                                        {'label': 'SAR', 'value': 'SAR'}
                                    ],
                                    multi=True,
                                    placeholder="Select Currency"
                                )
                            ], width=12)
                        ]),
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
                                html.P(id="summary-total-users", children="0", className="fs-5")
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
                            dbc.Button("Export Transactions", id="btn-export", color="primary", className="mb-3 float-end"),
                            dcc.Download(id="download-transactions")  # For download callback
                        ]),

                        # Data Table
                        dash_table.DataTable(
                            id='reconciliation-table',
                            columns=[
                                {'name': 'User ID', 'id': 'userId'},
                                {'name': 'Transaction ID', 'id': 'transactionId'},
                                {'name': 'Subscription Balance', 'id': 'subscriptionBalance'},
                                {'name': 'Payment Balance', 'id': 'paymentBalance'},
                                {'name': 'Currency', 'id': 'currency'},
                                {'name': 'Timestamp', 'id': 'timestamp'},
                            ],
                            page_size=10,
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left', 'padding': '8px'},
                            style_header={'backgroundColor': '#f4f4f4', 'fontWeight': 'bold'}
                        )
                    ])
                ], className="shadow-sm"), width=10
            )
        ], justify="center")

    ], fluid=True)
