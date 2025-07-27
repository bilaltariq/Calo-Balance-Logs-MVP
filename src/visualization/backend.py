import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
import pandas as pd
from dash import Output, Input, State, no_update
from dash import callback

def get_data():
    db = Database()
    db.connect()
    reconcile_df = db.select_table('reconcile_events')
    db.close_connection()
    return reconcile_df

from dash import Output, Input, State, callback
import pandas as pd

def register_callbacks(app):

    @app.callback(
        [
            Output("summary-total-users", "children"),
            Output("summary-total-mismatch", "children"),
            Output("summary-last-sync", "children"),
            Output("reconciliation-table", "data"),
            Output("store-filtered-data", "data")
        ],
        Input("btn-apply-filters", "n_clicks"),
        State("filter-user-id", "value"),
        State("filter-date-range", "start_date"),
        State("filter-date-range", "end_date"),
        State("filter-currency", "value"),
        prevent_initial_call=False
    )
    def apply_filters(n_clicks, selected_users, start_date, end_date, selected_currencies):

        reconcile_df = get_data()

        # Ensure datetime
        reconcile_df['date'] = pd.to_datetime(reconcile_df['timestamp'])
        reconcile_df['timestamp'] = pd.to_datetime(reconcile_df['timestamp'])

        # Apply user filter
        if selected_users:
            reconcile_df = reconcile_df[reconcile_df['user_id'].isin(selected_users)]

        # Apply currency filter
        if selected_currencies:
            reconcile_df = reconcile_df[reconcile_df['currency'].isin(selected_currencies)]

        # Apply date filter
        start = pd.to_datetime(start_date).date() if start_date else None
        end = pd.to_datetime(end_date).date() if end_date else None
        if start and end:
            reconcile_df = reconcile_df[
                (reconcile_df['date'].dt.date >= start) & (reconcile_df['date'].dt.date <= end)
            ]

        # Calculate mismatch
        count_users = (
            reconcile_df[
                reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat'])
            ]['user_id']
            .nunique()
        )

        total_mismatch = (
            (reconcile_df['new_balance'] - (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat']))
            [reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat'])]
            .sum()
        )
        formatted_total_mismatch = f"{total_mismatch:,.0f}"

        last_sync = reconcile_df['date'].max()

        return str(count_users), str(formatted_total_mismatch), str(last_sync), reconcile_df.to_dict('records'), reconcile_df.to_dict('records')


    @callback(
        Output("download-transactions", "data"),
        Input("btn-export", "n_clicks"),
        State("store-filtered-data", "data"),
        prevent_initial_call=True
    )
    def export_transactions(n_clicks, filtered_data):
        if not filtered_data:
            return no_update
        csv_string = pd.DataFrame(filtered_data).to_csv(index=False)
        return dict(content=csv_string, filename="transactions.csv")
