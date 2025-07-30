import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
import pandas as pd
from dash import Output, Input, State, no_update, callback, Dash, dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go


def prepare_anomaly_data():
    """Prepare anomaly data by calculating mismatch flags and amounts."""
    df = get_data()
    numeric_cols = ['amount', 'vat', 'old_balance', 'new_balance', 'paymentBalance', 'subscriptionBalance']
    df[numeric_cols] = df[numeric_cols].astype(float)
    df['is_mismatch'] = (df['mismatch_type'] != 'NO FOUND ISSUE').astype(int)
    df['mismatch_amount'] = df['new_balance'] - df['expected_new_balance']
    df = df[df['mismatch_amount'].round(0) != 0]
    df = df[df['is_mismatch'] == 1]
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def prepare_data():
    """Prepare main data with mismatch flag and cumulative calculations."""
    df = get_data()
    numeric_cols = ['amount', 'vat', 'old_balance', 'new_balance', 'paymentBalance', 'subscriptionBalance', 'expected_new_balance']
    df[numeric_cols] = df[numeric_cols].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['is_mismatch'] = (df['mismatch_type'] != 'NO FOUND ISSUE').astype(int)
    df = df[df['is_mismatch'] == 1]
    df['mismatch_amount'] = df['new_balance'] - df['expected_new_balance']
    return df


def get_data():
    """Retrieve reconcile_events data from the database."""
    db = Database()
    db.connect()
    reconcile_df = db.select_table('reconcile_events')
    db.close_connection()
    return reconcile_df


def register_callbacks(app):
    """Register Dash callbacks for anomaly analysis and reconciliation."""

    @app.callback(
        Output('country-warning', 'children'),
        Input('filter-country', 'value')
    )
    def show_country_warning(selected_countries):
        if selected_countries and len(selected_countries) > 1:
            return "Warning: Selected countries have different currencies. Aggregation will be inaccurate."
        return ""

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
        State("filter-country", "value"),
        State("filter-mismatch-type", "value"),
        State("filter-overdraft", "value"),
        prevent_initial_call=False
    )
    def apply_filters(n_clicks, selected_users, start_date, end_date, selected_country, selected_mismatch_types, is_over_draft):
        reconcile_df = get_data()
        reconcile_df['date'] = pd.to_datetime(reconcile_df['timestamp'])
        reconcile_df['timestamp'] = pd.to_datetime(reconcile_df['timestamp']).dt.date

        if selected_users:
            reconcile_df = reconcile_df[reconcile_df['user_id'].isin(selected_users)]
        if selected_country:
            reconcile_df = reconcile_df[reconcile_df['country'].isin(selected_country)]
        if selected_mismatch_types:
            reconcile_df = reconcile_df[reconcile_df['mismatch_type'].isin(selected_mismatch_types)]
        if is_over_draft:
            reconcile_df = reconcile_df[reconcile_df['is_overdraft'].isin(is_over_draft)]

        start = pd.to_datetime(start_date).date() if start_date else None
        end = pd.to_datetime(end_date).date() if end_date else None
        if start and end:
            reconcile_df = reconcile_df[
                (reconcile_df['date'].dt.date >= start) & (reconcile_df['date'].dt.date <= end)
            ]

        numeric_cols = ['new_balance', 'old_balance', 'amount', 'vat', 'paymentBalance', 'subscriptionBalance']
        reconcile_df[numeric_cols] = reconcile_df[numeric_cols].astype(float)

        count_users = (
            reconcile_df[
                (reconcile_df['mismatch_type'] != 'NO FOUND ISSUE') &
                (reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat']))
            ]['user_id']
            .nunique()
        )

        total_mismatch = (
            (reconcile_df['new_balance'] - (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat']))[
                (reconcile_df['mismatch_type'] != 'NO FOUND ISSUE') &
                (reconcile_df['new_balance'] != (reconcile_df['old_balance'] + reconcile_df['amount'] - reconcile_df['vat']))
            ]
            .sum()
        )

        formatted_total_mismatch = f"{total_mismatch:,.0f}"
        last_sync = reconcile_df[reconcile_df['mismatch_type'] != 'NO FOUND ISSUE']['date'].max()

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

    @callback(
        [
            Output('running-total-line', 'figure'),
            Output('country-filter', 'options'),
            Output('mismatch-type-filter', 'options'),
        ],
        [
            Input('country-filter', 'value'),
            Input('mismatch-type-filter', 'value'),
            Input('date-filter', 'start_date'),
            Input('date-filter', 'end_date')
        ]
    )
    def update_charts(country_filter, mismatch_filter, start_date, end_date):
        df = prepare_data()
        df = df.rename(columns={'transaction_id': 'id'})
        df = df[df['mismatch_type'] != 'NO FOUND ISSUE']

        df_users = df.copy()

        if country_filter:
            df = df[df['country'] == country_filter]
        if mismatch_filter:
            df = df[df['mismatch_type'].isin(mismatch_filter)]
        if start_date and end_date:
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

        country_options = [{'label': c, 'value': c} for c in df_users['country'].dropna().unique()]
        mismatch_options = [{'label': m, 'value': m} for m in df_users['mismatch_type'].dropna().unique()]

        df_sorted = df.sort_values('timestamp')
        df_sorted['cumulative_actual'] = df_sorted['new_balance'].cumsum()
        df_sorted['cumulative_expected'] = df_sorted['expected_new_balance'].cumsum()

        fig_running = go.Figure()
        fig_running.add_trace(go.Scatter(x=df_sorted['timestamp'], y=df_sorted['cumulative_actual'], mode='lines', name='Actual'))
        fig_running.add_trace(go.Scatter(x=df_sorted['timestamp'], y=df_sorted['cumulative_expected'], mode='lines', name='Expected'))
        fig_running.update_layout(title="Running Total: Actual vs Expected")

        return fig_running, country_options, mismatch_options

    @callback(
        [
            Output('anomaly-table', 'data'),
            Output('anomaly-country-filter', 'options'),
            Output('anomaly-user-filter', 'options'),
            Output('anomaly-mismatch-type-filter', 'options'),
            Output('anomaly-date-filter', 'start_date'),
            Output('anomaly-date-filter', 'end_date'),
            Output('pareto-chart', 'figure')
        ],
        [
            Input('anomaly-country-filter', 'value'),
            Input('anomaly-user-filter', 'value'),
            Input('anomaly-mismatch-type-filter', 'value'),
            Input('anomaly-date-filter', 'start_date'),
            Input('anomaly-date-filter', 'end_date'),
            Input('top-n-dropdown', 'value')   
        ]
    )
    def update_anomaly_charts(country_filter, user_filter, mismatch_filter, start_date, end_date, top_n):
        df_full = prepare_anomaly_data()
        df_full['short_id'] = df_full['user_id'].str[:6] + "…"

        country_options = [{'label': c, 'value': c} for c in df_full['country'].dropna().unique()]
        user_options = [{'label': u, 'value': u} for u in df_full['user_id'].dropna().unique()]
        mismatch_options = [{'label': m, 'value': m} for m in df_full['mismatch_type'].dropna().unique()]

        start_date_default = df_full['timestamp'].min()
        end_date_default = df_full['timestamp'].max()

        df_filtered = df_full.copy()
        if country_filter:
            df_filtered = df_filtered[df_filtered['country'] == country_filter]
        if mismatch_filter:
            df_filtered = df_filtered[df_filtered['mismatch_type'].isin(mismatch_filter)]
        if user_filter:
            df_filtered = df_filtered[df_filtered['user_id'] == user_filter]
        if start_date and end_date:
            df_filtered = df_filtered[
                (df_filtered['timestamp'] >= start_date) & 
                (df_filtered['timestamp'] <= end_date)
            ]

        anomalies = df_filtered

        pareto_data = anomalies.groupby('short_id').size().reset_index(name='count')
        pareto_data = pareto_data.head(top_n)
        pareto_data = pareto_data.sort_values('count', ascending=False).reset_index(drop=True)
        pareto_data['cum_percent'] = pareto_data['count'].cumsum() / pareto_data['count'].sum() * 100

        fig = go.Figure()
        fig.add_trace(go.Bar(x=pareto_data['short_id'], y=pareto_data['count'], name='Mismatch Count', marker_color='steelblue', yaxis='y1'))
        fig.add_trace(go.Scatter(x=pareto_data['short_id'], y=pareto_data['cum_percent'], name='Cumulative %', mode='lines+markers', marker_color='darkorange', yaxis='y2'))
        fig.add_hline(y=80, line_dash="dash", annotation_text="80% Threshold", annotation_position="top right", yref='y2')

        fig.update_layout(
            title="Pareto Analysis of User-Level Mismatches",
            xaxis=dict(title="User ID"),
            yaxis=dict(title="Mismatch Count", side="left"),
            yaxis2=dict(title="Cumulative %", overlaying='y', side="right", range=[0, 100]),
            bargap=0.3,
            template="plotly_white"
        )

        table_data = anomalies.to_dict('records')

        return table_data, country_options, user_options, mismatch_options, start_date_default, end_date_default, fig
