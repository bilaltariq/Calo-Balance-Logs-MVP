import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.storage.db_manager import Database
import pandas as pd
from dash import Output, Input, State, no_update
from dash import callback
from dash import Dash, dcc, html, dash_table, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

def prepare_anomaly_data():
    """Fetch reconcile data and calculate anomaly flags and mismatch amounts."""
    df = get_data()

    # Convert numeric columns to float
    numeric_cols = ['amount', 'vat', 'old_balance', 'new_balance', 'payment_balance', 'subscription_balance']
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Create mismatch flag
    df['is_anomaly'] = df['new_balance'] != (df['old_balance'] + df['amount'] - df['vat'])

    # Calculate mismatch amount
    df['mismatch_amount'] = df['new_balance'] - (df['old_balance'] + df['amount'] - df['vat'])

    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    return df


def prepare_data():
    df = get_data()

    # Ensure numeric columns are floats
    numeric_cols = ['amount', 'vat', 'old_balance', 'new_balance', 'payment_balance', 'subscription_balance']
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Calculate mismatch
    df['expected_new_balance'] = df['old_balance'] + df['amount'] - df['vat']
    df['is_mismatch'] = df['new_balance'] != df['expected_new_balance']
    df['mismatch_amount'] = df['new_balance'] - df['expected_new_balance']

    return df


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
        reconcile_df['timestamp'] = pd.to_datetime(reconcile_df['timestamp']).dt.date

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

        # Ensure numeric columns are float
        numeric_cols = ['new_balance', 'old_balance', 'amount', 'vat']
        reconcile_df[numeric_cols] = reconcile_df[numeric_cols].astype(float)

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


    # ---- Callbacks ----
    @callback(
        [
            Output('mismatch-trend', 'figure'),
            Output('transaction-volume', 'figure'),
            Output('mismatch-distribution', 'figure'),
            Output('source-type-pie', 'figure'),
            Output('balance-change', 'figure'),
            Output('currency-wise-bar', 'figure'),
            Output('running-total-line', 'figure'),
            Output('global-user-filter', 'options'),
            Output('currency-filter', 'options'),
        ],
        [
            Input('global-user-filter', 'value'),
            Input('currency-filter', 'value'),
            Input('date-filter', 'start_date'),
            Input('date-filter', 'end_date')
        ]
    )
    def update_charts(user_filter, currency_filter, start_date, end_date):
        df = prepare_data()
        df = df.rename(columns={'transaction_id':'id'})
        
        df_users = df.copy()
        
        # Apply filters
        if currency_filter:
            df = df[df['currency'] == currency_filter]
        if start_date and end_date:
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
        if user_filter:
            df = df[df['user_id'].isin(user_filter)]


        # Dropdown options
        currency_options = [{'label': c, 'value': c} for c in df['currency'].dropna().unique()]
        user_options = [{'label': u, 'value': u} for u in df_users['user_id'].dropna().unique()]


        # ---- 1. Mismatch Trend Over Time ----
        trend = df.groupby(df['timestamp'].dt.date)['is_mismatch'].sum().reset_index()
        fig_trend = px.line(trend, x='timestamp', y='is_mismatch', title="Mismatch Trend Over Time")

        # ---- 2. Daily Transaction Volume vs Mismatches ----
        volume = df.groupby(df['timestamp'].dt.date).agg(
            total_transactions=('id', 'count'),
            mismatches=('is_mismatch', 'sum')
        ).reset_index()
        fig_volume = go.Figure()
        fig_volume.add_trace(go.Bar(x=volume['timestamp'], y=volume['total_transactions'], name='Total Transactions'))
        fig_volume.add_trace(go.Bar(x=volume['timestamp'], y=volume['mismatches'], name='Mismatches'))
        fig_volume.update_layout(barmode='group', title="Transactions vs Mismatches")

        # ---- 3. Mismatch Amount Distribution ----
        fig_distribution = px.histogram(df[df['is_mismatch']], x='mismatch_amount', nbins=20,
                                        title="Mismatch Amount Distribution")

        # ---- 4. Source Type Contribution ----
        source_counts = df[df['is_mismatch']].groupby('source_type')['id'].count().reset_index()
        fig_pie = px.pie(source_counts, values='id', names='source_type', title="Mismatch by Source Type")

        # ---- 5. Balance Change per User ----
        balance_df = df[df['is_mismatch']].groupby('user_id').agg(
            old_balance=('old_balance', 'sum'),
            new_balance=('new_balance', 'sum')
        ).reset_index()
        fig_balance = go.Figure()
        fig_balance.add_trace(go.Bar(x=balance_df['user_id'], y=balance_df['old_balance'], name='Old Balance'))
        fig_balance.add_trace(go.Bar(x=balance_df['user_id'], y=balance_df['new_balance'], name='New Balance'))
        fig_balance.update_layout(barmode='group', title="Balance Change per User")

        # ---- 6. Currency-wise Mismatches ----
        currency_df = df[df['is_mismatch']].groupby('currency')['id'].count().reset_index()
        fig_currency = px.bar(currency_df, x='currency', y='id', title="Mismatches per Currency")

        # ---- 7. Running Total vs Expected ----
        df_sorted = df.sort_values('timestamp')
        df_sorted['cumulative_actual'] = df_sorted['new_balance'].cumsum()
        df_sorted['cumulative_expected'] = df_sorted['expected_new_balance'].cumsum()
        fig_running = go.Figure()
        fig_running.add_trace(go.Scatter(x=df_sorted['timestamp'], y=df_sorted['cumulative_actual'],
                                        mode='lines', name='Actual'))
        fig_running.add_trace(go.Scatter(x=df_sorted['timestamp'], y=df_sorted['cumulative_expected'],
                                        mode='lines', name='Expected'))
        fig_running.update_layout(title="Running Total: Actual vs Expected")

        return (fig_trend, fig_volume, fig_distribution, fig_pie,
                fig_balance, fig_currency, fig_running, user_options, currency_options)


    @callback(
        [
            # Graph outputs
            Output('anomaly-top-users', 'figure'),
            Output('anomaly-trend-user', 'figure'),
            Output('anomaly-amount-outliers', 'figure'),
            Output('anomaly-balance-drift', 'figure'),
            Output('anomaly-heatmap', 'figure'),
            Output('anomaly-table', 'data'),

            # Filter options
            Output('anomaly-currency-filter', 'options'),
            Output('anomaly-user-filter', 'options'),

            # Set default date range
            Output('anomaly-date-filter', 'start_date'),
            Output('anomaly-date-filter', 'end_date'),
        ],
        [
            Input('anomaly-currency-filter', 'value'),
            Input('anomaly-user-filter', 'value'),
            Input('anomaly-date-filter', 'start_date'),
            Input('anomaly-date-filter', 'end_date')
        ]
    )
    def update_anomaly_charts(currency_filter, user_filter, start_date, end_date):
        df = prepare_anomaly_data()

        # Apply filters
        if currency_filter:
            df = df[df['currency'] == currency_filter]
        if user_filter:
            df = df[df['user_id'] == user_filter]
        if start_date and end_date:
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

        # Filter only anomalies
        anomalies = df[df['is_anomaly']]

        # Dropdown options
        currency_options = [{'label': c, 'value': c} for c in df['currency'].dropna().unique()]
        user_options = [{'label': u, 'value': u} for u in df['user_id'].dropna().unique()]

        # Default date range
        start_date_default = df['timestamp'].min()
        end_date_default = df['timestamp'].max()

        # -------- 1. Top Anomalous Users --------
        top_users = anomalies.groupby('user_id')['mismatch_amount'].sum().nlargest(10).reset_index()
        fig_top_users = px.bar(
            top_users,
            x='mismatch_amount',
            y='user_id',
            orientation='h',
            title='Top Users by Mismatch Amount',
            labels={'mismatch_amount': 'Total Mismatch', 'user_id': 'User'}
        )

        # -------- 2. Mismatch Trend by User --------
        trend = anomalies.groupby([pd.Grouper(key='timestamp', freq='D'), 'user_id'])['mismatch_amount'].sum().reset_index()
        fig_trend_user = px.line(
            trend,
            x='timestamp',
            y='mismatch_amount',
            color='user_id',
            title='Mismatch Trend by User'
        )

        # -------- 3. Transaction Amount Outliers (Box Plot) --------
        fig_outliers = px.box(
            anomalies,
            x='user_id',
            y='amount',
            title='Transaction Amount Outliers by User'
        )

        # -------- 4. User Balance Drift --------
        drift = anomalies.groupby('timestamp')['mismatch_amount'].sum().cumsum().reset_index()
        fig_drift = px.line(
            drift,
            x='timestamp',
            y='mismatch_amount',
            title='Cumulative Balance Drift'
        )

        # -------- 5. Heatmap of Anomalies --------
        heatmap_data = anomalies.groupby(['user_id', pd.Grouper(key='timestamp', freq='D')])['mismatch_amount'].sum().reset_index()
        heatmap_pivot = heatmap_data.pivot(index='user_id', columns='timestamp', values='mismatch_amount').fillna(0)
        fig_heatmap = px.imshow(
            heatmap_pivot.values,
            labels=dict(x="Date", y="User ID", color="Mismatch"),
            x=heatmap_pivot.columns.strftime('%Y-%m-%d'),
            y=heatmap_pivot.index,
            title="Anomaly Heatmap"
        )

        # -------- 6. Detailed Anomaly Table --------
        table_data = anomalies.to_dict('records')

        return (
            fig_top_users,
            fig_trend_user,
            fig_outliers,
            fig_drift,
            fig_heatmap,
            table_data,
            currency_options,
            user_options,
            start_date_default,
            end_date_default
        )
