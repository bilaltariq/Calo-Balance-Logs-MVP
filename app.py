from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
from src.visualization.layout.components.header import header
from src.visualization.layout.layout_reconciliation import reconciliation_layout
from src.visualization.layout.components.footer import app_footer
from src.visualization import backend

# Initialize Dash with Bootstrap
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Calo Balance Dashboard"

# Layout with Header and Footer
app.layout = html.Div([
    header,

    dcc.Tabs(id='tabs', value='tab-reconciliation', children=[
        dcc.Tab(label='Reconciliation', value='tab-reconciliation'),
        dcc.Tab(label='Trends', value='tab-trends'),
        dcc.Tab(label='Anomalies', value='tab-anomalies'),
    ]),

    html.Div(id='tab-content'),

    app_footer()
])

# Callback to render tab content
@app.callback(
    Output('tab-content', 'children'),
    Input('tabs', 'value')
)
def render_tab_content(tab):
    if tab == 'tab-reconciliation':
        return reconciliation_layout()
    elif tab == 'tab-trends':
        return html.Div("Trends View (to be built)")
    elif tab == 'tab-anomalies':
        return html.Div("Anomalies View (to be built)")


backend.register_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True)
