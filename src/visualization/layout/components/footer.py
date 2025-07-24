from dash import html
import dash_bootstrap_components as dbc

def app_footer():
    """
    Returns the footer component for the dashboard.
    """
    return html.Footer(
        dbc.Container([
            dbc.Row([
                # Left column - About or tagline
                dbc.Col([
                    html.P("Calo Dashboard", className="fw-bold mb-1"),
                    html.P("Streamline Calo Apps for users.", className="text-muted small")
                ], md=6, sm=12),

                dbc.Col([
                    html.Ul([
                        #html.Li(html.A("Disclaimer", href="/disclaimer-terms-and-conditions", className="text-muted", style={"textDecoration": "none"})),
                        #html.Li(html.A("Contact", href="/contact", className="text-muted", style={"textDecoration": "none"})),
                    ], className="list-unstyled mb-0")
                ], md=6, sm=12, className="text-md-end mt-3 mt-md-0")
            ], className="py-3"),

            html.Hr(),

            dbc.Row([
                dbc.Col(
                    html.P("© 2025 Calo Dashboard", className="text-muted small mb-0 text-center"),
                    width=12
                )
            ])
        ]),
        style={
            "backgroundColor": "#F1F3F5",
            "borderTop": "1px solid #DDD",
            "marginTop": "40px",
            "paddingTop": "20px",
            "paddingBottom": "10px"
        }
    )
