from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

# Navbar Component
header = dbc.Navbar(
    dbc.Container([
        # Brand Name
        dbc.Row([
            dbc.Col(
                dcc.Link(
                    dbc.NavbarBrand("Calo Balance Dashboard", className="ms-2 fs-4 fw-bold text-dark"),
                    href="/", refresh=True
                ),
                width="auto"
            ),
        ], align="center", className="g-2 me-2"),

        # Hamburger Toggler for mobile
        dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),

        # Collapsible Navigation Menu
        dbc.Collapse(
            dbc.Nav([
                # dbc.NavItem(dcc.Link("Reconciliation", href="/", className="nav-link text-dark")),
                # dbc.NavItem(dcc.Link("Trends", href="/trends", className="nav-link text-dark")),
                # dbc.NavItem(dcc.Link("Anomalies", href="/anomalies", className="nav-link text-dark")),
            ],
            className="w-100 d-flex flex-column flex-md-row align-items-start align-items-md-center"),
            id="navbar-collapse",
            is_open=False,
            navbar=True
        )
    ]),
    color="light",   # Light background
    dark=False,      # Text will be dark
    sticky="top",
    className="shadow-sm border-bottom"
)

# Callback to toggle menu on mobile
def register_navbar_callbacks(app):
    @app.callback(
        Output("navbar-collapse", "is_open"),
        Input("navbar-toggler", "n_clicks"),
        State("navbar-collapse", "is_open"),
        prevent_initial_call=True
    )
    def toggle_navbar(n_clicks, is_open):
        return not is_open if n_clicks else is_open
