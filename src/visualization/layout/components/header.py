from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

header = dbc.Navbar(
    dbc.Container(
        [
            # Centered Brand Name
            dbc.Row(
                dbc.Col(
                    dcc.Link(
                        dbc.NavbarBrand(
                            "Calo Reconciliation Analysis",
                            className="fs-3 fw-bold text-white text-center"
                        ),
                        href="/",
                        refresh=True
                    ),
                    width=12,
                    className="text-center"
                ),
                align="center",
                justify="center",
                className="w-100"
            ),

            # Hamburger Toggler (still available for mobile menu if needed)
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                dbc.Nav(
                    [],
                    className="w-100 d-flex flex-column flex-md-row align-items-center justify-content-center"
                ),
                id="navbar-collapse",
                is_open=False,
                navbar=True
            ),
        ],
        fluid=True,
        className="justify-content-center"
    ),
    color="grey",  # Bootstrap primary blue
    dark=True,        # White text
    sticky="top",
    className="shadow border-bottom py-3"  # extra padding & shadow
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
