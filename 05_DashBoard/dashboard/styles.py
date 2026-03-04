import streamlit as st


def apply_ui_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --jato-bg: #F8FAFC;
            --jato-card: #FFFFFF;
            --jato-border: #E2E8F0;
            --jato-text: #0F172A;
            --jato-subtle: #64748B;
        }

        [data-testid="stAppViewContainer"] {
            background: var(--jato-bg);
        }

        [data-testid="stSidebar"] {
            background: #F8FAFC;
        }

        .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 1500px;
        }

        h1, h2, h3 {
            letter-spacing: .1px;
        }

        div[data-testid="stMetric"] {
            background: var(--jato-card);
            border: 1px solid var(--jato-border);
            border-radius: 12px;
            padding: 8px 12px;
        }

        div[data-testid="stMetricLabel"] p {
            color: var(--jato-subtle);
        }

        div[data-testid="stMetricValue"] {
            color: var(--jato-text);
        }

        button[data-baseweb="tab"] {
            border-radius: 8px;
            padding: 8px 12px;
        }

        [data-testid="stSidebar"] .stButton > button {
            border-radius: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(fig):
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        margin=dict(l=12, r=12, t=56, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            title=None,
        ),
        hovermode="x unified",
        font=dict(size=12, color="#0F172A"),
    )
    fig.update_xaxes(showgrid=False, linecolor="#E2E8F0")
    fig.update_yaxes(showgrid=True, gridcolor="#E2E8F0", zeroline=False)
    return fig
