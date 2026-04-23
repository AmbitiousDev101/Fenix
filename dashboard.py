"""
Fenix - Streamlit Dashboard
Visual dashboard with 4 pages: League Standings, Team Form, Head to Head, Iceberg Snapshots.

Run with: python -m streamlit run dashboard.py
"""

import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path
import os

from config.settings import DUCKDB_PATH, LAKEHOUSE_PATH

# -- Page Config ---------------------------------------------------------------

st.set_page_config(
    page_title="Fenix ⚽",
    page_icon="🦅",
    layout="wide",
)

# -- Custom CSS (Premium Modern Look) ------------------------------------------

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }

    .stApp {
        background: radial-gradient(circle at 10% 20%, #0f0c29 0%, #302b63 50%, #24243e 100%);
    }
    
    .stApp > header {
        background-color: transparent;
    }

    /* Titles */
    .main-title {
        text-align: center;
        background: linear-gradient(90deg, #ff6b35, #f7c948, #ff6b35);
        background-size: 200% auto;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 5rem;
        font-weight: 800;
        margin-bottom: 0;
        letter-spacing: -2px;
        animation: shine 3s linear infinite;
    }
    
    @keyframes shine {
        to { background-position: 200% center; }
    }

    .sub-title {
        text-align: center;
        color: #e0e0ff;
        font-size: 1.2rem;
        margin-top: -15px;
        opacity: 0.8;
        font-weight: 300;
        text-transform: uppercase;
        letter-spacing: 4px;
    }

    /* Custom Metric Cards (No Truncation) */
    .metric-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        text-align: center;
        height: 100%;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        border: 1px solid rgba(255, 107, 53, 0.5);
        background: rgba(255, 255, 255, 0.08);
    }
    .metric-label {
        color: #ff6b35;
        font-weight: 700;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
    }
    .metric-value {
        color: #ffffff;
        font-size: 1.8rem;
        font-weight: 800;
        line-height: 1.2;
    }

    /* Dataframes */
    div[data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.1);
        box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: rgba(10, 10, 20, 0.95);
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    .stRadio > label {
        color: #f7c948 !important;
        font-weight: 700 !important;
    }

    /* Section Headers */
    h1, h2, h3 {
        color: #ffffff !important;
        font-weight: 700 !important;
    }
</style>
""", unsafe_allow_html=True)


# -- Helpers -------------------------------------------------------------------

@st.cache_resource
def get_connection():
    """Create DuckDB connection with Iceberg extension."""
    conn = duckdb.connect(str(DUCKDB_PATH))
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    return conn


def iceberg_path(layer: str, table: str) -> str:
    """Build iceberg_scan path."""
    return str(LAKEHOUSE_PATH / layer / table).replace("\\", "/")


def check_data_exists() -> bool:
    """Check if Iceberg tables have been created."""
    # Check for team_form metadata
    path = LAKEHOUSE_PATH / "gold" / "team_form" / "metadata"
    return path.exists() and any(path.iterdir())


def custom_metric(label, value):
    """Render a custom metric card to avoid Streamlit truncation."""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
    </div>
    """, unsafe_allow_html=True)


# -- Header --------------------------------------------------------------------

st.markdown('<h1 class="main-title">FENIX</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">Data Lakehouse Architecture</p>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# -- Sidebar Navigation -------------------------------------------------------

st.sidebar.image("https://img.icons8.com/fluency/96/phoenix.png", width=80)
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select View",
    ["📊 League Standings", "🔥 Team Form", "⚔️ Head to Head", "📸 Iceberg Snapshots"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.info(
    "**Tech Stack:**\n"
    "• PySpark / Apache Iceberg\n"
    "• Redpanda (Kafka)\n"
    "• dbt / DuckDB\n"
    "• Streamlit"
)

# Check data exists
if not check_data_exists():
    st.warning("⚠️ No pipeline data found yet! Run the pipeline first:")
    st.code("docker exec -it -w /opt/airflow/fenix fenix-airflow python orchestrate.py --full", language="bash")
    st.stop()

conn = get_connection()

# ------------------------------------------------------------------------------
# PAGE 1: League Standings
# ------------------------------------------------------------------------------
if page == "📊 League Standings":
    st.header("🏆 Premier League Table")

    try:
        path = iceberg_path("gold", "team_form")
        standings = conn.execute(f"""
            SELECT team_name AS "Team",
                   COUNT(*) AS "P",
                   SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS "W",
                   SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS "D",
                   SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS "L",
                   SUM(goals_scored) AS "GF",
                   SUM(goals_conceded) AS "GA",
                   SUM(goals_scored) - SUM(goals_conceded) AS "GD",
                   SUM(points) AS "Pts"
            FROM iceberg_scan('{path}', allow_moved_paths = true)
            GROUP BY team_name
            ORDER BY "Pts" DESC, "GD" DESC, "GF" DESC
        """).fetchdf()

        if not standings.empty:
            standings.insert(0, "Pos", range(1, len(standings) + 1))

            col1, col2, col3 = st.columns(3)
            with col1:
                custom_metric("Top Team", standings.iloc[0]["Team"])
            with col2:
                custom_metric("Total Teams", len(standings))
            with col3:
                custom_metric("Total Matches", int(standings["P"].sum() // 2))

            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(standings, use_container_width=True, hide_index=True)
        else:
            st.info("No standings data available.")
    except Exception as e:
        st.error(f"Error loading standings: {e}")

# ------------------------------------------------------------------------------
# PAGE 2: Team Form
# ------------------------------------------------------------------------------
elif page == "🔥 Team Form":
    st.header("📈 Team Performance Deep-Dive")

    try:
        path = iceberg_path("gold", "team_form")
        teams = conn.execute(f"""
            SELECT DISTINCT team_name
            FROM iceberg_scan('{path}', allow_moved_paths = true)
            ORDER BY team_name
        """).fetchdf()

        if teams.empty:
            st.info("No team data available.")
            st.stop()

        selected_team = st.selectbox("Analyze Team", teams["team_name"].tolist())

        form_data = conn.execute(f"""
            SELECT team_name, utc_date, venue, goals_scored, goals_conceded,
                   result, points, points_last5,
                   goals_scored_rolling_avg_5 AS avg_gf,
                   goals_conceded_rolling_avg_5 AS avg_gc,
                   form_trend
            FROM iceberg_scan('{path}', allow_moved_paths = true)
            WHERE team_name = '{selected_team}'
            ORDER BY utc_date DESC
            LIMIT 20
        """).fetchdf()

        if not form_data.empty:
            latest = form_data.iloc[0]

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                trend_color = "🟢" if latest["form_trend"] == "improving" else "🟡"
                custom_metric("Form Trend", f"{trend_color} {latest['form_trend'].title()}")
            with c2:
                custom_metric("Points (Last 5)", int(latest["points_last5"]))
            with c3:
                custom_metric("Avg Goals (5)", f"{latest['avg_gf']:.1f}")
            with c4:
                custom_metric("Avg Conceded (5)", f"{latest['avg_gc']:.1f}")

            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Recent Match History")
            st.dataframe(
                form_data[["utc_date", "venue", "goals_scored", "goals_conceded",
                          "result", "points", "points_last5", "form_trend"]],
                use_container_width=True, hide_index=True
            )

            # Rolling averages chart
            chart_data = form_data[["utc_date", "avg_gf", "avg_gc"]].sort_values("utc_date")
            chart_data = chart_data.set_index("utc_date")
            st.subheader("Rolling Goal Averages (Last 5)")
            st.line_chart(chart_data)
        else:
            st.info(f"No form data for {selected_team}.")

    except Exception as e:
        st.error(f"Error loading team form: {e}")

# ------------------------------------------------------------------------------
# PAGE 3: Head to Head
# ------------------------------------------------------------------------------
elif page == "⚔️ Head to Head":
    st.header("⚔️ Rivalry Statistics")

    try:
        path_tf = iceberg_path("gold", "team_form")
        teams = conn.execute(f"""
            SELECT DISTINCT team_name
            FROM iceberg_scan('{path_tf}', allow_moved_paths = true)
            ORDER BY team_name
        """).fetchdf()

        team_list = teams["team_name"].tolist()
        col1, col2 = st.columns(2)
        with col1:
            team_a = st.selectbox("Team A", team_list, index=0)
        with col2:
            team_b = st.selectbox("Team B", team_list, index=min(1, len(team_list) - 1))

        path_mr = iceberg_path("gold", "match_results")
        h2h = conn.execute(f"""
            SELECT utc_date AS "Date", home_team_name AS "Home", away_team_name AS "Away",
                   home_score AS "HG", away_score AS "AG", result AS "Result"
            FROM iceberg_scan('{path_mr}', allow_moved_paths = true)
            WHERE (home_team_name = '{team_a}' AND away_team_name = '{team_b}')
               OR (home_team_name = '{team_b}' AND away_team_name = '{team_a}')
            ORDER BY utc_date DESC
        """).fetchdf()

        if not h2h.empty:
            total = len(h2h)
            a_wins = len(h2h[
                ((h2h["Home"] == team_a) & (h2h["Result"] == "home_win")) |
                ((h2h["Away"] == team_a) & (h2h["Result"] == "away_win"))
            ])
            b_wins = len(h2h[
                ((h2h["Home"] == team_b) & (h2h["Result"] == "home_win")) |
                ((h2h["Away"] == team_b) & (h2h["Result"] == "away_win"))
            ])
            draws = total - a_wins - b_wins

            st.markdown("<br>", unsafe_allow_html=True)
            c1, c2, c3, c4 = st.columns(4)
            with c1: custom_metric("Matches", total)
            with c2: custom_metric(f"{team_a} Wins", a_wins)
            with c3: custom_metric("Draws", draws)
            with c4: custom_metric(f"{team_b} Wins", b_wins)

            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(h2h, use_container_width=True, hide_index=True)
        else:
            st.info(f"No match history found between {team_a} and {team_b}.")

    except Exception as e:
        st.error(f"Error loading H2H data: {e}")

# ------------------------------------------------------------------------------
# PAGE 4: Iceberg Snapshots
# ------------------------------------------------------------------------------
elif page == "📸 Iceberg Snapshots":
    st.header("🕰️ Iceberg Time-Travel Explorer")
    st.markdown("Apache Iceberg maintains an immutable history of every update. These snapshots are captured directly from the metadata layer.")

    tables = {
        "Silver Matches": ("silver", "matches"),
        "Silver Standings": ("silver", "standings"),
        "Gold Team Form": ("gold", "team_form"),
        "Gold Match Results": ("gold", "match_results"),
    }

    for label, (layer, table) in tables.items():
        path = iceberg_path(layer, table)
        metadata_path = LAKEHOUSE_PATH / layer / table / "metadata"
        
        if metadata_path.exists():
            try:
                # Query the snapshots metadata table using DuckDB Iceberg extension column names
                snapshots = conn.execute(f"""
                    SELECT 
                        snapshot_id, 
                        epoch_ms(timestamp_ms) as committed_at, 
                        manifest_list
                    FROM iceberg_snapshots('{path}')
                    ORDER BY timestamp_ms DESC
                """).fetchdf()

                with st.expander(f"📋 {label} ({len(snapshots)} snapshots)", expanded=True):
                    st.dataframe(snapshots, use_container_width=True, hide_index=True)
                    if not snapshots.empty:
                        latest = snapshots.iloc[0]
                        st.success(f"Latest Commit: {latest['committed_at']}")
            except Exception as e:
                with st.expander(f"📋 {label}"):
                    st.warning(f"Error reading snapshots: {e}")
        else:
            with st.expander(f"📋 {label}"):
                st.info(f"Table metadata not found. Run the pipeline to create this table.")

# -- Footer --------------------------------------------------------------------

st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.markdown(
    '<p style="text-align: center; color: #a0a0b8; font-size: 0.8rem;">'
    '🦅 FENIX Football Lakehouse | PySpark • Iceberg • Kafka • dbt • DuckDB'
    '</p>',
    unsafe_allow_html=True,
)
