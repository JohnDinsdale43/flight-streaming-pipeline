import marimo

__generated_with = "0.19.9"
app = marimo.App(
    width="full",
    app_title="Flight On-Time Performance Dashboard",
)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # ✈️ Flight On-Time Performance Dashboard

        **KPI**: A flight is **on-time** if its delay is **≤ 15 minutes**.
        Use the filters below to slice by airline and route.
        """
    )
    return


@app.cell
def _():
    """Generate synthetic flight data and load into DuckDB."""
    import sys
    sys.path.insert(0, ".")

    import duckdb
    from src.generators.flight_generator import FlightDataGenerator
    from src.streaming.json_stream import stream_to_buffer
    from src.ingestion.arrow_loader import ndjson_buffer_to_arrow, arrow_to_duckdb

    # Generate 2,000 flights
    gen = FlightDataGenerator(seed=2026)
    records = gen.generate(2000)

    # Stream to NDJSON buffer -> Arrow -> DuckDB
    buf = stream_to_buffer(records)
    arrow_table = ndjson_buffer_to_arrow(buf.read())
    con = duckdb.connect()
    row_count = arrow_to_duckdb(arrow_table, con, table_name="flights")

    # Pre-compute the on-time flag and route column
    con.execute("""
        CREATE OR REPLACE TABLE flights_otp AS
        SELECT
            *,
            origin_airport || ' → ' || destination_airport AS route,
            CASE WHEN delay_minutes <= 15 THEN 1 ELSE 0 END AS is_ontime,
            CASE
                WHEN delay_minutes = 0  THEN 'On Time (0 min)'
                WHEN delay_minutes <= 15 THEN 'Minor (1-15 min)'
                WHEN delay_minutes <= 60 THEN 'Moderate (16-60 min)'
                ELSE 'Severe (>60 min)'
            END AS delay_bucket,
            EXTRACT(HOUR FROM CAST(scheduled_time AS TIMESTAMP)) AS sched_hour
        FROM flights
    """)

    print(f"✅ Loaded {row_count:,} flights into DuckDB")
    return con, row_count


@app.cell
def _(con, mo):
    """Build filter dropdowns from the data."""
    # Airline options
    airlines = [r[0] for r in con.execute(
        "SELECT DISTINCT airline FROM flights_otp ORDER BY airline"
    ).fetchall()]

    # Route options
    routes = [r[0] for r in con.execute(
        "SELECT DISTINCT route FROM flights_otp ORDER BY route"
    ).fetchall()]

    airline_filter = mo.ui.multiselect(
        options=airlines,
        label="Airlines",
    )

    route_filter = mo.ui.multiselect(
        options=routes,
        label="Routes",
    )

    mo.hstack(
        [airline_filter, route_filter],
        justify="start",
        gap=1.5,
    )
    return airline_filter, route_filter


@app.cell
def _(airline_filter, con, mo, route_filter):
    """Compute filtered OTP metrics and render KPI cards."""
    import plotly.express as px
    import plotly.graph_objects as go

    # Build dynamic WHERE clause
    clauses = []
    params = []
    if airline_filter.value:
        placeholders = ", ".join(["?" for _ in airline_filter.value])
        clauses.append(f"airline IN ({placeholders})")
        params.extend(airline_filter.value)
    if route_filter.value:
        placeholders = ", ".join(["?" for _ in route_filter.value])
        clauses.append(f"route IN ({placeholders})")
        params.extend(route_filter.value)

    where = "WHERE " + " AND ".join(clauses) if clauses else ""

    # ── KPI summary ──────────────────────────────────────────────
    kpi_sql = f"""
        SELECT
            COUNT(*)                          AS total_flights,
            SUM(is_ontime)                    AS ontime_count,
            ROUND(100.0 * SUM(is_ontime) / COUNT(*), 1) AS otp_pct,
            ROUND(AVG(delay_minutes), 1)      AS avg_delay,
            MAX(delay_minutes)                AS max_delay,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
        FROM flights_otp
        {where}
    """
    kpi = con.execute(kpi_sql, params).fetchone()
    total, ontime, otp_pct, avg_delay, max_delay, cancelled = kpi

    # ── KPI cards ────────────────────────────────────────────────
    def _card(title, value, subtitle="", color="#10b981"):
        return mo.md(f"""
<div style="
    background: linear-gradient(135deg, {color}22, {color}11);
    border: 1px solid {color}44;
    border-radius: 12px;
    padding: 20px 24px;
    text-align: center;
    min-width: 160px;
">
    <div style="font-size: 13px; color: #6b7280; font-weight: 500;">{title}</div>
    <div style="font-size: 32px; font-weight: 700; color: {color}; margin: 4px 0;">{value}</div>
    <div style="font-size: 12px; color: #9ca3af;">{subtitle}</div>
</div>
""")

    otp_color = "#10b981" if otp_pct >= 80 else "#f59e0b" if otp_pct >= 60 else "#ef4444"

    kpi_cards = mo.hstack(
        [
            _card("Total Flights", f"{total:,}", "in filtered set"),
            _card("On-Time Performance", f"{otp_pct}%", f"{ontime:,} of {total:,} flights", otp_color),
            _card("Avg Delay", f"{avg_delay} min", "across all flights", "#3b82f6"),
            _card("Max Delay", f"{max_delay} min", "worst single flight", "#8b5cf6"),
            _card("Cancelled", f"{cancelled:,}", "flights cancelled", "#ef4444"),
        ],
        justify="center",
        gap=1,
    )

    # ── OTP by Airline (bar chart) ───────────────────────────────
    otp_airline_sql = f"""
        SELECT
            airline,
            COUNT(*) AS total,
            ROUND(100.0 * SUM(is_ontime) / COUNT(*), 1) AS otp_pct
        FROM flights_otp
        {where}
        GROUP BY airline
        ORDER BY otp_pct DESC
    """
    otp_airline_df = con.execute(otp_airline_sql, params).fetchdf()

    fig_airline = px.bar(
        otp_airline_df,
        x="airline",
        y="otp_pct",
        color="otp_pct",
        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
        range_color=[50, 100],
        text="otp_pct",
        labels={"otp_pct": "OTP %", "airline": "Airline"},
        title="On-Time Performance by Airline (≤15 min delay)",
    )
    fig_airline.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_airline.update_layout(
        plot_bgcolor="#fafafa",
        xaxis_tickangle=-35,
        yaxis_range=[0, 105],
        coloraxis_showscale=False,
        height=420,
        margin=dict(t=50, b=80),
    )

    # ── OTP by Hour (line chart) ─────────────────────────────────
    otp_hour_sql = f"""
        SELECT
            sched_hour AS hour,
            COUNT(*) AS total,
            ROUND(100.0 * SUM(is_ontime) / COUNT(*), 1) AS otp_pct
        FROM flights_otp
        {where}
        GROUP BY sched_hour
        ORDER BY sched_hour
    """
    otp_hour_df = con.execute(otp_hour_sql, params).fetchdf()

    fig_hour = px.area(
        otp_hour_df,
        x="hour",
        y="otp_pct",
        markers=True,
        labels={"otp_pct": "OTP %", "hour": "Scheduled Hour (UTC)"},
        title="On-Time Performance by Hour of Day",
    )
    fig_hour.add_hline(
        y=80, line_dash="dash", line_color="#ef4444",
        annotation_text="80% target", annotation_position="top left",
    )
    fig_hour.update_layout(
        plot_bgcolor="#fafafa",
        yaxis_range=[0, 105],
        height=400,
        margin=dict(t=50, b=40),
    )
    fig_hour.update_traces(
        fill="tozeroy",
        fillcolor="rgba(16,185,129,0.12)",
        line_color="#10b981",
    )

    # ── Delay Distribution (pie chart) ───────────────────────────
    delay_dist_sql = f"""
        SELECT
            delay_bucket,
            COUNT(*) AS cnt
        FROM flights_otp
        {where}
        GROUP BY delay_bucket
        ORDER BY
            CASE delay_bucket
                WHEN 'On Time (0 min)' THEN 1
                WHEN 'Minor (1-15 min)' THEN 2
                WHEN 'Moderate (16-60 min)' THEN 3
                ELSE 4
            END
    """
    delay_dist_df = con.execute(delay_dist_sql, params).fetchdf()

    fig_pie = px.pie(
        delay_dist_df,
        names="delay_bucket",
        values="cnt",
        title="Delay Distribution",
        color="delay_bucket",
        color_discrete_map={
            "On Time (0 min)": "#10b981",
            "Minor (1-15 min)": "#6ee7b7",
            "Moderate (16-60 min)": "#f59e0b",
            "Severe (>60 min)": "#ef4444",
        },
        hole=0.4,
    )
    fig_pie.update_layout(height=400, margin=dict(t=50, b=20))

    # ── OTP by Route (top 20 bar chart) ──────────────────────────
    otp_route_sql = f"""
        SELECT
            route,
            COUNT(*) AS total,
            ROUND(100.0 * SUM(is_ontime) / COUNT(*), 1) AS otp_pct
        FROM flights_otp
        {where}
        GROUP BY route
        HAVING COUNT(*) >= 3
        ORDER BY otp_pct ASC
        LIMIT 20
    """
    otp_route_df = con.execute(otp_route_sql, params).fetchdf()

    fig_route = px.bar(
        otp_route_df,
        y="route",
        x="otp_pct",
        orientation="h",
        color="otp_pct",
        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
        range_color=[50, 100],
        text="otp_pct",
        labels={"otp_pct": "OTP %", "route": "Route"},
        title="Bottom 20 Routes by On-Time Performance (min 3 flights)",
    )
    fig_route.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_route.update_layout(
        plot_bgcolor="#fafafa",
        xaxis_range=[0, 110],
        coloraxis_showscale=False,
        height=520,
        margin=dict(l=140, t=50, b=40),
    )

    # ── Flight detail table ──────────────────────────────────────
    detail_sql = f"""
        SELECT
            flight_id,
            airline,
            route,
            flight_type,
            status,
            scheduled_time,
            delay_minutes,
            CASE WHEN is_ontime = 1 THEN '✅' ELSE '❌' END AS on_time,
            terminal,
            gate,
            aircraft_type
        FROM flights_otp
        {where}
        ORDER BY delay_minutes DESC
        LIMIT 200
    """
    detail_df = con.execute(detail_sql, params).fetchdf()

    # ── Layout ───────────────────────────────────────────────────
    mo.vstack([
        kpi_cards,
        mo.hstack([mo.as_html(fig_airline), mo.as_html(fig_pie)], widths=[0.6, 0.4]),
        mo.hstack([mo.as_html(fig_hour), mo.as_html(fig_route)], widths=[0.5, 0.5]),
        mo.md("### Flight Detail"),
        mo.ui.table(detail_df),
    ])
    return


if __name__ == "__main__":
    app.run()
