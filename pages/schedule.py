"""
Streamlit app page displaying CTA expected schedule.
"""

import datetime

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from pages.utils.utils import load_s3_parquet_data


# Reusable functions
def create_scheduled_trains_histogram(df: pd.DataFrame, color: str) -> alt.LayerChart:
    """
    Creates histogram using Altair to display the scheduled trains for a given train line
    and schedule type (weekday, Saturday, Sunday) binned by hour.

    Args:
        df: Input Pandas dataframe containing the data for the train line and schedule type
        color: String representing hexcode color for bars

    Returns:
        alt.LayerChart: Layered Altair bar chart
    """
    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("new_trips_started:Q", title="Trains Scheduled"),
            tooltip=["new_trips_started"],
        )
    )

    # Create text layer explicitly bound to the same Y-axis
    text = chart.mark_text(align="center", baseline="bottom", dy=-5).encode(
        text="new_trips_started"
    )

    return (
        (chart + text)
        .properties(title="Trains Scheduled by Hour")
        .configure_view(stroke=None)
    )


def create_average_headway_histogram(df: pd.DataFrame, color: str) -> alt.LayerChart:
    """
    Creates histogram using Altair to display the average headway for a given train line
    and schedule type (weekday, Saturday, Sunday) binned by hour.

    Args:
        df: Input Pandas dataframe containing the data for the train line and schedule type
        color: String representing hexcode color for bars

    Returns:
        alt.LayerChart: Layered Altair bar chart
    """
    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("avg_headway:Q", title="Average Headway (Minutes)"),
            tooltip=["avg_headway_mmss"],
        )
    )

    # Create text layer explicitly bound to the same Y-axis
    text = chart.mark_text(align="center", baseline="bottom", dy=-5).encode(
        text="avg_headway_mmss"
    )

    return (
        (chart + text)
        .properties(title="Average Duration Between Scheduled Trains (Headway)")
        .configure_view(stroke=None)
    )


def create_aggregate_scheduled_trains_bar_chart(df: pd.DataFrame) -> alt.LayerChart:
    """
    Creates a bar chart using Altair to display the aggregate count of
    trains scheduled for a given train line and schedule type (weekday, Saturday, Sunday)

    Args:
        df: Input Pandas dataframe containing the data for the train line and schedule type

    Returns:
        alt.LayerChart: Layered Altair bar chart
    """
    # Create a base chart with shared encodings
    base = alt.Chart(df).encode(
        x=alt.X(
            "EFFECTIVE_DATE:T", title="Effective Date", axis=alt.Axis(format="%m/%d/%Y")
        ),
        y=alt.Y("SCHEDULED_RUNS:Q", title="Scheduled Runs"),
        color=alt.Color(
            "LINE:N",
            scale=alt.Scale(domain=df["LINE"].tolist(), range=df["HEX_CODE"].tolist()),
            legend=None,
        ),
        xOffset="LINE:N",
    )

    # Combine bars and text
    bars = base.mark_bar(size=100).encode(
        tooltip=[alt.Tooltip("SCHEDULED_RUNS:Q", title="Scheduled Runs", format=",")]
    )

    text = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-5,  # Adjusts vertical position so text sits above the bar
    ).encode(text="SCHEDULED_RUNS:Q", color=alt.value("black"))

    return (
        (bars + text)
        .properties(title="Scheduled Runs Over Time")
        .configure_view(stroke=None)
    )


# Initial page setup and source data load
st.title("Schedule")
historical_schedule_data = load_s3_parquet_data(
    s3_path=f"s3://{st.secrets['env']['ACCOUNT_NUMBER']}-cta-analytics-project/gtfs_expected_cta_schedule/*.parquet"
)
current_effective_date = str(historical_schedule_data["start_date"].max())
formatted_current_effective_date = datetime.datetime.strptime(
    current_effective_date, "%Y%m%d"
).strftime("%m/%d/%Y")
direction_filter = None
train_lines = sorted(historical_schedule_data["route_long_name"].unique())

col1, col2 = st.columns(2)
with col1:
    line = st.selectbox(label="Line", options=train_lines)
with col2:
    schedule_type = st.selectbox(
        label="Schedule", options=["Weekday", "Saturday", "Sunday"]
    )

# SQL queries and resulting dataframes
filtered_schedules = """
    SELECT
        *,
        CASE 
            WHEN ? = 'Weekday' THEN monday + tuesday + wednesday + thursday + friday
            WHEN ? = 'Saturday' THEN saturday
            WHEN ? = 'Sunday' THEN sunday
            ELSE 0 
        END AS runs_calculation
    FROM historical_schedule_data
    WHERE runs_calculation > 0
    AND route_long_name = ?
    """
df_filtered_schedules = duckdb.query(
    query=filtered_schedules, params=[schedule_type, schedule_type, schedule_type, line]
).df()
duckdb.register("filtered_schedules", df_filtered_schedules)

chart_color = f"#{df_filtered_schedules['route_color'].max()}"

trains_per_hour = """
    WITH trip_starts AS (
        SELECT
            trip_id,
            MIN(arrival_time) AS first_arrival
        FROM filtered_schedules
        GROUP BY trip_id
    )
    SELECT
        split_part(first_arrival, ':', 1)::INTEGER AS hour,
        COUNT(trip_id) AS new_trips_started
    FROM trip_starts
    GROUP BY 1
    ORDER BY 1;
    """
df_trains_per_hour = duckdb.query(query=trains_per_hour).df()

average_headway = """
    WITH split_data AS (
        SELECT
            stop_id,
            (split_part(arrival_time, ':', 1)::INTEGER * 3600 +
            split_part(arrival_time, ':', 2)::INTEGER * 60 +
            split_part(arrival_time, ':', 3)::INTEGER) AS arrival_seconds
        FROM filtered_schedules
    ),
    lagged_data AS (
        SELECT
            stop_id,
            arrival_seconds,
            LAG(arrival_seconds) OVER (
                PARTITION BY stop_id 
                ORDER BY arrival_seconds
            ) AS prev_seconds
        FROM split_data
    ),
    headway_calc AS (
        SELECT
            floor((arrival_seconds / 3600) % 24) AS hour,
            (arrival_seconds - prev_seconds) / 60.0 AS headway_minutes
        FROM lagged_data
        WHERE prev_seconds IS NOT NULL
    )
    SELECT
        hour,
        AVG(headway_minutes) AS avg_headway,
        format('{:02d}:{:02d}', 
            (round(AVG(headway_minutes) * 60) / 60)::INTEGER, 
            (round(AVG(headway_minutes) * 60) % 60)::INTEGER
        ) AS avg_headway_mmss
    FROM headway_calc
    GROUP BY hour
    ORDER BY hour;
    """
df_average_headway = duckdb.query(query=average_headway).df()

total_trains = """
    SELECT
        ROUTE_LONG_NAME AS LINE,
        strptime(CAST(START_DATE AS VARCHAR), '%Y%m%d') AS EFFECTIVE_DATE,
        CONCAT('#', ROUTE_COLOR) AS HEX_CODE,
        CAST(COUNT(DISTINCT TRIP_ID) AS INTEGER) AS SCHEDULED_RUNS
    FROM filtered_schedules
    GROUP BY
        ROUTE_LONG_NAME,
        START_DATE,
        ROUTE_COLOR,
    ORDER BY SCHEDULED_RUNS DESC
    """

df_aggregate_scheduled_trains = duckdb.query(query=total_trains).df()

# Individual charts
hourly_scheduled_runs_chart = create_scheduled_trains_histogram(
    df=df_trains_per_hour, color=chart_color
)
average_headway_chart = create_average_headway_histogram(
    df=df_average_headway, color=chart_color
)
scheduled_weekday_trains_chart = create_aggregate_scheduled_trains_bar_chart(
    df=df_aggregate_scheduled_trains
)

# Final chart additions
tab1, tab2, tab3 = st.tabs(["Current Schedule", "Average Headway", "Historical Data"])
with tab1:
    st.altair_chart(hourly_scheduled_runs_chart)
    st.caption(
        """
        ** Note that there is no direction filter (North/South). This is because GTFS data only provides the 
        northbound/southbound distinction for the Red, Blue, and Green line schedules. All other lines are 
        treated as if they are constantly going in the same direction, which is inaccurate.
        """
    )
    st.caption(
        f"""
        ** Metrics are based on current schedule, which is active as of {formatted_current_effective_date}
        """
    )
with tab2:
    st.altair_chart(average_headway_chart)
    st.caption(
        """
        ** Note that there is no direction filter (North/South). This is because GTFS data only provides the 
        northbound/southbound distinction for the Red, Blue, and Green line schedules. All other lines are 
        treated as if they are constantly going in the same direction, which is inaccurate.
        """
    )
    st.caption(
        f"""
        ** Metrics are based on current schedule, which is active as of {formatted_current_effective_date}
        """
    )
with tab3:
    st.altair_chart(scheduled_weekday_trains_chart)
    st.caption(
        """
        ** Note that there is no direction filter (North/South). This is because GTFS data only provides the 
        northbound/southbound distinction for the Red, Blue, and Green line schedules. All other lines are 
        treated as if they are constantly going in the same direction, which is inaccurate.
        """
    )
