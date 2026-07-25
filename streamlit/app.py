import time

import boto3
import pandas as pd
import streamlit as st
import plotly.express as px


# ======================================================
# PAGE CONFIGURATION
# ======================================================

st.set_page_config(
    page_title="Healthcare Metrics Dashboard",
    page_icon="🏥",
    layout="wide"
)


# ======================================================
# AWS / ATHENA CONFIGURATION
# ======================================================

AWS_REGION = "us-east-1"

ATHENA_DATABASE = "healthcare_gold"

ATHENA_OUTPUT_LOCATION = (
    "s3://healthcare-target-data-bucket/"
    "athena-query-results/"
)


# ======================================================
# ATHENA CLIENT
# ======================================================

athena = boto3.client(
    "athena",
    region_name=AWS_REGION
)


# ======================================================
# STANDARDIZE COLUMN NAMES
# ======================================================

def standardize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
    )

    return df


# ======================================================
# RUN ATHENA QUERY
# ======================================================

def run_athena_query(query):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": ATHENA_DATABASE
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT_LOCATION
        }
    )

    query_execution_id = response["QueryExecutionId"]

    while True:

        query_status = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        state = (
            query_status["QueryExecution"]
            ["Status"]
            ["State"]
        )

        if state in [
            "SUCCEEDED",
            "FAILED",
            "CANCELLED"
        ]:
            break

        time.sleep(1)

    if state != "SUCCEEDED":

        reason = (
            query_status["QueryExecution"]
            ["Status"]
            .get(
                "StateChangeReason",
                "Unknown Athena error"
            )
        )

        raise Exception(
            f"Athena query failed: {reason}"
        )

    all_rows = []

    next_token = None

    while True:

        if next_token:

            results = athena.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token
            )

        else:

            results = athena.get_query_results(
                QueryExecutionId=query_execution_id
            )

        rows = (
            results["ResultSet"]["Rows"]
        )

        all_rows.extend(rows)

        next_token = results.get(
            "NextToken"
        )

        if not next_token:
            break

    if not all_rows:
        return pd.DataFrame()

    data = []

    for row in all_rows:

        values = []

        for column in row["Data"]:

            values.append(
                column.get("VarCharValue")
            )

        data.append(values)

    columns = data[0]

    records = data[1:]

    df = pd.DataFrame(
        records,
        columns=columns
    )

    return df


# ======================================================
# LOAD STAFFING METRICS
# ======================================================

@st.cache_data(ttl=3600)
def get_staffing_metrics():

    query = """
        SELECT *
        FROM staffing_metrics
    """

    df = run_athena_query(query)

    return standardize_columns(df)


# ======================================================
# LOAD FACILITY METRICS
# ======================================================

@st.cache_data(ttl=3600)
def get_facility_metrics():

    query = """
        SELECT
            provnum,
            provname,
            certified_beds,
            avg_residents,
            overall_rating,
            health_rating,
            staffing_rating,
            occupancy_rate,
            avg_staffing_ratio,
            staffing_pressure_index,
            state
        FROM facility_metrics
    """

    df = run_athena_query(query)

    return standardize_columns(df)


# ======================================================
# LOAD QUALITY METRICS
# ======================================================

@st.cache_data(ttl=3600)
def get_quality_metrics():

    query = """
        SELECT *
        FROM quality_metrics
    """

    df = run_athena_query(query)

    return standardize_columns(df)


# ======================================================
# LOAD PATIENT VOLUME
# ======================================================

@st.cache_data(ttl=3600)
def get_patient_volume():

    query = """
        SELECT *
        FROM top_patient_volume
    """

    df = run_athena_query(query)

    return standardize_columns(df)


# ======================================================
# CONVERT COLUMNS TO NUMERIC
# ======================================================

def convert_numeric_columns(df, columns):

    for column in columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    return df


# ======================================================
# LOAD ALL GOLD DATA
# ======================================================

try:

    staffing_df = get_staffing_metrics()

    facility_df = get_facility_metrics()

    quality_df = get_quality_metrics()

    patient_volume_df = get_patient_volume()

except Exception as e:

    st.error(
        f"Error loading Gold data from Athena: {e}"
    )

    st.stop()


# ======================================================
# NUMERIC CONVERSIONS
# ======================================================

staffing_df = convert_numeric_columns(
    staffing_df,
    [
        "TOTAL_NURSE_HOURS",
        "AVG_PATIENT_CENSUS",
        "RN_HOURS",
        "LPN_HOURS",
        "CNA_HOURS",
        "EMPLOYEE_HOURS",
        "CONTRACT_HOURS",
        "NURSE_TO_PATIENT_RATIO",
        "EMPLOYEE_STAFF_PERCENT",
        "CONTRACT_STAFF_PERCENT"
    ]
)


facility_df = convert_numeric_columns(
    facility_df,
    [
        "CERTIFIED_BEDS",
        "AVG_RESIDENTS",
        "OVERALL_RATING",
        "HEALTH_RATING",
        "STAFFING_RATING",
        "OCCUPANCY_RATE",
        "AVG_STAFFING_RATIO",
        "STAFFING_PRESSURE_INDEX"
    ]
)


quality_df = convert_numeric_columns(
    quality_df,
    [
        "OVERALL_RATING",
        "HEALTH_RATING",
        "STAFFING_RATING",
        "FACILITY_QUALITY_SCORE"
    ]
)


patient_volume_df = convert_numeric_columns(
    patient_volume_df,
    [
        "TOTAL_PATIENT_DAYS"
    ]
)


# ======================================================
# VALIDATE FACILITY DATA
# ======================================================

required_columns = [
    "STATE",
    "PROVNUM",
    "PROVNAME"
]

missing_columns = [
    column
    for column in required_columns
    if column not in facility_df.columns
]

if missing_columns:

    st.error(
        f"Missing columns in facility_metrics: "
        f"{missing_columns}"
    )

    st.write(
        "Available columns:",
        facility_df.columns.tolist()
    )

    st.stop()


# ======================================================
# PAGE TITLE
# ======================================================

st.title(
    "🏥 Healthcare Metrics Dashboard"
)

st.write(
    "Healthcare staffing, facility quality, "
    "occupancy, staffing pressure, and "
    "patient volume analytics."
)


# ======================================================
# SIDEBAR FILTER
# ======================================================

st.sidebar.header(
    "Dashboard Filters"
)

states = sorted(
    facility_df["STATE"]
    .dropna()
    .unique()
)

selected_state = st.sidebar.selectbox(
    "Select State",
    ["All States"] + list(states)
)


# ======================================================
# APPLY STATE FILTER
# ======================================================

if selected_state == "All States":

    filtered_staffing = staffing_df.copy()

    filtered_facility = facility_df.copy()

    filtered_quality = quality_df.copy()

    filtered_patient_volume = (
        patient_volume_df.copy()
    )

else:

    filtered_facility = facility_df[
        facility_df["STATE"] == selected_state
    ].copy()

    filtered_quality = quality_df[
        quality_df["STATE"] == selected_state
    ].copy()

    filtered_patient_volume = (
        patient_volume_df[
            patient_volume_df["STATE"]
            == selected_state
        ].copy()
    )

    if "STATE" in staffing_df.columns:

        filtered_staffing = staffing_df[
            staffing_df["STATE"] == selected_state
        ].copy()

    else:

        filtered_staffing = staffing_df.copy()


# ======================================================
# KPI CARDS
# ======================================================

st.header(
    "Key Performance Indicators"
)

col1, col2, col3, col4 = st.columns(4)


with col1:

    total_facilities = (
        filtered_facility["PROVNUM"]
        .nunique()
    )

    st.metric(
        "Total Facilities",
        f"{total_facilities:,}"
    )


with col2:

    total_nurse_hours = (
        filtered_staffing["TOTAL_NURSE_HOURS"]
        .sum()
    )

    st.metric(
        "Total Nurse Hours",
        f"{total_nurse_hours:,.0f}"
    )


with col3:

    average_occupancy = (
        filtered_facility["OCCUPANCY_RATE"]
        .mean()
    )

    st.metric(
        "Average Occupancy Rate",
        f"{average_occupancy:.2f}%"
    )


with col4:

    average_quality = (
        filtered_quality[
            "FACILITY_QUALITY_SCORE"
        ].mean()
    )

    st.metric(
        "Average Quality Score",
        f"{average_quality:.2f}"
    )


# ======================================================
# STAFFING BAR CHARTS
# ======================================================

st.header(
    "👩‍⚕️ Staffing Metrics"
)

col1, col2 = st.columns(2)


# ------------------------------------------------------
# TOTAL HOURS BY STAFF TYPE
# ------------------------------------------------------

with col1:

    staff_hours = pd.DataFrame(
        {
            "STAFF_TYPE": [
                "RN",
                "LPN",
                "CNA"
            ],
            "HOURS": [
                filtered_staffing[
                    "RN_HOURS"
                ].sum(),

                filtered_staffing[
                    "LPN_HOURS"
                ].sum(),

                filtered_staffing[
                    "CNA_HOURS"
                ].sum()
            ]
        }
    )

    fig = px.bar(
        staff_hours,
        x="STAFF_TYPE",
        y="HOURS",
        title="Total Hours by Staff Type",
        text_auto=True
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ------------------------------------------------------
# EMPLOYEE VS CONTRACT HOURS
# ------------------------------------------------------

with col2:

    staff_distribution = pd.DataFrame(
        {
            "STAFF_TYPE": [
                "Employee Hours",
                "Contract Hours"
            ],
            "HOURS": [
                filtered_staffing[
                    "EMPLOYEE_HOURS"
                ].sum(),

                filtered_staffing[
                    "CONTRACT_HOURS"
                ].sum()
            ]
        }
    )

    fig = px.bar(
        staff_distribution,
        x="STAFF_TYPE",
        y="HOURS",
        title="Employee vs Contract Hours",
        text_auto=True
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ======================================================
# FACILITY BAR CHARTS
# ======================================================

st.header(
    "🏢 Facility Metrics"
)

col1, col2 = st.columns(2)


# ------------------------------------------------------
# TOP FACILITIES BY OCCUPANCY
# ------------------------------------------------------

with col1:

    occupancy_display = (
        filtered_facility
        .dropna(
            subset=[
                "OCCUPANCY_RATE"
            ]
        )
        .sort_values(
            "OCCUPANCY_RATE",
            ascending=False
        )
        .head(10)
    )

    fig = px.bar(
        occupancy_display,
        x="OCCUPANCY_RATE",
        y="PROVNAME",
        orientation="h",
        title="Top 10 Facilities by Occupancy Rate",
        text_auto=".2f"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ------------------------------------------------------
# TOP FACILITIES BY STAFFING PRESSURE
# ------------------------------------------------------

with col2:

    pressure_display = (
        filtered_facility
        .dropna(
            subset=[
                "STAFFING_PRESSURE_INDEX"
            ]
        )
        .sort_values(
            "STAFFING_PRESSURE_INDEX",
            ascending=False
        )
        .head(10)
    )

    fig = px.bar(
        pressure_display,
        x="STAFFING_PRESSURE_INDEX",
        y="PROVNAME",
        orientation="h",
        title="Top 10 Facilities by Staffing Pressure",
        text_auto=".2f"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ======================================================
# NURSE-TO-PATIENT RATIO
# ======================================================

st.header(
    "👨‍⚕️ Nurse-to-Patient Ratios"
)

staffing_ratio_display = (
    filtered_staffing
    .dropna(
        subset=[
            "NURSE_TO_PATIENT_RATIO"
        ]
    )
    .groupby(
        [
            "PROVNUM",
            "PROVNAME",
            "STATE"
        ],
        as_index=False
    )[
        "NURSE_TO_PATIENT_RATIO"
    ]
    .mean()
    .sort_values(
        "NURSE_TO_PATIENT_RATIO",
        ascending=False
    )
    .head(10)
)


if not staffing_ratio_display.empty:

    fig = px.bar(
        staffing_ratio_display,
        x="NURSE_TO_PATIENT_RATIO",
        y="PROVNAME",
        orientation="h",
        title=(
            "Top 10 Facilities by "
            "Nurse-to-Patient Ratio"
        ),
        text_auto=".2f"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ======================================================
# QUALITY BAR CHARTS
# ======================================================

st.header(
    "⭐ Facility Quality Metrics"
)

col1, col2 = st.columns(2)


# ------------------------------------------------------
# TOP FACILITIES BY QUALITY SCORE
# ------------------------------------------------------

with col1:

    quality_display = (
        filtered_quality
        .dropna(
            subset=[
                "FACILITY_QUALITY_SCORE"
            ]
        )
        .sort_values(
            "FACILITY_QUALITY_SCORE",
            ascending=False
        )
        .head(10)
    )

    fig = px.bar(
        quality_display,
        x="FACILITY_QUALITY_SCORE",
        y="PROVNAME",
        orientation="h",
        title="Top 10 Facilities by Quality Score",
        text_auto=".2f"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ------------------------------------------------------
# AVERAGE QUALITY RATINGS
# ------------------------------------------------------

with col2:

    quality_ratings = pd.DataFrame(
        {
            "RATING_TYPE": [
                "Overall Rating",
                "Health Rating",
                "Staffing Rating"
            ],
            "AVERAGE_SCORE": [
                filtered_quality[
                    "OVERALL_RATING"
                ].mean(),

                filtered_quality[
                    "HEALTH_RATING"
                ].mean(),

                filtered_quality[
                    "STAFFING_RATING"
                ].mean()
            ]
        }
    )

    fig = px.bar(
        quality_ratings,
        x="RATING_TYPE",
        y="AVERAGE_SCORE",
        title="Average Quality Ratings",
        text_auto=".2f"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ======================================================
# PATIENT VOLUME
# ======================================================

st.header(
    "👥 Top Patient Volume Facilities"
)

patient_volume_display = (
    filtered_patient_volume
    .dropna(
        subset=[
            "TOTAL_PATIENT_DAYS"
        ]
    )
    .sort_values(
        "TOTAL_PATIENT_DAYS",
        ascending=False
    )
    .head(10)
)


if not patient_volume_display.empty:

    fig = px.bar(
        patient_volume_display,
        x="TOTAL_PATIENT_DAYS",
        y="PROVNAME",
        orientation="h",
        title="Top 10 Facilities by Patient Days",
        text_auto=True
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ======================================================
# QUALITY HEATMAP
# ======================================================

st.header(
    "🔥 Quality Heatmap"
)

quality_heatmap = (
    filtered_quality
    .groupby(
        "STATE",
        as_index=False
    )
    .agg(
        AVG_QUALITY_SCORE=(
            "FACILITY_QUALITY_SCORE",
            "mean"
        ),
        AVG_OVERALL_RATING=(
            "OVERALL_RATING",
            "mean"
        ),
        AVG_HEALTH_RATING=(
            "HEALTH_RATING",
            "mean"
        ),
        AVG_STAFFING_RATING=(
            "STAFFING_RATING",
            "mean"
        )
    )
)


quality_heatmap_pivot = (
    quality_heatmap
    .set_index("STATE")
    [
        [
            "AVG_QUALITY_SCORE",
            "AVG_OVERALL_RATING",
            "AVG_HEALTH_RATING",
            "AVG_STAFFING_RATING"
        ]
    ]
)


fig = px.imshow(
    quality_heatmap_pivot,
    aspect="auto",
    title="Facility Quality Metrics by State",
    labels={
        "x": "Quality Metric",
        "y": "State",
        "color": "Score"
    }
)

st.plotly_chart(
    fig,
    use_container_width=True
)


# ======================================================
# STAFFING HEATMAP
# ======================================================

st.header(
    "🔥 Staffing Heatmap"
)

staffing_heatmap = (
    filtered_staffing
    .groupby(
        "STATE",
        as_index=False
    )
    .agg(
        TOTAL_NURSE_HOURS=(
            "TOTAL_NURSE_HOURS",
            "sum"
        ),
        RN_HOURS=(
            "RN_HOURS",
            "sum"
        ),
        LPN_HOURS=(
            "LPN_HOURS",
            "sum"
        ),
        CNA_HOURS=(
            "CNA_HOURS",
            "sum"
        ),
        EMPLOYEE_HOURS=(
            "EMPLOYEE_HOURS",
            "sum"
        ),
        CONTRACT_HOURS=(
            "CONTRACT_HOURS",
            "sum"
        )
    )
)


staffing_heatmap_pivot = (
    staffing_heatmap
    .set_index("STATE")
    [
        [
            "TOTAL_NURSE_HOURS",
            "RN_HOURS",
            "LPN_HOURS",
            "CNA_HOURS",
            "EMPLOYEE_HOURS",
            "CONTRACT_HOURS"
        ]
    ]
)


fig = px.imshow(
    staffing_heatmap_pivot,
    aspect="auto",
    title="Staffing Metrics by State",
    labels={
        "x": "Staffing Metric",
        "y": "State",
        "color": "Hours"
    }
)

st.plotly_chart(
    fig,
    use_container_width=True
)