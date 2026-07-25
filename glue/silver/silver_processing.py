import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name


# ============================================================
# 1. Initialize AWS Glue Job
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ============================================================
# 2. Define S3 Paths
# ============================================================

bronze_path = "s3://healthcare-target-data-bucket/bronze/"
silver_path = "s3://healthcare-target-data-bucket/silver/"


# ============================================================
# 3. Define Input Files
# ============================================================

provider_file = "NH_ProviderInfo_Oct2024.csv"
state_average_file = "NH_StateUSAverages_Oct2024.csv"
survey_dates_file = "NH_SurveyDates_Oct2024.csv"
pbj_file = "PBJ_Daily_Nurse_Staffing_Q2_2024.csv"


# ============================================================
# 4. Read CSV Function
# ============================================================

def read_csv(file_name):

    file_path = bronze_path + file_name

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(file_path)
    )


# ============================================================
# 5. Transform Provider Information
# ============================================================

def transform_provider_info(df):

    # Replace these column names with the actual columns
    # that you want to remove

    columns_to_drop = [
        "Average Number of Residents per Day Footnote",
        "Overall Rating Footnote",
        "Health Inspection Rating Footnote",
        "QM Rating Footnote",
        "Reported Staffing Footnote",
        "Physical Therapist Staffing Footnote",
        "Special Focus Status",
        "Long-Stay QM Rating Footnote",
        "Geocoding Footnote",
        "Staffing Rating Footnote",
        "Total nursing staff turnover footnote",
        "Administrator turnover footnote",
        "Registered Nurse turnover footnote",
        "Short-Stay QM Rating Footnote"
    ]

    df = df.drop(*columns_to_drop)

    return df


# ============================================================
# 6. Transform State USAverages
# ============================================================

def transform_state_averages(df):

    # Replace these with the actual columns
    # that you want to remove

    columns_to_drop = [
        "Average Number of Residents per Day Footnote",
        "Overall Rating Footnote",
        "Health Inspection Rating Footnote",
        "QM Rating Footnote",
        "Reported Staffing Footnote",
        "Physical Therapist Staffing Footnote",
        "Special Focus Status",
        "Long-Stay QM Rating Footnote",
        "Geocoding Footnote",
        "Staffing Rating Footnote",
        "Total nursing staff turnover footnote",
        "Administrator turnover footnote",
        "Registered Nurse turnover footnote",
        "Short-Stay QM Rating Footnote"
    ]

    df = df.drop(*columns_to_drop)

    return df


# ============================================================
# 7. Transform Survey Dates
# ============================================================

def transform_survey_dates(df):

    # Replace these with the actual columns
    # that you want to remove

    columns_to_drop = [
        "Average Number of Residents per Day Footnote",
        "Overall Rating Footnote",
        "Health Inspection Rating Footnote",
        "QM Rating Footnote",
        "Reported Staffing Footnote",
        "Physical Therapist Staffing Footnote",
        "Special Focus Status",
        "Long-Stay QM Rating Footnote",
        "Geocoding Footnote",
        "Staffing Rating Footnote",
        "Total nursing staff turnover footnote",
        "Administrator turnover footnote",
        "Registered Nurse turnover footnote",
        "Short-Stay QM Rating Footnote"
    ]

    df = df.drop(*columns_to_drop)

    return df


# ============================================================
# 8. Write Data to Silver
# ============================================================

def write_to_silver(df, file_name):

    output_path = silver_path + file_name.replace(".csv", "")

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(output_path)
    )


# ============================================================
# 9. Process NH_ProviderInfo_Oct2024.csv
# ============================================================

provider_df = read_csv(provider_file)

provider_df = transform_provider_info(provider_df)

write_to_silver(
    provider_df,
    provider_file
)


# ============================================================
# 10. Process NH_StateUSAverages_Oct2024.csv
# ============================================================

state_average_df = read_csv(state_average_file)

state_average_df = transform_state_averages(state_average_df)

write_to_silver(
    state_average_df,
    state_average_file
)


# ============================================================
# 11. Process NH_SurveyDates_Oct2024.csv
# ============================================================

survey_dates_df = read_csv(survey_dates_file)

survey_dates_df = transform_survey_dates(survey_dates_df)

write_to_silver(
    survey_dates_df,
    survey_dates_file
)


# ============================================================
# 12. Process PBJ File
# ============================================================

pbj_df = read_csv(pbj_file)

# No transformations are required for this file.
# It is simply written from Bronze to Silver.

write_to_silver(
    pbj_df,
    pbj_file
)


# ============================================================
# 13. Commit Job
# ============================================================

job.commit()