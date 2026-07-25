import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ======================================================
# Initialize Glue Job
# ======================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# ======================================================
# Read Silver Data
# ======================================================

pbj = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3://healthcare-target-data-bucket/silver/PBJ_Daily_Nurse_Staffing_Q2_2024/"
    )
)


provider = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3://healthcare-target-data-bucket/silver/NH_ProviderInfo_Oct2024/"
    )
)


state_avg = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3://healthcare-target-data-bucket/silver/NH_StateUSAverages_Oct2024/"
    )
)


survey = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3://healthcare-target-data-bucket/silver/NH_SurveyDates_Oct2024/"
    )
)


# ======================================================
# Prepare PBJ Dataset
# ======================================================

pbj = (

    pbj

    # --------------------------------------------------
    # Convert date
    # --------------------------------------------------

    .withColumn(
        "WorkDate",
        F.to_date(
            col("WorkDate")
        )
    )

    # --------------------------------------------------
    # Convert hours to numeric
    # --------------------------------------------------

    .withColumn(
        "Hrs_RN",
        col("Hrs_RN").cast("double")
    )

    .withColumn(
        "Hrs_LPN",
        col("Hrs_LPN").cast("double")
    )

    .withColumn(
        "Hrs_CNA",
        col("Hrs_CNA").cast("double")
    )

    .withColumn(
        "Hrs_RN_emp",
        col("Hrs_RN_emp").cast("double")
    )

    .withColumn(
        "Hrs_LPN_emp",
        col("Hrs_LPN_emp").cast("double")
    )

    .withColumn(
        "Hrs_CNA_emp",
        col("Hrs_CNA_emp").cast("double")
    )

    .withColumn(
        "Hrs_RN_ctr",
        col("Hrs_RN_ctr").cast("double")
    )

    .withColumn(
        "Hrs_LPN_ctr",
        col("Hrs_LPN_ctr").cast("double")
    )

    .withColumn(
        "Hrs_CNA_ctr",
        col("Hrs_CNA_ctr").cast("double")
    )

    .withColumn(
        "MDScensus",
        col("MDScensus").cast("double")
    )

    # --------------------------------------------------
    # Create Year and Month
    # --------------------------------------------------

    .withColumn(
        "YEAR",
        F.year(
            col("WorkDate")
        )
    )

    .withColumn(
        "MONTH",
        F.month(
            col("WorkDate")
        )
    )

    # --------------------------------------------------
    # Calculate Total Nurse Hours
    # --------------------------------------------------

    .withColumn(
        "TOTAL_NURSE_HOURS",

        F.coalesce(
            col("Hrs_RN"),
            F.lit(0)
        )

        +

        F.coalesce(
            col("Hrs_LPN"),
            F.lit(0)
        )

        +

        F.coalesce(
            col("Hrs_CNA"),
            F.lit(0)
        )

    )

)
# ======================================================
# 1. STAFFING METRICS
# ======================================================

staffing_metrics = (

    pbj

    .groupBy(

        "PROVNUM",
        "PROVNAME",
        "STATE",
        "YEAR",
        "MONTH"

    )

    .agg(

        # ----------------------------------------------
        # Total Nurse Hours
        # ----------------------------------------------

        F.sum(
            "TOTAL_NURSE_HOURS"
        )
        .alias(
            "TOTAL_NURSE_HOURS"
        ),

        # ----------------------------------------------
        # Average Patient Census
        # ----------------------------------------------

        F.avg(
            "MDScensus"
        )
        .alias(
            "AVG_PATIENT_CENSUS"
        ),

        # ----------------------------------------------
        # RN Hours
        # ----------------------------------------------

        F.sum(
            "Hrs_RN"
        )
        .alias(
            "RN_HOURS"
        ),

        # ----------------------------------------------
        # LPN Hours
        # ----------------------------------------------

        F.sum(
            "Hrs_LPN"
        )
        .alias(
            "LPN_HOURS"
        ),

        # ----------------------------------------------
        # CNA Hours
        # ----------------------------------------------

        F.sum(
            "Hrs_CNA"
        )
        .alias(
            "CNA_HOURS"
        ),

        # ----------------------------------------------
        # Employee Hours
        # ----------------------------------------------

        F.sum(

            F.coalesce(
                col("Hrs_RN_emp"),
                F.lit(0)
            )

            +

            F.coalesce(
                col("Hrs_LPN_emp"),
                F.lit(0)
            )

            +

            F.coalesce(
                col("Hrs_CNA_emp"),
                F.lit(0)
            )

        )
        .alias(
            "EMPLOYEE_HOURS"
        ),

        # ----------------------------------------------
        # Contract Hours
        # ----------------------------------------------

        F.sum(

            F.coalesce(
                col("Hrs_RN_ctr"),
                F.lit(0)
            )

            +

            F.coalesce(
                col("Hrs_LPN_ctr"),
                F.lit(0)
            )

            +

            F.coalesce(
                col("Hrs_CNA_ctr"),
                F.lit(0)
            )

        )
        .alias(
            "CONTRACT_HOURS"
        )

    )

)


# ======================================================
# Calculate Staffing Ratios
# ======================================================

staffing_metrics = (

    staffing_metrics

    # --------------------------------------------------
    # Nurse-to-Patient Ratio
    # --------------------------------------------------

    .withColumn(

        "NURSE_TO_PATIENT_RATIO",

        F.round(

            when(

                col("AVG_PATIENT_CENSUS") > 0,

                col("TOTAL_NURSE_HOURS")
                /
                col("AVG_PATIENT_CENSUS")

            )
            .otherwise(0),

            2

        )

    )

    # --------------------------------------------------
    # Employee Staff Percentage
    # --------------------------------------------------

    .withColumn(

        "EMPLOYEE_STAFF_PERCENT",

        F.round(

            when(

                col("TOTAL_NURSE_HOURS") > 0,

                (

                    col("EMPLOYEE_HOURS")
                    /
                    col("TOTAL_NURSE_HOURS")

                )
                *
                100

            )
            .otherwise(0),

            2

        )

    )

    # --------------------------------------------------
    # Contract Staff Percentage
    # --------------------------------------------------

    .withColumn(

        "CONTRACT_STAFF_PERCENT",

        F.round(

            when(

                col("TOTAL_NURSE_HOURS") > 0,

                (

                    col("CONTRACT_HOURS")
                    /
                    col("TOTAL_NURSE_HOURS")

                )
                *
                100

            )
            .otherwise(0),

            2

        )

    )

)


# ======================================================
# Write Gold Staffing Metrics
# ======================================================

staffing_metrics.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .partitionBy(
        "STATE",
        "YEAR",
        "MONTH"
    ) \
    .csv(
        "s3://healthcare-target-data-bucket/gold/staffing_metrics/"
    )


# ======================================================
# Prepare Provider Dataset
# ======================================================

provider_df = (

    provider

    .select(

        col(
            "CMS Certification Number (CCN)"
        )
        .alias(
            "PROVNUM"
        ),

        col(
            "Provider Name"
        )
        .alias(
            "PROVNAME"
        ),

        col(
            "State"
        )
        .alias(
            "STATE"
        ),

        col(
            "Number of Certified Beds"
        )
        .cast("double")
        .alias(
            "CERTIFIED_BEDS"
        ),

        col(
            "Average Number of Residents per Day"
        )
        .cast("double")
        .alias(
            "AVG_RESIDENTS"
        ),

        col(
            "Overall Rating"
        )
        .cast("double")
        .alias(
            "OVERALL_RATING"
        ),

        col(
            "Health Inspection Rating"
        )
        .cast("double")
        .alias(
            "HEALTH_RATING"
        ),

        col(
            "Staffing Rating"
        )
        .cast("double")
        .alias(
            "STAFFING_RATING"
        )

    )

    # Remove duplicate facilities

    .dropDuplicates(
        ["PROVNUM"]
    )

)


# ======================================================
# Calculate Facility Metrics
# ======================================================

facility_metrics = (

    provider_df

    # --------------------------------------------------
    # Occupancy Rate
    # --------------------------------------------------

    .withColumn(

        "OCCUPANCY_RATE",

        F.round(

            when(

                col("CERTIFIED_BEDS") > 0,

                (

                    col("AVG_RESIDENTS")
                    /
                    col("CERTIFIED_BEDS")

                )
                *
                100

            )
            .otherwise(0),

            2

        )

    )

    # --------------------------------------------------
    # Join Staffing Metrics
    # --------------------------------------------------

    .join(

        staffing_metrics

        .groupBy(
            "PROVNUM"
        )

        .agg(

            F.avg(
                "NURSE_TO_PATIENT_RATIO"
            )
            .alias(
                "AVG_STAFFING_RATIO"
            )

        ),

        "PROVNUM",

        "left"

    )

    # --------------------------------------------------
    # Staffing Pressure Index
    # --------------------------------------------------

    .withColumn(

        "STAFFING_PRESSURE_INDEX",

        F.round(

            when(

                col("AVG_STAFFING_RATIO") > 0,

                col("AVG_RESIDENTS")
                /
                col("AVG_STAFFING_RATIO")

            )
            .otherwise(0),

            2

        )

    )

)


# ======================================================
# Write Gold Facility Metrics
# ======================================================

facility_metrics.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .partitionBy(
        "STATE"
    ) \
    .csv(
        "s3://healthcare-target-data-bucket/gold/facility_metrics/"
    )
# ======================================================
# 2. TOP 10 PATIENT VOLUME FACILITIES
# ======================================================

patient_volume = (

    pbj

    .groupBy(

        "PROVNUM",
        "PROVNAME",
        "STATE"

    )

    .agg(

        F.sum(
            "MDScensus"
        )

        .alias(
            "TOTAL_PATIENT_DAYS"
        )

    )

    .orderBy(

        F.desc(
            "TOTAL_PATIENT_DAYS"
        )

    )

    .limit(
        10
    )

)


# ======================================================
# Write Top Patient Volume Metrics
# ======================================================

patient_volume.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .csv(
        "s3://healthcare-target-data-bucket/gold/top_patient_volume/"
    )


# ======================================================
# 3. QUALITY METRICS
# ======================================================

quality_metrics = (

    provider_df

    .select(

        "PROVNUM",
        "PROVNAME",
        "STATE",

        "OVERALL_RATING",
        "HEALTH_RATING",
        "STAFFING_RATING"

    )

    # --------------------------------------------------
    # Facility Quality Score
    # Average of Available Ratings
    # --------------------------------------------------

    .withColumn(

        "FACILITY_QUALITY_SCORE",

        F.round(

            (

                F.coalesce(
                    col("OVERALL_RATING"),
                    F.lit(0)
                )

                +

                F.coalesce(
                    col("HEALTH_RATING"),
                    F.lit(0)
                )

                +

                F.coalesce(
                    col("STAFFING_RATING"),
                    F.lit(0)
                )

            )

            /

            (

                (col("OVERALL_RATING").isNotNull())
                .cast("int")

                +

                (col("HEALTH_RATING").isNotNull())
                .cast("int")

                +

                (col("STAFFING_RATING").isNotNull())
                .cast("int")

            ),

            2

        )

    )

)


# ======================================================
# Write Gold Quality Metrics
# ======================================================

quality_metrics.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .partitionBy(
        "STATE"
    ) \
    .csv(
        "s3://healthcare-target-data-bucket/gold/quality_metrics/"
    )


# ======================================================
# Finish Glue Job
# ======================================================

job.commit()