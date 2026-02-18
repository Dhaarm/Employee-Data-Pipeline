from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
import sys

# ------------------ Spark Session Initialization ------------------
try:
    spark = SparkSession.builder.appName("EmployeePipeline").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Reduce Spark logs to errors only
except Exception as e:
    print(f"Failed to initialize SparkSession: {e}")
    sys.exit(1)

# ------------------ Logging Setup ------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EmployeePipeline")

def is_blank(column):
    """Return True if a column is NULL or contains only whitespace"""
    return col(column).isNull() | (trim(col(column)) == "")

try:
    # ------------------ Read Raw CSV Data ------------------
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv("/opt/spark/data/employees_raw.csv")

    df = df.withColumn("employee_id", col("employee_id").cast("int"))

    total_records = df.count()
    logger.info(f"Total records read: {total_records}")

except Exception as e:
    logger.error(f"Error reading CSV file: {e}")
    spark.stop()
    sys.exit(1)

try:
    # ------------------ Data Quality Checks ------------------
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    window = Window.partitionBy("employee_id")

    df = df \
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd")) \
        .withColumn("duplicate_flag", count("*").over(window) > 1) \
        .withColumn("department",
            when(col("department").isNull() | (trim(col("department")) == ""), "N/A")
            .otherwise(upper(col("department")))
        ) \
        .withColumn("address",
            when(col("address").isNull() | (trim(col("address")) == ""), "N/A")
            .otherwise(col("address"))
        ) \
        .withColumn("city",
            when(col("city").isNull() | (trim(col("city")) == ""), "N/A")
            .otherwise(upper(col("city")))
        ) \
        .withColumn("invalid_email_flag",
            when(col("email").isNull(), True)
            .otherwise(~lower(col("email")).rlike(email_regex))
        ) \
        .withColumn("invalid_manager_id",
            when(col("manager_id").isNull() | (trim(col("manager_id")) == ""), True)
            .otherwise(False)
        ) \
        .withColumn("invalid_salary",
            when(col("salary").isNull() | (trim(col("salary")) == ""), True)
            .otherwise(False)
        ) \
        .withColumn("future_hire_flag",
            when(col("hire_date").isNull(), True)
            .otherwise(col("hire_date") > current_date())
        ) \
        .withColumn("salary", when(col("salary") == "", None).otherwise(col("salary"))) \
        .withColumn("salary", regexp_replace(col("salary"), "[$,]", "")) \
        .withColumn("null_flag",
            is_blank("employee_id") |
            is_blank("first_name") |
            is_blank("last_name") |
            is_blank("email") |
            is_blank("invalid_manager_id") |
            is_blank("invalid_salary") |
            is_blank("hire_date")
        ) \
        .withColumn("rejection_reason",
            concat_ws(", ",
                when(col("duplicate_flag"), "DUPLICATE_EMPLOYEE_ID"),
                when(col("invalid_email_flag"), "INVALID_EMAIL"),
                when(col("invalid_manager_id"), "INVALID_MANAGER_ID"),
                when(col("invalid_salary"), "INVALID_SALARY"),
                when(col("future_hire_flag"), "FUTURE_HIRE_DATE"),
                when(col("null_flag"), "NULL_CRITICAL_FIELD")
            )
        )

except Exception as e:
    logger.error(f"Error during data quality checks: {e}")
    spark.stop()
    sys.exit(1)

try:
    # ------------------ Separate Rejected and Valid Records ------------------
    rejected_df = df.filter(col("rejection_reason") != "") \
        .withColumn("employee_id", col("employee_id").cast("int")) \
        .withColumn("manager_id", col("manager_id").cast("int")) \
        .withColumn("salary", regexp_replace(col("salary"), "[$,]", "")) \
        .withColumn("salary", col("salary").cast("double"))

    valid_df = df.filter(col("rejection_reason") == "") \
        .withColumn("employee_id", col("employee_id").cast("int")) \
        .withColumn("salary", col("salary").cast("double")) \
        .withColumn("manager_id", col("manager_id").cast("int"))

    logger.info(f"Rejected records: {rejected_df.count()}")
    logger.info(f"Valid records: {valid_df.count()}")

except Exception as e:
    logger.error(f"Error separating rejected and valid records: {e}")
    spark.stop()
    sys.exit(1)

try:
    # ------------------ Write Rejected Records to PostgreSQL ------------------
    rejected_df.select(
        "employee_id","first_name","last_name","email","hire_date","job_title",
        "department","salary","manager_id","address","city","state",
        "zip_code","birth_date","status","rejection_reason"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/employees") \
        .option("dbtable", "employees_rejected") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

except Exception as e:
    logger.error(f"Error writing rejected records to PostgreSQL: {e}")
    spark.stop()
    sys.exit(1)

try:
    # ------------------ Transform Valid Records ------------------
    valid_df = valid_df \
        .withColumn("first_name", initcap("first_name")) \
        .withColumn("last_name", initcap("last_name")) \
        .withColumn("email", lower("email")) \
        .withColumn("salary", regexp_replace("salary", "[$,]", "").cast("double")) \
        .withColumn("age", floor(datediff(current_date(), "birth_date") / 365)) \
        .withColumn("tenure_years", round(datediff(current_date(), "hire_date") / 365, 1)) \
        .withColumn("salary_band",
            when(col("salary") < 50000, "Junior")
            .when(col("salary").between(50000, 80000), "Mid")
            .otherwise("Senior")
        ) \
        .withColumn("full_name", concat_ws(" ", "first_name", "last_name")) \
        .withColumn("email_domain", split("email", "@").getItem(1))

    # ------------------ Load Clean Data to PostgreSQL ------------------
    valid_df.select(
        "employee_id","first_name","last_name","full_name",
        "email","email_domain","hire_date","job_title",
        "department","salary","salary_band","manager_id",
        "address","city","state","zip_code","birth_date",
        "age","tenure_years","status"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/employees") \
        .option("dbtable", "employees_clean") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logger.info("Pipeline Completed Successfully")

except Exception as e:
    logger.error(f"Error transforming or writing valid records: {e}")
    spark.stop()
    sys.exit(1)

finally:
    # ------------------ Stop Spark Session ------------------
    spark.stop()
