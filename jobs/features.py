"""Schema and feature engineering shared by silver->gold, training AND the API (avoids training/serving skew)."""
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)

RAW_SCHEMA = StructType([
    StructField("application_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("event_ts", StringType()),
    StructField("age", IntegerType()),
    StructField("income", DoubleType()),
    StructField("employment_years", IntegerType()),
    StructField("loan_amount", DoubleType()),
    StructField("loan_term_months", IntegerType()),
    StructField("interest_rate", DoubleType()),
    StructField("credit_score", IntegerType()),
    StructField("num_open_accounts", IntegerType()),
    StructField("num_late_payments", IntegerType()),
    StructField("debt_to_income", DoubleType()),
    StructField("home_ownership", StringType()),
    StructField("loan_purpose", StringType()),
    StructField("default", IntegerType()),
])

# fields a client must provide to score an application
INPUT_FIELDS = ["age", "income", "employment_years", "loan_amount", "loan_term_months", "interest_rate",
                "credit_score", "num_open_accounts", "num_late_payments", "debt_to_income",
                "home_ownership", "loan_purpose"]
INPUT_SCHEMA = StructType([f for f in RAW_SCHEMA.fields if f.name in INPUT_FIELDS])

CATEGORICAL = ["home_ownership", "loan_purpose"]
NUMERIC_RAW = ["age", "income", "employment_years", "loan_amount", "loan_term_months", "interest_rate",
               "credit_score", "num_open_accounts", "num_late_payments", "debt_to_income"]
NUMERIC_DERIVED = ["payment_to_income", "late_per_account"]
NUMERIC = NUMERIC_RAW + NUMERIC_DERIVED


def add_features(df):
    """Derived features. Expects income > 0 and loan_term_months > 0 (guaranteed by silver validation / API validation)."""
    monthly_payment = F.col("loan_amount") * (1 + F.col("interest_rate") / 100) / F.col("loan_term_months")
    return (df
            .withColumn("payment_to_income", monthly_payment / (F.col("income") / 12))
            .withColumn("late_per_account", F.col("num_late_payments") / (F.col("num_open_accounts") + 1)))
