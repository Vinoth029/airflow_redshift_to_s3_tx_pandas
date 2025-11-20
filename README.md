# **Redshift → Pandas DataFrame → Transform → Save to S3 (Parquet, CSV, JSON)**

This document provides a full production-grade example of fetching data from a Redshift table into a **Pandas DataFrame**, performing transformations, and writing the transformed results back to **Amazon S3** in **three formats**:

* Parquet
* CSV
* JSON

All steps are implemented using Airflow.

---

# 🚀 **1. What This DAG Does**

### ✔ Fetch data from Redshift as a Pandas DataFrame

Using `RedshiftSQLHook.get_pandas_df()`.

### ✔ Transform DataFrame in the next task

Includes example transformations.

### ✔ Save output to S3 in multiple formats

Uses boto3 to store:

* `orders.parquet`
* `orders.csv`
* `orders.json`

---

# 🧱 **2. Complete Airflow DAG Code**

```python
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO, StringIO

from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator


# ---------------------------------------
# 1. Fetch DF from Redshift
# ---------------------------------------
def fetch_dataframe(**context):
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    df = hook.get_pandas_df("""
        SELECT order_id, customer_id, amount, order_ts
        FROM staging.orders_stg
        LIMIT 10;
    """)

    # convert DF → JSON for XCom
    df_json = df.to_json(orient="records")
    context["ti"].xcom_push(key="orders_raw", value=df_json)

    print("Raw DataFrame:")
    print(df)


# ---------------------------------------
# 2. Transform DF
# ---------------------------------------
def transform_dataframe(**context):
    ti = context["ti"]

    df_json = ti.xcom_pull(task_ids="fetch_dataframe", key="orders_raw")
    df = pd.read_json(df_json)

    # Example transformations
    df["amount_with_tax"] = df["amount"] * 1.10
    df["order_date"] = pd.to_datetime(df["order_ts"]).dt.date

    # send transformed DF to XCom as JSON
    ti.xcom_push(key="orders_transformed", value=df.to_json(orient="records"))

    print("Transformed DataFrame:")
    print(df)


# ---------------------------------------
# 3. Save Files to S3
# ---------------------------------------
def write_to_s3(**context):
    ti = context["ti"]
    df_json = ti.xcom_pull(task_ids="transform_dataframe", key="orders_transformed")
    df = pd.read_json(df_json)

    bucket = "my-output-bucket"  # replace with your bucket
    prefix = "analytics/orders_output"  # output folder

    s3 = boto3.client("s3")

    # -------------- PARQUET --------------
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/orders.parquet",
        Body=parquet_buffer.getvalue(),
    )

    # -------------- CSV --------------
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/orders.csv",
        Body=csv_buffer.getvalue(),
    )

    # -------------- JSON --------------
    json_buffer = df.to_json(orient="records")
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/orders.json",
        Body=json_buffer,
    )

    print("Successfully written Parquet, CSV & JSON to S3!")


# ---------------------------------------
# DAG Definition
# ---------------------------------------
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="redshift_df_transform_write_s3",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["redshift", "dataframe", "s3", "pandas"],
) as dag:

    fetch_dataframe = PythonOperator(
        task_id="fetch_dataframe",
        python_callable=fetch_dataframe,
        provide
```
