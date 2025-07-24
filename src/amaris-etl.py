import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime


# ENV VARIABLES
db_name = "amaris-db"
table_suppliers = "suppliers"
table_clients = "clients"
table_transactions = "transactions"
target_bucket = "alejandrogs-datalake"

today_date = datetime.now()

current_year = format(today_date.year, '02d' )
current_month = format(today_date.month, '02d' )
current_day = format(today_date.day, '02d' )

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# READ CSVs FILES

suppliers_df = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, 
    table_name=table_suppliers,
    transformation_ctx="datasource_suppliers",
    format_options={
        "withHeader": True,
    }
).toDF()

clients_df = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, 
    table_name=table_clients,
    transformation_ctx="datasource_clients"
).toDF()

transactions_df = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, 
    table_name=table_transactions,
    transformation_ctx="datasource_transactions"
).toDF()

# TRANSFORMATIONS

clients_df = clients_df.withColumn("identification", col("identification").cast(StringType()))
transactions_df = transactions_df.withColumn("amount", col("amount").cast(DoubleType())) \
                .withColumn("price_per_unit", col("price_per_unit").cast(DoubleType()))
suppliers_df = suppliers_df.withColumnRenamed("energy_type", "main_industry_energy_type")


transactions_df = transactions_df.withColumn("client_name", lower(transactions_df["client_name"]))

transactions_df = transactions_df.dropDuplicates(["id"])

transactions_df = transactions_df.withColumn("total_price", ( col("price_per_unit") * col("amount") ))

industrial_df = transactions_df.filter(col("transaction_type") == "venta") \
            .join(suppliers_df, transactions_df.supplier_id == suppliers_df.id, "full") \
            .drop("client_id", "supplier_name") \
            .drop(suppliers_df.id)
            

residential_df = transactions_df.filter(col("transaction_type") == "compra") \
            .join(clients_df, transactions_df.client_id == clients_df.identification, "full") \
            .drop("identification", "name") 
            

industrial_dyf = DynamicFrame.fromDF(industrial_df, glueContext, "industrial_df")
residential_dyf = DynamicFrame.fromDF(residential_df, glueContext, "residential_df")

# WRITE PARQUET FILE
# TODO: SHOULD USE STRUCTURE: .../parquets/year=2025/month=07/day=23/
glueContext.write_dynamic_frame.from_options(
    frame = industrial_dyf,
    connection_type = "s3",
    connection_options = {"path": f"s3://{target_bucket}/energy_sales/parquets/industrial/{current_year}/{current_month}/{current_day}/"}, 
    format = "parquet",
    transformation_ctx = "write_parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame = residential_dyf,
    connection_type = "s3",
    connection_options = {"path": f"s3://{target_bucket}/energy_sales/parquets/residential/{current_year}/{current_month}/{current_day}/"}, 
    format = "parquet",
    transformation_ctx = "write_parquet"
)

job.commit()
