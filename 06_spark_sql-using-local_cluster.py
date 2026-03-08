#!/usr/bin/env python
# coding: utf-8
#  INFO: https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--green_taxi_data_path', required=True)
parser.add_argument('--yellow_taxi_data_path', required=True)
parser.add_argument('--output_path', required=True)

# Ensure no white spaces in the arguments, otherwise parsing will fail.
# Sample command:
#  uv run python 06_spark_sql-using-local_cluster.py \
#  --green_taxi_data_path=data/pq/green/2020/*/ \
#  --yellow_taxi_data_path=data/pq/yellow/2020/*/ \
#  --output_path=data/report-2020

args = parser.parse_args()

green_taxi_data_path = args.green_taxi_data_path
yellow_taxi_data_path = args.yellow_taxi_data_path
output_path = args.output_path

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df_green = spark.read.option("recursiveFileLookup", "true").parquet(green_taxi_data_path)

df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(yellow_taxi_data_path)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_columns = ['VendorID',
 'pickup_datetime',
 'dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge']

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.coalesce(1).write.parquet(output_path, mode='overwrite')







