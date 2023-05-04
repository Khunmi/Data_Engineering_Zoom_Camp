#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

parser = argparse.ArgumentParser()

parser.add_argument('--input_data', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_data = args.input_data
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

initial_schema = types.StructType([
    types.StructField("PatientId", types.StringType(), True),
    types.StructField("AppointmentID", types.StringType(), True),
    types.StructField("Gender", types.StringType(), True),
    types.StructField("ScheduledDay", types.StringType(), True),
    types.StructField("AppointmentDay", types.StringType(), True),  
    types.StructField("Age", types.IntegerType(), True),
    types.StructField("Neighbourhood", types.StringType(), True),
    types.StructField("Scholarship", types.IntegerType(), True),
    types.StructField("Hipertension", types.IntegerType(), True),
    types.StructField("Diabetes", types.IntegerType(), True),
    types.StructField("Alcoholism", types.IntegerType(), True),
    types.StructField("Handcap", types.IntegerType(), True),
    types.StructField("SMS_received", types.IntegerType(), True),
    types.StructField("No-show", types.StringType(), True) 
])

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-northamerica-northeast2-694271669995-sguk3pmh')

df_appointment = spark.read \
        .option("header", "true") \
        .schema(initial_schema) \
        .csv(input_data)

df_appointment = df_appointment \
    .withColumnRenamed('Hipertension', 'Hypertension') \
    .withColumnRenamed('Handcap', 'Handicap') \
    .withColumnRenamed('No-show', 'No_show')

df_appointment.registerTempTable('raw_temp')

df_result = spark.sql("""
SELECT 
    -- date column cleaning
    LEFT(ScheduledDay,10) AS ScheduledDay,
    LEFT(AppointmentDay,10) AS AppointmentDay,

    -- Other Columns 
    PatientId,
    AppointmentID,
    Gender,
    Age,
    Neighbourhood,
    Scholarship,
    Hypertension,
    Diabetes,
    Alcoholism,
    SMS_received,
    No_show
    
FROM
    raw_temp
    --filtering age column
WHERE Age > 0
""")

#writing data into cloud storage(bucket)
#df_result.coalesce(1).write.parquet(output, mode='overwrite')

#writing parquet directly into bigquery
df_result.write.format('bigquery') \
    .option('table', output) \
    .save()



#uploads data to GCS via CLI
##gsutil -m cp -r capstone/ gs://dtc_data_lake_khunmi-academy-376002/capstone

