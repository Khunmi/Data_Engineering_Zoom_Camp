## Capstone Project: Batch Processing

## Objective 
To satify an end to end data engineering life cycle.

### Files and Tech Stack used
GCS - google cloud storage for data storing
capstone_proj.py - This script reads csv data from GCS and transforms it using Spark. Finally it writes the output dataset to a table called "capstone2023" as a Parquet file in Bigquery
Pysaprk - Triggered Spark jobs within Google cloud storage using the following code snippet via the command line interface 
''' gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=northamerica-northeast2 \
    --project=khunmi-academy-376002 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_khunmi-academy-376002/code/capstone_proj.py \
    -- \
        --input_data=gs://dtc_data_lake_khunmi-academy-376002/capstone/ \
        --output=trips_data_all.capstone2023'''

Google Data Studio: To generate insight from my dataset, I connected my data source located in my data warehouse(Bigquery).

### Dataset

This dataset collects information from 100k medical appointments in Brazil and is focused on the
question of whether or not patients show up for their appointment. A number of characteristics
about the patient are included in each row.‘ScheduledDay’ tells us on what day the patient setup their appointment.‘Neighborhood’ indicates the location of the hospital.‘Scholarship’ indicates whether or not the patient is enrolled in Brasilian welfare program Bolsa Família:

        Variable                Data_ Type
    1. PatientId                   float64
    2. Appointment                 IDint64
    3. Gender                       object
    4. ScheduledDay                 object
    5. AppointmentDay               object
    6. Age                           int64
    7. Neighbourhood                object
    8. Scholarship                   int64
    9. Hipertension                  int64
    10. Diabetes                     int64
    11. Alcoholism                   int64
    12. Handcap                      int64
    13. SMS_received                 int64
    14. No-show                     object



## Other files in this repo details on interacting with ETL processes and getting comfortable with orchestrating, Ifrastructure as code and many others.