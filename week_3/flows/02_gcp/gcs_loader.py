from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os
import requests
import io
import pyarrow.parquet as pq
import pyarrow as pa
import gzip


@task(retries=3)
def fetch(dataset_url: str, months) -> Path:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    

    for item in months:
        url = dataset_url.format(month=item, year=2019, color = 'yellow')
        response = requests.get(url, stream = True)
        

        if response.status_code == 200:
            file_path = os.path.join("data/yellow",f"yellow_tripdata_2019-{item}.csv.gz")
                    # Set the input and output file paths
            input_file_path = os.path.join(file_path)
            output_file_path = os.path.join(file_path.replace('.csv.gz', '.parquet'))
            with open(input_file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)

            with gzip.open(input_file_path, "rb") as file:
                df = pd.read_csv(file)

                df["PULocationID"] = df.PULocationID.astype("Int64")
                df["DOLocationID"] = df.DOLocationID.astype("Int64")
                df["passenger_count"]= df.passenger_count.astype("Int64")
                df["payment_type"]=df.payment_type.astype("float")
                df["RatecodeID"]=df.RatecodeID.astype("float")
                #df["trip_type"]= df.trip_type.astype("float")
                df["VendorID"]= df.VendorID.astype("Int64")
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_file_path)

            os.remove(input_file_path)

            print(f"{item} downloaded and replaced successfully!")
            
        else:
            print(f"Failed to download {item}.")
            
    return (output_file_path)

@task()
def write_gcs(paths: list[Path]) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    for path in paths:
        if len(str(paths.index(path)+1)) > 1:
            gcs_path = f"yellow_trips/yellow_tripdata_2019-{paths.index(path)+1}.parquet"
            gcs_block.upload_from_path(from_path=path, to_path=gcs_path)
        else:
           gcs_path = f"yellow_trips/yellow_tripdata_2019-0{paths.index(path)+1}.parquet"
           gcs_block.upload_from_path(from_path=path, to_path=gcs_path) 
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    #color = "yellow"
    #year = 2019
    month_1 = ['01','02','03','04','05','06','07','08','09','10','11','12']
    dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{color}_tripdata_{year}-{month}.csv.gz"
   
    file_paths = [fetch(dataset_url, [month]) for month in month_1]
    write_gcs(file_paths)


if __name__ == "__main__":
    
    etl_web_to_gcs()
