from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os
import requests


@task(retries=3)
def fetch(dataset_url: str, months) -> Path:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    

    for item in months:
        url = dataset_url.format(month=item, year=2019)
        response = requests.get(url)

        if response.status_code == 200:
            file_path = os.path.join("data/",f"fhv_tripdata_2019-{item}.csv.gz")
            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"{item} downloaded successfully!")
        else:
            print(f"Failed to download {item}.")

        
    return (file_path)

@task()
def write_gcs(paths: list[Path]) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    for path in paths:
        if len(str(paths.index(path)+1)) > 1:
            gcs_path = f"data/fhv_tripdata_2019-{paths.index(path)+1}.csv.gz"
            gcs_block.upload_from_path(from_path=path, to_path=gcs_path)
        else:
           gcs_path = f"data/fhv_tripdata_2019-0{paths.index(path)+1}.csv.gz"
           gcs_block.upload_from_path(from_path=path, to_path=gcs_path) 
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    #color = "yellow"
    #year = 2019
    month_1 = ['01','02','03','04','05','06','07','08','09','10','11','12']
    dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
   
    file_paths = [fetch(dataset_url, [month]) for month in month_1]
    write_gcs(file_paths)


if __name__ == "__main__":
    
    etl_web_to_gcs()
