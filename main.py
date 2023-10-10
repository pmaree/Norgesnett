import os
from datetime import datetime

from lib.measurements import fetch_raw, raw_to_bronze, bronze_to_silver
from lib.analysis import statistics
from lib import Logging

log = Logging()

PATH = os.path.dirname(__file__)


def run_fetch_raw():
    from_date ='2023-03-01T00:00:00'
    to_date = '2023-08-20T00:00:00'
    usagepoints_file = "2023-10-03T14:53:40.381116"

    log.info(f"[{datetime.now().isoformat()}] Retrieve AMI data for usagepoints file {usagepoints_file} in range {from_date} to {to_date}")

    fetch_raw(src_path=PATH + f"/data/bronze/usagepoints/{usagepoints_file}",
              dst_path=PATH + f"/data/raw/measurements/",
              from_date=from_date,
              to_date=to_date)


def run_raw_to_bronze():
    raw_to_bronze(src_path=PATH + f"/data/raw/measurements/", dst_path=PATH + f"/data/bronze/measurements/")


def run_bronze_to_silver():
    bronze_to_silver(src_path=PATH + f"/data/bronze/measurements/",
                     dst_path=PATH + f"/data/silver/",
                     date_from=datetime(2023,3,1), date_to=datetime(2023,8,1))

def run_statistics():
    statistics(src_path=PATH + f"/data/bronze/measurements/",
               dst_path=PATH + f"/data/silver/",
               date_from=datetime(2023, 3, 1), date_to=datetime(2023, 9, 1))




if __name__ == "__main__":
    #run_fetch_raw()
    #run_raw_to_bronze()
    #run_bronze_to_silver()
    run_statistics()


