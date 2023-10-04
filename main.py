import os
from datetime import datetime

from lib.measurements import fetch_measurements
from lib import Logging

log = Logging()

PATH = os.path.dirname(__file__)

if __name__ == "__main__":

    from_date ='2023-03-01T00:00:00'
    to_date = '2023-10-01T00:00:00'
    usagepoints_file = "2023-10-03T14:53:40.381116"

    log.info(f"[{datetime.now().isoformat()}] Retrieve AMI data for usagepoints file {usagepoints_file} in range {from_date} to {to_date}")

    fetch_measurements(src_path=PATH + f"/data/bronze/usagepoints/{usagepoints_file}",
                       dst_path=PATH + f"/data/raw/measurements/",
                       from_date=from_date,
                       to_date=to_date)
