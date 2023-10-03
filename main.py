import os

from lib.measurements import fetch_measurements

PATH = os.path.dirname(__file__)


if __name__ == "__main__":

    fetch_measurements(src_path=PATH + f"/data/bronze/usagepoints/2023-10-03T14:53:40.381116",
                       dst_path=PATH + f"/data/raw/measurements/",
                       from_date='2023-03-01T00:00:00',
                       to_date='2023-10-01T00:00:00')
