from datetime import datetime
from lib.etl import etl_raw

import os
PATH = os.path.dirname(__file__)

src_path = PATH + '/../data/bronze/usagepoints/2023-11-23'
dst_path = PATH + '/../data/bronze/measurements/'
from_date = datetime(year=2022, month=9, day=1)
to_date = datetime(year=2023, month=5, day=1)

etl_raw(src_path=src_path, dst_path=dst_path, from_date=from_date, to_date=to_date)