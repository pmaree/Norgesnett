from lib.etl import etl_raw_to_bronze

import os
PATH = os.path.dirname(__file__)

src_path = PATH + '/../../data/raw/measurements/'
dst_path = PATH + '/../../data/bronze/measurements/'

etl_raw_to_bronze(src_path=src_path, dst_path=dst_path)