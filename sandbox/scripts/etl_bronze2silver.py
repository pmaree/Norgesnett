from lib.etl import etl_bronze_to_silver

import os
PATH = os.path.dirname(__file__)

src_path = PATH + '/../../data/bronze/measurements/'
dst_path = PATH + '/../../data/silver/measurements/'

etl_bronze_to_silver(src_path=src_path, dst_path=dst_path)