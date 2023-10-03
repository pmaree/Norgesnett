from datetime import datetime
import polars as pl
import pandas as pd
import os, shutil

from lib.api import fetch_bulk, Query, QueryRes

# fetch raw AMI measurement for those AMI's associated with a topology
def fetch_measurements(src_path: str, dst_path: str, from_date: datetime, to_date: datetime):
    df = pl.read_parquet(src_path)

    for topology in df.partition_by(by='topology', maintain_order=True, as_dict=True).items():
        topology_path = os.path.join(dst_path, topology[0])
        if os.path.exists(topology_path):
            shutil.rmtree(topology_path)
        os.mkdir(topology_path)

        ami_id = pd.DataFrame(topology[1], columns=['topology','ami_id'])['ami_id'].values

        for query in fetch_bulk(Query(ami_id=ami_id, from_date=from_date, to_date=to_date, resolution=1, type=1)):
            print(f"[{topology[0]}] Write parquet for batch {query.name}")
            query.df.write_parquet(os.path.join(topology_path, query.name))
        for query in fetch_bulk(Query(ami_id=ami_id, from_date=from_date, to_date=to_date, resolution=1, type=3)):
            print(f"[{topology[0]}] Write parquet for batch {query.name}")
            query.df.write_parquet(os.path.join(topology_path, query.name))


# process the raw measurements
def process_measuremets(src_path: str, dst_path: str):
    pass

