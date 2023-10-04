from datetime import datetime
import polars as pl
import os, shutil

from lib import Logging

log = Logging()

from lib.api import fetch_bulk, Query, QueryRes

# fetch raw AMI measurement for those AMI's associated with a topology
def fetch_measurements(src_path: str, dst_path: str, from_date: datetime, to_date: datetime):
    df = pl.read_parquet(src_path)

    topology_list = df.unique(subset=['topology']).select(pl.col('topology')).to_numpy().flatten().tolist()

    for topology_name in topology_list:
        topology_path = os.path.join(dst_path, topology_name)
        if os.path.exists(topology_path):
            shutil.rmtree(topology_path)

    for topology_name in topology_list:

        topology_data = df.filter(pl.col('topology') == topology_name)
        topology_path = os.path.join(dst_path, topology_name)
        os.mkdir(topology_path)

        ami_id = topology_data.select(pl.col('ami_id')).to_series()

        for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_id, from_date=from_date, to_date=to_date, resolution=1, type=1)):
            log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}>")
            query.df.write_parquet(os.path.join(topology_path, query.name))

        for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_id, from_date=from_date, to_date=to_date, resolution=1, type=3)):
            log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}>")
            query.df.write_parquet(os.path.join(topology_path, query.name))


# process the raw measurements
def process_measuremets(src_path: str, dst_path: str):
    pass

