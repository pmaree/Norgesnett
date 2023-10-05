from datetime import datetime
import polars as pl
import os, shutil

from lib import Logging

log = Logging()

from lib.api import fetch_bulk, Query


class ProcessingRegister:
    def __init__(self, root_path: str, df: pl.DataFrame):
        self.root = root_path
        self.path = os.path.join(root_path, 'registry')
        if os.path.isfile(self.path) is False:
            df = df.groupby('topology').agg(pl.col('ami_id').alias('ami_ids')).with_columns(pl.col('ami_ids').apply(lambda x: len(x)).alias('ami_id_cnt')).sort(by='ami_id_cnt', descending=True).with_columns( processed = False)
            df.write_parquet(self.path)
        self.prepare()

    def entry(self, topology_name: str):
        topology_path = os.path.join(self.root, topology_name)
        os.mkdir(topology_path)
        return topology_path, topology_name

    def read(self) ->pl.DataFrame:
        return pl.read_parquet(self.path)

    def update(self, topology: str, processed: bool = True):
        df = self.read()
        df = df.with_columns(processed=pl.when(pl.col('topology')==topology).then(processed).otherwise(pl.col("processed")).alias('processed'))
        df.write_parquet(self.path)

    def prepare(self):
        df = self.read()
        for row in df.rows(named=True):
            topology_path = os.path.join(self.root, row['topology'])
            if os.path.exists(topology_path) and (row['processed'] is False):
                shutil.rmtree(topology_path)


# fetch raw AMI measurement for those AMI's associated with a topology
def fetch_measurements(src_path: str, dst_path: str, from_date: datetime, to_date: datetime):

    # keep tracked of fetched data
    registry = ProcessingRegister(root_path=dst_path, df=pl.read_parquet(src_path))

    for row in registry.read().rows(named=True):

        processed = row['processed']
        ami_ids = row['ami_ids']
        ami_id_cnt = row['ami_id_cnt']

        if processed is False:

            topology_path, topology_name = registry.entry(topology_name=row['topology'])
            ami_ids = row['ami_ids']

            log.info(f"[{datetime.utcnow()}] Topology {topology_name} selected for historical measurement retrieval with {ami_id_cnt} AMI associations.")

            for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_ids, from_date=from_date, to_date=to_date, resolution=1, type=1)):
                log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}>")
                query.df.write_parquet(os.path.join(topology_path, query.name))

            for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_ids, from_date=from_date, to_date=to_date, resolution=1, type=3)):
                log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}>")
                query.df.write_parquet(os.path.join(topology_path, query.name))

            log.info(f"[{datetime.utcnow()}] Topology {topology_name} completed historical measurement retrieval with {ami_id_cnt} AMI associations.")

            registry.update(topology=topology_name, processed=True)


# process the raw measurements
def process_measuremets(src_path: str, dst_path: str):
    pass

