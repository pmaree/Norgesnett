import time
from datetime import datetime
import polars as pl
import os, shutil

from lib import Logging

log = Logging()


from lib.api import fetch_bulk, Query


time_format = '%Y-%m-%dT%H:%M:%S'


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

        # some processing stats
        processed = df.filter(pl.col('processed')==True).shape[0]
        unprocessed = df.filter(pl.col('processed')==False).shape[0]
        total = processed + unprocessed
        return processed, unprocessed, total

    def prepare(self):
        df = self.read()
        for row in df.rows(named=True):
            topology_path = os.path.join(self.root, row['topology'])
            if os.path.exists(topology_path) and (row['processed'] is False):
                shutil.rmtree(topology_path)


# fetch raw AMI measurement for those AMI's associated with a topology
def fetch_raw(src_path: str, dst_path: str, from_date: datetime, to_date: datetime):

    while True:
        try:
            # keep tracked of fetched data
            registry = ProcessingRegister(root_path=dst_path, df=pl.read_parquet(src_path))

            for row in registry.read().rows(named=True):

                processed = row['processed']
                ami_ids = row['ami_ids']
                ami_id_cnt = row['ami_id_cnt']

                if processed is False:

                    topology_path, topology_name = registry.entry(topology_name=row['topology'])

                    log.info(f"[{datetime.utcnow()}] Topology {topology_name} selected for historical measurement retrieval with {ami_id_cnt} AMI associations.")

                    for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_ids, from_date=from_date, to_date=to_date, resolution=1, type=1)):
                        log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}> with {query.sample_cnt} samples")
                        query.df.write_parquet(os.path.join(topology_path, query.name))

                    for query in fetch_bulk(Query(topology=topology_name, ami_id=ami_ids, from_date=from_date, to_date=to_date, resolution=1, type=3)):
                        log.info(f"[{datetime.utcnow()}] {topology_name} successful parquet write for batch <{query.name}> with {query.sample_cnt} samples")
                        query.df.write_parquet(os.path.join(topology_path, query.name))

                    processed, _, total = registry.update(topology=topology_name, processed=True)

                    log.info(f"[{datetime.utcnow()}] Topology {topology_name} [{processed}/{total}] completed historical measurement retrieval with {ami_id_cnt} AMI associations.")

                    if processed == total:
                        log.info(f"Processing completed for [{processed}/{total}] topologies. Goodbye.")
                        break

        except Exception as e:
            log.exception(e)
            time.sleep(60*30)


# process the raw measurements
def raw_to_bronze(src_path: str, dst_path: str):
    registry_path = os.path.join(src_path, 'registry')

    # clean up previous processed data
    shutil.rmtree(dst_path)
    os.mkdir(dst_path)

    if os.path.exists(registry_path):
        df = pl.read_parquet(registry_path)
        df = df.filter(pl.col('processed') == True)
        if df.shape[0]:

            df_processed = pl.DataFrame()
            for index, row in enumerate(df.iter_rows(named=True)):
                topology_name = row['topology']
                file_list = os.listdir(os.path.join(src_path,topology_name))

                df_topology = pl.DataFrame()
                for file_name in file_list:
                    df_pl = pl.read_parquet(os.path.join(src_path, topology_name, file_name)).drop(['status','length'])
                    if df_pl.shape[0]:
                        df_topology = df_pl if df_topology.is_empty() else df_topology.vstack(df_pl)

                if df_topology.shape[0]:
                    file_name = f"{topology_name}_{df_topology.select(pl.min('fromTime')).item()}_{df_topology.select(pl.max('fromTime')).item()}"
                    save_path = os.path.join(dst_path, file_name)
                    df_topology.write_parquet(save_path)

                    log.info(f"[{index}] Processed measurements for {topology_name} with {df_topology.shape[0]} sample records taken from {df_topology.select(pl.min('fromTime')).item()} to {df_topology.select(pl.max('fromTime')).item()}")
                else:
                    log.info(f"[{index}] Skipped processing measurements for {topology_name} with {df_topology.shape[0]}")


def bronze_to_silver(src_path: str, dst_path: str, date_from: datetime, date_to: datetime):
    file_list = os.listdir(src_path)
    for file_name in file_list:
        # read parquet
        file_path = os.path.join(src_path, file_name)
        df = pl.read_parquet(file_path)

        # capture data range of interest
        df = df.with_columns( pl.col("fromTime").str.to_datetime(format=time_format))
        df = df.with_columns( pl.col("toTime").str.to_datetime(format=time_format))
        df = df.filter(pl.col("fromTime").is_between(date_from, date_to)).sort(by='fromTime')

        # split data into respective Pload, Pprod
        df_load = df.filter(pl.col('type')==1)
        df_prod = df.filter(pl.col('type')==3)