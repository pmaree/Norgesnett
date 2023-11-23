from datetime import datetime
import polars as pl
import pandas as pd
import os, json, requests

from lib import Logging

PATH = os.path.dirname(__file__)
log = Logging()


def preprocess():

    # source and destination for ETL
    coordinates_path = PATH + f"/../data/raw/coordinates/usagepoints.csv"
    src_path = PATH + f"/../data/raw/usagepoints/"
    dst_path = PATH + f"/../data/bronze/usagepoints/"

    # build file list of topologies containing usagepoints
    files = []
    for file_path in os.listdir(src_path):
        if os.path.isfile(os.path.join(src_path, file_path)):
            files.append(file_path)

    # build usagepoints coordinate map
    if os.path.isfile(coordinates_path):
        with open(coordinates_path,'r') as fp:
            df_coord = pd.read_csv(fp, sep=',').rename(columns={'filename': 'topology'})

    # parse constructed file list of usagepoints
    df=None
    missing_coordinates = []
    for index, file_path in enumerate(files):
        with open(os.path.join(src_path, file_path)) as fp:
            data = fp.read()
            topology = file_path.split('.json')[0]
            ami_id = pl.DataFrame(json.loads(data)).select('IdentifiedObject.name').rename({'IdentifiedObject.name': 'ami_id'}).to_numpy().flatten().tolist()

            coord = df_coord.loc[df_coord['topology']==topology]
            # Holtermanns v. 7, 7030 Trondheim
            longitude = 59.857865
            latitude = 10.660569

            try:
                longitude = coord.iloc[0]['longitude']
                latitude = coord.iloc[0]['latitude']
            except Exception as e:
                missing_coordinates.append(topology)

            # TODO hack to solve data quality issue from Norgesnett
            if latitude < longitude:
                latitude_ = latitude
                latitude = longitude
                longitude = latitude_

            data = {'topology': topology,
                    'longitude':longitude,
                    'latitude':latitude,
                    'ami_id':ami_id}
            df_ = pl.DataFrame(data, schema={'topology': pl.Utf8,
                                             'longitude':pl.Float64,
                                             'latitude':pl.Float64,
                                             'ami_id': pl.Utf8})
            df = df_ if df is None else df.vstack(df_)
            print(f"[{index}] Parsed {df_.shape[0]} unique AMI ID's for topology {topology} at longitude:latitude {longitude}:{latitude}")

    assert(df.n_unique('topology') == len(files))
    log.warning(f"Miss {len(missing_coordinates)} coordinates for following topologies: {missing_coordinates}")

    # remove null entries
    nan_cnt = (df.null_count().select(pl.all()).sum(axis=1).alias('nan')).item()
    if nan_cnt:
        df = df.drop_nulls()

    # save parsed data
    dst_file_path = os.path.join(dst_path, f"{datetime.now().date()}")
    df.write_parquet(dst_file_path)

    log.info(f"Parsed {index} topologies and saved results at {dst_file_path}")


if __name__ == "__main__":
    preprocess()