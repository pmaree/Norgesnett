from datetime import datetime
import polars as pl
import os, json


PATH = os.path.dirname(__file__)


def prepare_usagepoints(src_path: str, dst_path: str):

    # build file list of topologies containing usagepoints
    files = []
    for file_path in os.listdir(src_path):
        if os.path.isfile(os.path.join(src_path, file_path)):
            files.append(file_path)

    # parse constructed file list of usagepoints
    df=None
    for index, file_path in enumerate(files):
        print(f"[{index}] Parse {file_path} for usage points")
        with open(os.path.join(src_path, file_path)) as fp:
            data = fp.read()
            topology = file_path.split('.',1)[0]
            ami_id = pl.DataFrame(json.loads(data)).select('IdentifiedObject.name').rename({'IdentifiedObject.name': 'ami_id'}).to_numpy().flatten().tolist()
            data = {'topology': topology, 'ami_id':ami_id}
            df_ = pl.DataFrame(data, schema={'topology': pl.Utf8, 'ami_id': pl.Utf8})
            df = df_ if df is None else df.vstack(df_)
            print(f"[{index}] Parsed {df_.shape[0]} unique AMI ID's for topology {topology}")

    # remove null entries
    nan_cnt = (df.null_count().select(pl.all()).sum(axis=1).alias('nan')).item()
    if nan_cnt:
        df = df.drop_nulls()

    # save parsed data
    df.write_parquet(os.path.join(dst_path) + f"/{datetime.now().isoformat(sep='T')}")