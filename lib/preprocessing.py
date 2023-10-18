from datetime import datetime
import polars as pl
import os, json, time


PATH = os.path.dirname(__file__)

from lib.etl import etl_bronze_to_silver
from lib import Logging

log = Logging()

def prepare_usagepoints():

    # source and destination for ETL
    src_path = PATH + f"/../data/raw/usagepoints"
    dst_path = PATH + f"/../data/bronze/usagepoints/"

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
    df.write_parquet(os.path.join(dst_path), f"/{datetime.now().isoformat(sep='T')}")


def prepare_features(date_from: str, date_to: str, features_name: str='features', verbose: bool=False):

    # source and destination for ETL
    bronze_path = PATH + f"/../data/bronze/measurements"
    silver_path = PATH + f"/../data/silver/"

    # build feature table per neighborhood
    t0 = time.time()
    file_list = os.listdir(bronze_path)
    df_features = pl.DataFrame()
    for index, file_name in enumerate(file_list):

        topology = file_name.split(sep='_2023')[0]
        log.info(f"[{datetime.now().isoformat()}] Compile feature list for topology {topology} being {index} of {len(file_list)}")

        t0 = time.time()
        df = etl_bronze_to_silver(topology=topology, date_from=date_from, date_to=date_to)

        def get_topology(df: pl.DataFrame)->pl.Utf8:
            return df.select(pl.col('topology').first()).item()

        def get_data_from(df: pl.DataFrame)->pl.Utf8:
            return df.select('fromTime').min().item().isoformat()

        def get_data_to(df: pl.DataFrame)->pl.Utf8:
            return df.select('toTime').max().item().isoformat()

        def get_sample_cnt(df: pl.DataFrame)->pl.Int64:
            return df.shape[0]

        def get_ami_cnt(df: pl.DataFrame)->pl.Int64:
            return df.n_unique('meteringPointId')

        def get_ami_load_cnt(df: pl.DataFrame)->pl.Int64:
            return df.filter(pl.col('p_load_kwh')>0).n_unique(subset='meteringPointId')

        def get_ami_prod_cnt(df: pl.DataFrame)->pl.Int64:
            return df.filter(pl.col('p_prod_kwh')>0).n_unique(subset='meteringPointId')

        def get_res_pen(df: pl.DataFrame)->pl.Float64:
            return round(get_ami_prod_cnt(df)/max(1,get_ami_load_cnt(df))*100,1)

        def get_p_load_max(df: pl.DataFrame)->pl.Float64:
            load_max = df.filter(pl.col('p_load_kwh')>0).select(pl.col('p_load_kwh')).max().item()
            return float() if  load_max is None else round(load_max,1)

        def get_p_prod_max(df: pl.DataFrame)->pl.Float64:
            prod_max =df.filter(pl.col('p_prod_kwh')>0).select(pl.col('p_prod_kwh')).max().item()
            return float() if  prod_max is None else round(prod_max,1)

        def get_net_export_max(df: pl.DataFrame)->pl.Float64:
            export_max = df.select((pl.col('p_prod_kwh')-pl.col('p_load_kwh'))).max().item()
            return float() if  export_max is None else round(export_max,1)

        def get_net_export_min(df: pl.DataFrame)->pl.Float64:
            export_min = round(df.select((pl.col('p_prod_kwh')-pl.col('p_load_kwh'))).min().item(),1)
            return float() if  export_min is None else round(export_min,1)

        def get_agg_avg_features(df:pl.DataFrame, every='24h'):
            df = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every=every).agg(
                (pl.col('p_load_kwh').sum()).alias('p_load_nb_avg_kwh'),
                (pl.col('p_prod_kwh').sum()).alias('p_prod_nb_avg_kwh'),
                ((pl.col('p_prod_kwh')-pl.col('p_load_kwh')).sum()).alias('p_export_nb_avg_kwh'),
            )
            return {'p_load_nb_max_24h_agg_kwh': round(df.select('p_load_nb_avg_kwh').max().item(),1),
                    'p_prod_nb_max_24h_agg_kwh': round(df.select('p_prod_nb_avg_kwh').max().item(),1),
                    'p_gexp_nb_max_24h_agg_kwh': round(df.select('p_export_nb_avg_kwh').max().item(),1)}


        # compile features list
        df_feature = pl.DataFrame(
            {**{'topology': get_topology(df),
                'date_from': get_data_from(df),
                'date_to': get_data_to(df),
                'sample_cnt':get_sample_cnt(df),
                'ami_cnt': get_ami_cnt(df),
                'ami_load_cnt': get_ami_load_cnt(df),
                'ami_prod_cnt': get_ami_prod_cnt(df),
                'res_pen_pers': get_res_pen(df),
                'p_load_max': get_p_load_max(df),
                'p_prod_max': get_p_prod_max(df),
                'net_export_max': get_net_export_max(df),
                'net_export_min': get_net_export_min(df)},
               **get_agg_avg_features(df)
             })

        if verbose:
            with pl.Config() as cfg:
                cfg.set_tbl_cols(-1)
                cfg.set_tbl_width_chars(1000)
                print(df_feature)

        # stack features
        df_features = df_feature if df_features.is_empty() else df_features.vstack(df_feature)

    # save features
    log.info(f"[{datetime.now().isoformat()}] Completed feature list construction in {time.time()-t0:.2f} seconds. Write file to {os.path.join(silver_path,file_name)}")
    df_features.write_parquet(os.path.join(silver_path,features_name))