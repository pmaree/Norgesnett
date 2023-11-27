from datetime import datetime, timedelta
import polars as pl
import os

from lib import Logging

PATH = os.path.dirname(__file__)
log = Logging()

class Tf:
    def __init__(self, path: str):
        self.df = pl.read_parquet(path)

    def get(self, name: str):
        return (self.df.filter(pl.col('topology')==name)
                .rename({'primary_rated_voltage':'pri_kv','primary_rated_apparent_power':'pri_kva','secondary_rated_voltage':'sec_kv','secondary_rated_apparent_power':'sec_kva'})
                .select('pri_kv','pri_kva','sec_kv','sec_kva')).to_dicts()[0]


class Coord:
    def __init__(self, path: str):
        self.df = pl.read_parquet(path)

    def get(self, name: str) -> dict:
        return self.df.filter(pl.col('filename')==name).select('price_area', 'latitude', 'longitude').to_dicts()[0]


trafo = Tf(path=os.path.join(os.path.dirname(__file__), '../../data/bronze/transformers/stations'))
coord = Coord(path=os.path.join(os.path.dirname(__file__), '../../data/bronze/coordinates/usagepoints'))


def load_transformer_data(path:str) -> pl.DataFrame:
    return pl.read_parquet(path)


class Topology:
    def __init__(self, path: str, name: str, num: int):
        self.__topology = name
        self.__df = pl.read_parquet(os.path.join(path, name))
        self.num = num

    @property
    def name(self):
        return self.__topology

    @property
    def data(self):
        return self.__df


def topology_gen(path:str) -> Topology:
    topology_list = os.listdir(path)
    for topology in topology_list:
        yield Topology(path=path, name=topology, num=len(topology_list))


def get_summary(df: pl.DataFrame) -> dict:
    return {'date_from':df.select('fromTime').min().item(),
            'date_to':df.select('toTime').max().item(),
            'topology_name':df.select('topology').unique().item(),
            'ami_cnt':df.select('meteringPointId').n_unique()
            }

def get_diversity_factor(df: pl.DataFrame) -> dict:
    # calculates the daily peak loads for the respective individual AMI meters
    # to test: df.filter(pl.col('fromTime').is_between(datetime(2022,9,1),datetime(2022,9,2)))
    #          .filter((pl.col('meteringPointId')=='707057500017289154')).select(pl.col('p_load_kwh').max())
    daily_ami_peak_loads = (df.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1d', by=['meteringPointId']).agg(pl.col('p_load_kwh').max())
                           .with_columns(pl.col('fromTime').map_elements(lambda d: d + timedelta(days=1))
                                         .alias('toTime')).rename({'p_load_kwh':'daily_ami_peak'})
                           .select('fromTime','toTime','meteringPointId','daily_ami_peak'))

    # sum the daily peaks for all AMI's in neighborhood
    # test: daily_ami_peak_loads.filter(pl.col('fromTime')==datetime(2022,9,1)).select(pl.col('daily_ami_peak').sum())
    sum_daily_ami_peak_loads = daily_ami_peak_loads.sort('fromTime').group_by_dynamic('fromTime', every='1d').agg(pl.col('daily_ami_peak').sum().alias('sum_daily_ami_peaks'))


    # solve for the collective neighborhood daily peak load. we aggregate over 1h to get hourly neighborhood loads, and group over daily increments and
    # choose the maximum
    # test:
    daily_nb_peak_loads = (df.sort('fromTime').group_by_dynamic('fromTime', every='1h').agg(pl.col('p_load_kwh').sum())
                            .group_by_dynamic('fromTime', every='1d').agg(pl.col('p_load_kwh').max()).rename({'p_load_kwh':'daily_nb_peak'}))

    # solve the diversity and coincidence factor (these are recipricols of each other). Large diversity factor allows for smaller
    # dimensioning of trafo. If we choose the smallest historical diversity factor, then we can get a conservative (over specked)
    # trafo dimensioning.
    factor = sum_daily_ami_peak_loads.join(daily_nb_peak_loads, on='fromTime', validate='1:1')\
        .with_columns((pl.col('sum_daily_ami_peaks')/(pl.col('daily_nb_peak'))).alias('diversity_factor'))

    # get the index of argument that minimize / maximize load factors to store their values
    idx_min = factor.select('diversity_factor').to_series().arg_min()
    idx_max = factor.select('diversity_factor').to_series().arg_max()

    return {'df_min':factor.select('diversity_factor').min().item(),
            'df_min_num': factor.select('sum_daily_ami_peaks')[idx_min].item(),
            'df_min_den': factor.select('daily_nb_peak')[idx_min].item(),
            'df_max':factor.select('diversity_factor').max().item(),
            'df_max_num': factor.select('sum_daily_ami_peaks')[idx_max].item(),
            'df_max_den': factor.select('daily_nb_peak')[idx_max].item()
            }


def generator():

    src_path = PATH + f"/../../data/silver/measurements"
    dst_path = PATH + f"/../../data/silver/features"

    df_features = pl.DataFrame()
    for index, tf in enumerate(topology_gen(path=src_path)):

        log.info(f"[{datetime.now().isoformat()}] Compile production feature list for topology {tf.name} being {index} of {tf.num}")

        try:
            df_feature = pl.DataFrame({**get_summary(tf.data),**coord.get(tf.name),**trafo.get(tf.name),**get_diversity_factor(tf.data)})

            df_features = df_feature if df_features.is_empty() else df_features.vstack(df_feature)
        except Exception as e:
            log.exception(f"[{datetime.now().isoformat()}] Failed in generating a feature list for for topology {tf.name} being {index} of {tf.num}: {e}")

    # save features
    dst_file_path = os.path.join(dst_path, "production")
    log.info(f"[{datetime.now().isoformat()}] Completed feature list for peak load analysis. Write file to {dst_file_path}")
    df_features.write_parquet(os.path.join(dst_file_path))


if __name__ == "__main__":
    generator()
