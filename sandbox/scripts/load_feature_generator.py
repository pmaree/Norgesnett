from datetime import datetime, timedelta
import polars as pl
import os

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
    def __init__(self, path: str, name: str):
        self.__topology = name
        self.__df = pl.read_parquet(os.path.join(path, name))

    @property
    def name(self):
        return self.__topology

    @property
    def data(self):
        return self.__df


def topology_gen(path:str) -> Topology:
    topology_list = os.listdir(path)
    for topology in topology_list:
        yield Topology(path=path, name=topology)


def get_summary(df: pl.DataFrame) -> dict:
    return {'date_from':df.select('fromTime').min().item(),
            'date_to':df.select('toTime').max().item(),
            'topology_name':df.select('topology').unique().item(),
            'ami_cnt':df.select('meteringPointId').n_unique()
            }

def get_diversity_factor(df: pl.DataFrame) -> dict:
    # 
    daily_ami_peak_loads = (df.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1d', by=['meteringPointId']).agg(pl.col('p_load_kwh').max())
                           .with_columns(pl.col('fromTime').map_elements(lambda d: d + timedelta(days=1))
                                         .alias('toTime')).rename({'p_load_kwh':'daily_ami_peak'})
                           .select('fromTime','toTime','meteringPointId','daily_ami_peak'))

    sum_daily_ami_peak_loads = daily_ami_peak_loads.sort('fromTime').group_by_dynamic('fromTime', every='1d').agg(pl.col('daily_ami_peak').sum().alias('sum_daily_ami_peaks'))


    daily_nb_peak_loads = (df.sort('fromTime').group_by_dynamic('fromTime', every='1h').agg(pl.col('p_load_kwh').sum())
                            .group_by_dynamic('fromTime', every='1d').agg(pl.col('p_load_kwh').max()).rename({'p_load_kwh':'daily_nb_peak'}))

    diversity_factor = sum_daily_ami_peak_loads.join(daily_nb_peak_loads, on='fromTime', validate='1:1').with_columns((pl.col('sum_daily_ami_peaks')/(pl.col('daily_nb_peak'))).alias('diversity_factor'))


    return {'diversity_factor':diversity_factor.select('diversity_factor').min().item()}


def generator():

    topology_measurement_path = os.path.join(os.path.dirname(__file__), '../../data/silver/measurements/')

    topology_features = pl.DataFrame()
    for tf in topology_gen(path=topology_measurement_path):
        get_diversity_factor(tf.data)
        topology_features = pl.DataFrame({**get_summary(tf.data),
                                          **coord.get(tf.name),
                                          **trafo.get(tf.name),
                                          **get_diversity_factor(tf.data)
                                          })



if __name__ == "__main__":
    generator()
