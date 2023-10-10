import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import polars as pl
import numpy as np
import os

time_format = '%Y-%m-%dT%H:%M:%S'

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

class Analysis:
    def __init__(self, name):
        self.stats = {}

        self._name = name.split(sep='_2023')[0]
        self._ami_cnt = 1
        self._prod_cnt = 0
        self._load_cnt = 1
        self._pluskunde = False
        self._max_export_kWh = float(0)
        self._ave_export_kWh = float(0)
        self._max_production_kWh = float(0)
        self._max_consumption_kWh = float(0)

    @property
    def to_polars(self) -> pl.DataFrame:
        return pl.DataFrame({'topology':self._name,
                             'ami_cnt':self._ami_cnt,
                             'prod_cnt': self._prod_cnt,
                             'load_cnt': self._load_cnt,
                             'penetration': self._prod_cnt/self._ami_cnt*100,
                             'pluskunder': self._pluskunde,
                             'max_export_kWh': self._max_export_kWh,
                             'ave_export_kWh': self._ave_export_kWh,
                             'max_production_kWh': self._max_production_kWh,
                             'max_consumption_kWh': self._max_consumption_kWh})

    @property
    def pluskunde(self)->pl.Boolean:
        return self._pluskunde

    @pluskunde.setter
    def pluskunde(self, value):
        self._pluskunde = value

    @property
    def max_production_kWh(self)->pl.Float64:
        return self._max_production_kWh

    @max_production_kWh.setter
    def max_production_kWh(self, value):
        self._max_production_kWh = value

    @property
    def max_consumption_kWh(self)->pl.Float64:
        return self._max_consumption_kWh

    @max_consumption_kWh.setter
    def max_consumption_kWh(self, value):
        self._max_consumption_kWh = value

    @property
    def ami_cnt(self)->pl.Float64:
        return self._ami_cnt

    @ami_cnt.setter
    def ami_cnt(self, value):
        self._ami_cnt = value

    @property
    def prod_cnt(self)->pl.Float64:
        return self._prod_cnt

    @prod_cnt.setter
    def prod_cnt(self, value):
        self._prod_cnt = value

    @property
    def load_cnt(self)->pl.Float64:
        return self._load_cnt

    @load_cnt.setter
    def load_cnt(self, value):
        self._load_cnt = value

    @property
    def max_export_kWh(self)->pl.Float64:
        return self._max_export_kWh

    @max_export_kWh.setter
    def max_export_kWh(self, value)->pl.Float64:
        self._max_export_kWh = value

    @property
    def ave_export_kWh(self)->pl.Float64:
        return self._ave_export_kWh

    @ave_export_kWh.setter
    def ave_export_kWh(self, value)->pl.Float64:
        self._ave_export_kWh = value

def sample_timeseries(resolution_hour: float, values: list, index: list) -> pd.DataFrame:
    series = pd.Series(data=values, index=index)
    interpolated = series.resample(rule=f"{np.round(resolution_hour * 60)}T").interpolate(method='linear')
    sampled_timeseries = pd.DataFrame(data=interpolated, columns=['value'])
    sampled_timeseries.reset_index(inplace=True)
    sampled_timeseries = sampled_timeseries.rename(columns={'index': 'fromTime'})
    return sampled_timeseries


# interpolate for missing data and pad with zeros on date range outside of data scope
def interpolate(df: pd.DataFrame, date_from: datetime, date_to: datetime, reindex=False, plot=False) -> pd.DataFrame:
    resolution_hour = 1
    values = df['value'].values
    index = df['fromTime'].values

    df = pd.DataFrame(sample_timeseries(resolution_hour=resolution_hour, values=values, index=index))

    if reindex:
        df = df.set_index('fromTime')
        df = df.resample('H').sum()
        time_range = pd.date_range(start=date_from, end=date_to, freq='H')
        df = df.reindex(time_range, fill_value=0)
        df = df.reset_index(names='fromTime')

    if plot:
        fig = plt.figure()
        plt.plot(df['fromTime'], df['value'], 'b-', label='Original')
        plt.plot(df['fromTime'], df['value'], 'g--', label='Lerp')
        plt.tick_params(axis='x', labelrotation=15, labelsize=8, size=2)
        fig.suptitle(f'Interplation of timeseries', fontsize=12)
        fig.show()

    return df

# return all AMI points considered a prosumer
def get_prosumers(df:pl.DataFrame)->pl.DataFrame:

    ami_cnt = df.n_unique(subset='meteringPointId')
    df_consumers = pl.DataFrame(schema=df.schema)
    df_producers = df.filter(pl.col('type')==3).unique(subset='meteringPointId')

    for producer in df_producers.rows(named=True):
        df_ = df.filter((pl.col('type')==1) & (pl.col('meteringPointId')==producer['meteringPointId']))
        df_consumers = df_ if df_consumers.is_empty() else df_consumers.vstack(df_)

    return {'consumer':df_consumers, 'producer':df_producers, 'ami_cnt':ami_cnt}


def self_consumption(prosumers: dict) ->pd.DataFrame:

    # run statistics
    df_avg_load = prosumers['consumer'].sort(by=['fromTime']).groupby_dynamic('fromTime', every='1h').agg(pl.col('value').mean())
    df_avg_prod = prosumers['producer'].sort(by=['fromTime']).groupby_dynamic('fromTime', every='1h').agg(pl.col('value').mean())

    # retrieve date range
    date_from = df_avg_load.select('fromTime').min().item()
    date_to =df_avg_load.select('fromTime').max().item()

    df_self_consumption = pd.DataFrame()
    # do interpolation of series
    df_interp_load = interpolate(df=df_avg_load.to_pandas(), date_from=date_from, date_to=date_to, reindex=False)
    df_interp_prod = interpolate(df=df_avg_prod.to_pandas(), date_from=date_from, date_to=date_to, reindex=True)

    # solve for self consumption
    df_self_consumption = df_interp_load.rename(columns={'value': 'consumption'})
    df_self_consumption['production'] = df_interp_prod['value']
    df_self_consumption['prosumer'] = df_self_consumption['production'] - df_self_consumption['consumption']
    df_self_consumption['grid_export'] = df_self_consumption.prosumer.where(df_self_consumption.prosumer > 0, 0)

    # return self consumption
    return df_self_consumption

def statistics(src_path: str, dst_path: str, date_from: datetime, date_to: datetime, name: str='statistics'):

    dst_path = os.path.join(dst_path, name)
    if os.path.exists(dst_path):
        with pl.Config() as cfg:
            cfg.set_tbl_cols(-1)
            cfg.set_tbl_rows(50)
            cfg.set_tbl_width_chars(width=1500)
            df = pl.read_parquet(dst_path).sort(by='penetration', descending=True)
            print('')

        return

    file_list = os.listdir(src_path)

    df_stats = pl.DataFrame()

    for index, file_name in enumerate(file_list):

        print(f"[{index}] Process {file_name}")

        # read parquet
        file_path = os.path.join(src_path, file_name)
        df = pl.read_parquet(file_path)

        stats = Analysis(file_name)
        stats.ami_cnt = df.n_unique(subset='meteringPointId')
        stats.load_cnt = df.filter(pl.col('type') == 1).unique(subset='meteringPointId').shape[0]
        stats.prod_cnt = df.filter(pl.col('type') == 3).unique(subset='meteringPointId').shape[0]

        # only process if we have production

        # time slicing
        df = df.with_columns( pl.col("fromTime").str.to_datetime(format=time_format),pl.col("toTime").str.to_datetime(format=time_format) ).sort(by="fromTime")
        #date_from = max(df.select('fromTime').min().item(), date_from)
        #date_to = min(df.select('fromTime').max().item(), date_to)
        df = df.filter(pl.col("fromTime").is_between(date_from, date_to)).sort(by='fromTime')

        if stats.prod_cnt:
            stats.pluskunde = True
            prosumers = get_prosumers(df)
            stats.max_production_kWh = prosumers['producer'].unique(subset='meteringPointId').filter(pl.col('type')==3).select(pl.col('value').max()).item()
            stats.max_consumption_kWh = prosumers['consumer'].unique(subset='meteringPointId').filter(pl.col('type')==1).select(pl.col('value').max()).item()

            # get self-consumption statistic
            df_self_consumption = self_consumption(prosumers)

            stats.max_export_kWh = df_self_consumption['grid_export'].max()
            stats.ave_export_kWh = df_self_consumption['grid_export'].max()/prosumers['ami_cnt']
        else:
            stats.max_consumption_kWh = df.filter(pl.col('type')==1).select(pl.col('value')).max().item()

        df_stats = stats.to_polars if df_stats.is_empty() else df_stats.vstack(stats.to_polars)

    df_stats.write_parquet(dst_path)
    print(f"Complete. Save statistics to {dst_path}")



