from lib.analysis import metric_1
from datetime import datetime, timedelta
import polars as pl
import os

from lib.analysis import interpolate_date_range, interpolate

time_format = '%Y-%m-%dT%H:%M:%S'

PATH = os.path.dirname(__file__)


class Metric():
    def __init__(self, metric_desc: str, metric_fun, sample_size: int):
        self.metric_desc=metric_desc
        self.metric_fun=metric_fun
        self.sample_size = sample_size

    @property
    def desc(self):
        return self.metric_desc

    @property
    def eval(self):
        assert(self.metric_fun is not None)
        return self.metric_fun

    @property
    def sample(self):
        return self.sample_size


class Ranking(Metric):
    def __init__(self, sample_size: int):
        self.sample_size = sample_size

    @property
    def metric_a(self):
        return Metric(metric_desc='metric a',
                      metric_fun=lambda df: (df.with_columns(df.select(['prod_cnt'])
                                                             .map_rows(lambda x: x[0])).with_columns(pl.col('map') / pl.col('map').max())
                                             .rename({'map': 'metric_a'})
                                             .sort('metric_a', descending=True)),
                      sample_size=self.sample_size)

    @property
    def metric_b(self):
        return Metric(metric_desc='metric b',
                      metric_fun=lambda df: (df.with_columns(df.select(['max_production_kWh', 'max_consumption_kWh'])
                                                             .map_rows(lambda x: (x[0] * 0.5 - x[1]) / 100)).with_columns(pl.col('map') / pl.col('map').max())
                                             .rename({'map': 'metric_b'})
                                             .sort('metric_b', descending=True)),
                      sample_size=self.sample_size)


class Processing:
    def __init__(self, metric: Metric):
        self.desc = metric.desc
        self.path = PATH + f"/data/silver"
        self.df_sample = metric.eval(pl.read_parquet(os.path.join(self.path, 'statistics'))).head(n=metric.sample)

    @property
    def list_topologies(self) -> pl.List:
        return self.df_sample.select('topology').to_series().to_list()

    @property
    def raw_measurements(self) -> pl.DataFrame:
        path = PATH + f"/data/bronze/measurements"
        file_list = os.listdir(path)

        df = pl.DataFrame()
        for file_name in file_list:
            topology = file_name.split(sep='_2023')[0]
            if topology in self.list_topologies:
                df_ = pl.read_parquet(os.path.join(path, file_name)).with_columns(topology = pl.lit(topology))
                df = df_ if df.is_empty() else df.vstack(df_)
        return df

    def _process_type(self, df_type: pl.DataFrame, date_range: dict) -> pl.DataFrame:
        df_interp = interpolate(df_type.to_pandas(), date_range=date_range, zero_padding=True)
        df = (pl.DataFrame.
              _from_pandas(df_interp)
              .with_columns([pl.col('fromTime')
                            .map_elements(lambda x: x + timedelta(hours=1)).alias('toTime'),
                             pl.lit(df_type.select(pl.col('topology').first()).item()).alias('topology'),
                             pl.lit(df_type.select(pl.col('meteringPointId').first()).item()).alias('meteringPointId'),
                             pl.lit(df_type.select(pl.col('type').first()).item()).alias('type'),
                             pl.lit(df_type.select(pl.col('unit').first()).item()).alias('unit')]))
        return df.select(['fromTime', 'toTime', 'topology', 'meteringPointId', 'type', 'value', 'unit'])

    def _timeseries_extraction(self, df: pl.DataFrame, ami_id: str)->pl.DataFrame:
        df = df.filter(pl.col('meteringPointId')==ami_id)
        date_range = interpolate_date_range(df)

        df_timeseries = pl.DataFrame()
        type_list = df.select(pl.col('type')).unique().to_series().to_list()

        for type in type_list:
            df_type = df.filter(pl.col('type') == type)
            df_ = self._process_type(df_type=df_type, date_range=date_range)
            df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)
        return df_timeseries

    def timeseries(self, topology: str, date_from: datetime, date_to: datetime, save=True)->pl.DataFrame:
        if topology not in self.list_topologies:
            raise Exception(f"{topology} not in availible list of {self.list_topologies} topologies")

        # initial raw read and pre-filter of measurents in appropriate range
        df = (self.raw_measurements
              .filter(pl.col('topology')==topology)
              .sort(by='fromTime')
              .with_columns( [pl.col("fromTime").str.to_datetime(format=time_format),
                              pl.col("toTime").str.to_datetime(format=time_format)]))\
            .filter(pl.col("fromTime").is_between(date_from, date_to)).sort(by='fromTime')

        # retrief list of AMI
        df_timeseries = pl.DataFrame()
        ami_list = df.select('meteringPointId').unique().to_series().to_list()
        for ami_id in ami_list:
            df_ = self._timeseries_extraction(df=df, ami_id=ami_id)
            df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)

        if save:
            file_name = f"{df_timeseries.select(pl.col('topology').first()).item()}_{df_timeseries.select(pl.col('fromTime').min()).item()}_{df_timeseries.select(pl.col('fromTime').max()).item()} [{self.desc}]"
            df_timeseries.write_parquet(os.path.join(self.path, file_name))

        return df_timeseries

if __name__ == "__main__":

    exp = Processing(Ranking(sample_size=4).metric_a)

    timeseries = exp.timeseries(topology='S_1297429_T_1297434', date_from=datetime(2023, 3, 1), date_to=datetime(2023, 9, 1))





