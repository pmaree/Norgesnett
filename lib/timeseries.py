import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
import polars as pl
import numpy as np

time_format = '%Y-%m-%dT%H:%M:%S'


def _plot(df_orig: pl.DataFrame, df_interp: pl.DataFrame):

    fig, ax = plt.subplots()
    plt.plot(df_orig.select(pl.col('fromTime')), df_orig.select(pl.col('value')), 'b-', label='Original')
    plt.plot(df_interp.select(pl.col('fromTime')), df_interp.select(pl.col('value')), 'g--', label='Interp')

    plt.tick_params(axis='x', labelrotation=15, labelsize=8, size=2)
    fig.suptitle(f'Interpolation of timeseries', fontsize=12)
    ax.set_ylabel('kWh')

    plt.legend(loc="upper left")
    fig.tight_layout()
    fig.show()


# interpolate for missing data and pad with zeros on date range outside of data scope
# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
def interpolate(df: pl.DataFrame, rule='1H', plot=False) -> pl.DataFrame:


    df_interp = _sample_timeseries(df=df, rule=rule)

    if plot:
        _plot(df_orig=df, df_interp=df_interp)

    '''
    if zero_padding:
        df = df.set_index('fromTime')
        df = df.resample('H').sum()
        time_range = pd.date_range(start=date_range['from'], end=date_range['to'], freq='H')
        df = df.reindex(time_range, fill_value=0)
        df = df.reset_index(names='fromTime')

    if plot:
        
    '''

    return df_interp


def _sample_timeseries(df: pl.DataFrame, rule: str) -> pl.DataFrame:

    # convert to pandas and get index and values
    df = df.to_pandas()
    series = pd.Series(data=df['value'].values, index=df['fromTime'].values)
    interpolated = series.resample(rule=rule).interpolate(method='linear')
    sampled_timeseries = pd.DataFrame(data=interpolated, columns=['value'])
    sampled_timeseries.reset_index(inplace=True)
    sampled_timeseries = sampled_timeseries.rename(columns={'index': 'time'})
    return pl.from_pandas(sampled_timeseries)


# returns timeseries data for AMI's associated with topology df interpolated on datetime ranfge
def timeseries(df_topology: pl.DataFrame, date_from: datetime, date_to: datetime)->pl.DataFrame:

    # initial raw read and pre-filter of measurents in appropriate range
    df = (df_topology.sort(by='fromTime')
          .with_columns( [pl.col("fromTime").str.to_datetime(format=time_format),
                          pl.col("toTime").str.to_datetime(format=time_format)])) \
        .filter(pl.col("fromTime").is_between(date_from, date_to)).sort(by='fromTime')

    # retrief list of AMI
    df_timeseries = pl.DataFrame()
    ami_list = df.select('meteringPointId').unique().to_series().to_list()

    for ami_id in ami_list:
        df_ami = df.filter(pl.col('meteringPointId') == ami_id)
        df_ = _timeseries_for_ami(df_ami=df_ami)
        df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)

    return df_timeseries


def _timeseries_for_ami(df_ami: pl.DataFrame)->pl.DataFrame:

    df_timeseries = pl.DataFrame()
    type_list = df_ami.select(pl.col('type')).unique().to_series().to_list()

    for type in type_list:
        df_type = df_ami.filter(pl.col('type') == type)
        df_ = _timeseries_for_type(df_type=df_type)
        df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)

    return df_timeseries


def _timeseries_for_type(df_type: pl.DataFrame) -> pl.DataFrame:

    df_interp = interpolate(df_type, zero_padding=True)

    df = (df_interp.rename({'time': 'fromTime'}).with_columns(
        [
            pl.col('fromTime').map_elements(lambda x: x + timedelta(hours=1)).alias('toTime'),
            pl.lit(df_type.select(pl.col('topology').first()).item()).alias('topology'),
            pl.lit(df_type.select(pl.col('meteringPointId').first()).item()).alias('meteringPointId'),
            pl.lit(df_type.select(pl.col('type').first()).item()).alias('type'),
            pl.lit(df_type.select(pl.col('unit').first()).item()).alias('unit')
        ]
    ))

    return df.select(['fromTime', 'toTime', 'topology', 'meteringPointId', 'type', 'value', 'unit'])
