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
def interpolate(df: pl.DataFrame, date_from: datetime, date_to: datetime, rule='1H',  zero_padding=True, plot=False) -> pl.DataFrame:

    df_orig = df.to_pandas()

    # upsample dataseries
    series = pd.Series(data=df_orig['value'].values, index=df_orig['fromTime'].values)
    interp = series.resample(rule=rule).interpolate(method='linear')
    df_interp = pd.DataFrame(data=interp, columns=['value'])
    df_interp.reset_index(inplace=True)
    df_interp = df_interp.rename(columns={'index': 'fromTime'})

    # zero pad boundaries
    if zero_padding:
        df_interp = df_interp.set_index('fromTime')
        df_interp = df_interp.resample('H').sum()
        time_range = pd.date_range(start=date_from, end=date_to, freq='H')
        df_interp = df_interp.reindex(time_range, fill_value=0)
        df_interp = df_interp.reset_index(names='fromTime')

    df_interp = pl.from_pandas(df_interp)
    if plot:
        _plot(df_orig=df, df_interp=df_interp)

    return df_interp

# returns timeseries data for AMI's associated with topology df interpolated on datetime ranfge
def timeseries(df_topology: pl.DataFrame, date_from: datetime, date_to: datetime)->pl.DataFrame:

    # retrief list of AMI
    df_timeseries = pl.DataFrame()
    ami_list = df_topology.select('meteringPointId').unique().to_series().to_list()

    for ami_id in ami_list:
        df_ami = df_topology.filter(pl.col('meteringPointId') == ami_id)
        df_ = _timeseries_for_ami(df_ami=df_ami, date_from=date_from, date_to=date_to)
        df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)

    return df_timeseries


def _timeseries_for_ami(df_ami: pl.DataFrame, date_from: datetime, date_to: datetime)->pl.DataFrame:

    df_timeseries = pl.DataFrame()
    type_list = df_ami.select(pl.col('type')).unique().to_series().to_list()

    for type in type_list:
        df_type = df_ami.filter(pl.col('type') == type)
        df_ = _timeseries_for_type(df_type=df_type, date_from=date_from, date_to=date_to)
        df_timeseries = df_ if df_timeseries.is_empty() else df_timeseries.vstack(df_)

    # resolve grid export TODO: Need to be expanded to other types on implemented by Norgesnett
    df_=df_timeseries.filter(pl.col('type')==1).sort(by='fromTime', descending=False).rename({'value':'p_load_kwh'}).with_columns(pl.col('fromTime').cast(pl.Datetime())).drop(['type','unit'])
    if df_timeseries.filter(pl.col('type')==3).shape[0]:
        # joint production column if available
        df_timeseries = df_.join(df_timeseries.filter(pl.col('type')==3).sort(by='fromTime', descending=False).rename({'value':'p_prod_kwh'}).with_columns(pl.col('fromTime').cast(pl.Datetime())).select(['fromTime','p_prod_kwh']),on='fromTime', validate='1:1')
    else:
        # else set to zero.
        df_timeseries = df_.with_columns(p_prod_kwh=pl.lit(0.0))

    return df_timeseries


def _timeseries_for_type(df_type: pl.DataFrame, date_from: datetime, date_to: datetime) -> pl.DataFrame:

    # Create a boolean mask to identify outliers and filter them out
    std = df_type['value'].std()
    if std:
        mean = df_type['value'].mean()
        mask = (df_type['value'] < mean + 2*std) & (df_type['value'] > mean - 2*std)
        df_type = df_type.filter(mask)

    # Now interpolate / extrapolate and fill for outliers
    df_interp = interpolate(df_type, date_from=date_from, date_to=date_to)

    df = (df_interp.with_columns(
        [
            pl.col('fromTime').map_elements(lambda x: x + timedelta(hours=1)).alias('toTime'),
            pl.lit(df_type.select(pl.col('topology').first()).item()).alias('topology'),
            pl.lit(df_type.select(pl.col('meteringPointId').first()).item()).alias('meteringPointId'),
            pl.lit(df_type.select(pl.col('type').first()).item()).alias('type'),
            pl.lit(df_type.select(pl.col('unit').first()).item()).alias('unit')
        ]
    ))

    return df.select(['fromTime', 'toTime', 'topology', 'meteringPointId', 'type', 'value', 'unit'])
