from datetime import datetime, timedelta
import polars as pl
import os

from lib import Logging

PATH = os.path.dirname(__file__)


def print_df(df: pl.DataFrame, sort_by: str, n:int):
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
        cfg.set_tbl_width_chars(1000)
        cfg.set_tbl_rows(-1)
        print(df.sort(by=sort_by, descending=True).head(n))


if __name__ == "__main__":
    src_path = PATH + f"/../../data/silver/features/load"

    df = pl.read_parquet(src_path)
    #df = df.with_columns((pl.col('nb_day_peak_max')/pl.col('df_min')).alias('sec_est_ub_kva'),  # conservative, over-size trafo (low DF)
    #                     (pl.col('nb_day_peak_max')/pl.col('df_max')).alias('sec_est_lb_kva'))
    # optimistic, under-size trafo (high DF)
    #df = df.with_columns(((pl.col('sec_kva')-pl.col('sec_est_lb_kva'))/(pl.col('sec_est_ub_kva')-pl.col('sec_est_lb_kva'))).alias('sec_kva_ration') )
    df = df.with_columns((pl.col('nb_day_peak_max')/pl.col('sec_kva')).alias('tf_utilization')*100)
    print_df(df, sort_by='tf_utilization', n=100)
