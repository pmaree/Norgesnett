import polars as pl
import pandas as pd
import os
import numpy as np

PATH = os.path.dirname(__file__)

def plot():
    path = PATH+f"/../data/silver/"

    df_pl = pl.read_parquet(os.path.join(path,'features'))
    prod_cnt_list = np.linspace(0,
                                df_pl.select(pl.col('ami_prod_cnt').max()).item(),
                                df_pl.select(pl.col('ami_prod_cnt').max()).item()+1)

    df_pd = pd.DataFrame(columns=['prod_ami_cnt','topology_cnt'])

    for prod_ami_cnt in prod_cnt_list:
        topology_cnt = df_pl.filter(pl.col('ami_prod_cnt')>=prod_ami_cnt).n_unique('topology')
        df_pd.loc[len(df_pd.index)] = [prod_ami_cnt,topology_cnt]
    print('')

if __name__ == "__main__":
    plot()