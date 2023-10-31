import numpy as np
import polars as pl
import os

PATH = os.getcwd()
path = f"{PATH}/../data/silver/"
print(path)

# Filter neighborhoods on plusskunde penetration
def r1_filter_penetration(df: pl.DataFrame, lower_limit: float):
    return df.filter(pl.col('res_pen_pers')>lower_limit)


# Filter for neighborhood size (closed-set of AMI grouping having at least 1 PCC)
# Reference: https://gridarchitecture.pnnl.gov/media/Modern-Distribution-Grid_Volume_II_v2_0.pdf (20 -150 reference)
def r2_filter_ami_cnt_lower_limit(df: pl.DataFrame, lower_limit: float):
    return df.filter(pl.col('ami_cnt')>lower_limit)


# Filter for a neighborhood having a minimal number of production points
def r3_filter_ami_prod_cnt_lower_limit(df: pl.DataFrame, lower_limit: float):
    return df.filter(pl.col('ami_prod_cnt')>lower_limit)


# Filter for a neighborhood having at least 1 production point producing more than limit
def r4_filter_include_for_production_over(df: pl.DataFrame, lower_limit: float):
    return df.filter(pl.col('p_prod_max')>lower_limit)


# Filter for neighborhoods that as at least 1 plusskunde net exporting more than limit
def r5_filter_for_ami_net_export(df: pl.DataFrame, lower_limit: float):
    return df.filter(pl.col('net_export_max')>lower_limit)


# Filter for neighorhoods with aggregated net export ratio penetration
def r6_filter_for_neighborhood_net_export_to_peak_load(df: pl.DataFrame, lower_limit: float):

    df = df.filter(pl.col('p_gexp_nb_max_24h_agg_kwh')>0).filter(pl.col('p_load_nb_max_24h_agg_kwh')>0)
    df = df.with_columns((pl.col('p_gexp_nb_max_24h_agg_kwh')/pl.col('p_load_nb_max_24h_agg_kwh')*100).alias('agg_net_export_load_ratio')).sort(by='agg_net_export_load_ratio', descending=True)
    return df


# Scenario
def scenario(df: pl.DataFrame, scenario_args: dict) -> dict:
    df = r1_filter_penetration(df, scenario_args['r1_lb'])
    df = r2_filter_ami_cnt_lower_limit(df, scenario_args['r2_lb'])
    df = r3_filter_ami_prod_cnt_lower_limit(df, scenario_args['r3_lb'])
    df = r4_filter_include_for_production_over(df, scenario_args['r4_lb'])
    df = r5_filter_for_ami_net_export(df, scenario_args['r5_lb'])
    #df = r6_filter_for_neighborhood_net_export_to_peak_load(df,  scenario_args['r6_lb'])

    # solve some statistics
    return {'number_of_neighborhoods'}


def print_df(df: pl.DataFrame, sort_by: str='ami_cnt', n:int=10):
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
        cfg.set_tbl_width_chars(1000)
        cfg.set_tbl_rows(-1)
        print(df.sort(by=sort_by, descending=True).head(n))


def reduction_path_1(df: pl.DataFrame):

    n_unique = df.n_unique(subset='topology')
    print(f"Number of neighborhoods: {n_unique}")

    # Rule 1
    df = df.filter(pl.col('ami_prod_cnt')>0)
    print(f"Number of neighborhoods with Plusskunder: {df.shape[0]}")

    # Rule 2
    nb_ami_lb = 20
    df = df.filter(pl.col('ami_cnt')>nb_ami_lb)
    n_unique = df.n_unique(subset='topology')
    print(f"Number of neighborhoods with minimum of {nb_ami_lb} AMI's: {n_unique}")

    # Rule 3
    pv_penetration_limit = 15
    fuse_i = 63
    fuse_v = 230
    phase = np.sqrt(3)
    df = (df.with_columns([(pl.col('ami_cnt')*fuse_i*fuse_v*phase/1000).alias('peak_feeder_load_kw'),
                           (pl.col('ami_cnt')*pl.col('p_prod_max')).alias('pv_peak_power_kw')])
          .with_columns((pl.col('pv_peak_power_kw')/pl.col('peak_feeder_load_kw')*100).alias('pv_penetration'))).sort(by='pv_penetration', descending=True)
    df = df.filter(pl.col('pv_penetration')>pv_penetration_limit)
    print(f"Number of neighborhoods exceeding {pv_penetration_limit}% PV penedation limit with 100% Plusskunder penetration: {df.shape[0]}")

    # Rule 3
    net_grid_export_limit = 6
    df = df.filter(pl.col('net_export_max')>net_grid_export_limit)
    df = df.sort(by='ami_prod_cnt', descending=True)
    print(f"Number of neighborhoods with net export exceeding {net_grid_export_limit} AMI's: {df.shape[0]}")

    # Rule 5
    plusskunder_ratio = 5.1
    df = df.filter(pl.col('plusk_ratio')>plusskunder_ratio)
    print(f"Number of neighborhoods with Plusskunder ration to grid size exceeding {plusskunder_ratio}% AMI's: {df.shape[0]}")

    print_df(df=df, sort_by='ami_cnt', n=100)
    print('done')


def reduction_path_2(df):

    n_unique = df.n_unique(subset='topology')
    print(f"Number of neighborhoods: {n_unique}")

    # Rule 1
    df = df.filter(pl.col('ami_prod_cnt')>0)
    print(f"Number of neighborhoods with Plusskunder: {df.shape[0]}")

    # Rule 2
    nb_ami_lb = 20
    df = df.filter(pl.col('ami_cnt')>nb_ami_lb)
    n_unique = df.n_unique(subset='topology')
    print(f"Number of neighborhoods with minimum of {nb_ami_lb} AMI's: {n_unique}")

    # Rule 6
    sf_c_lb = 10
    df_r6 = df.with_columns((pl.col('net_nb_self_consumption_kwh_max_1h_agg')/pl.col('ami_prod_cnt')).alias('sc_f')).sort('sc_f', descending=True)
    df_r6 = df_r6.filter(pl.col('sc_f')>sf_c_lb).drop('sc_f')
    n_unique = df_r6.n_unique(subset='topology')
    print(f"Number of neighborhoods that has self-consumption production exceeding {sf_c_lb}kWh : {n_unique}")

    # Rule 7
    df_r7 = df.filter(pl.col('net_nb_export_kwh_max_1h_agg')>0)
    n_unique = df_r7.n_unique(subset='topology')
    print(f"Number of neighborhoods with positive net export : {n_unique}")

    df = pl.concat([df_r6,df_r7])
    n_unique = df.n_unique(subset='topology')
    print(f"Unique topologies for neighborhoods on r6 and r7 : {n_unique}")

if __name__ == "__main__":
    # load features table
    df = pl.read_parquet(os.path.join(path,'features'))
    reduction_path_1(df)
    reduction_path_2(df)


    #args = {'r1_lb':0, 'r2_lb':20, 'r3_lb':1, 'r4_lb':10, 'r5_lb':10}
    #scenario(df=df, scenario_args=args)





