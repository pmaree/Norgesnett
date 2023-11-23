import numpy as np
import polars as pl
import os, json

PATH = os.getcwd()
path = f"{PATH}/../../data/bronze/features/"

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

def reduction_path_3(df):

    def print_rule(df, nr):
        print(f"Rule {nr}: Number of neighborhoods = {df.shape[0]}")
        print(f"Rule {nr}: AMI min = {df.select(pl.col('ami_cnt')).min().item()}")
        print(f"Rule {nr}: AMI max = {df.select(pl.col('ami_cnt')).max().item()}")
        print(f"Rule {nr}: nbhd. Agg Prod = {df.select(pl.col('nb_prod_max')).mean().item()}")
        print(f"Rule {nr}: nbhd. agg ex. = {df.select(pl.col('nb_ex_max')).mean().item()}")

    # Rule 1
    df_r1 = df.filter(pl.col('ami_prod_max')>9).filter(pl.col('ami_cnt')>1)
    print_rule(df_r1, 1)

    # Rule 2
    pv_penetration_limit = 0.35
    df_r2 = df.filter(pl.col('nb_prod_max')/pl.col('nb_load_max')>pv_penetration_limit)
    print_rule(df_r2, 2)

    # Rule 3
    peak_load_per_house_kwh = 3
    pv_penetration_limit = 0.35
    PF = 0.95
    DF = 1.4
    df_r3 = df.with_columns((pl.col('ami_cnt')*peak_load_per_house_kwh/PF/DF).alias('peak_tf_load_kwh'))\
        .filter(pl.col('nb_prod_max')/pl.col('peak_tf_load_kwh')>pv_penetration_limit)\
        .drop('peak_tf_load_kwh')
    print_rule(df_r3, 3)

    df_ = pl.concat([df_r1,df_r2,df_r3])
    print(f"Rule 1-3 identified {df_.n_unique('topology')} topologies from {df_.shape[0]}")
    return df_.unique(subset='topology')

def reduction_path_4(df):
    topologies = {'093':'S_1557714_T_1557719',
                  '273':'S_1134740_T_1192382',
                  '365H': 'S_1134814_T_1134470',
                  '56': 'S_1134691_T_1192378',
                  '65H':'S_17279_T_1513'}
    df_ = pl.DataFrame(schema=df.schema)
    for key, value in topologies.items():
        df_ = pl.concat([df_, df.filter(pl.col('topology')==value)])
    print_df(df_)
    return df_

def reduction_path_5(df):
    df = df.filter(pl.col('ami_load_cnt')>0 ).filter(pl.col('ami_prod_cnt')>0)


    limit_r1 = 12 # limit on the produciton range peak for neighborhood
    limit_r2 = 0.8 # persentage how peak loads (ducks head) are over ducks back
    limit_r3 = 1 # the misalignment of peaks in production and consumption does not help!

    df_r1 = df.filter(pl.col('nb_aggmaxp_val')>limit_r1)
    print(f"[reduction_path_5]: Rule 1 -> {df_r1.n_unique('topology')}")

    df_r2 = df.filter((pl.col('nb_aggmaxl_val')-pl.col('nb_aggavgl_val'))/pl.col('nb_aggavgl_val')>limit_r2)
    print(f"[reduction_path_5]: Rule 2 -> {df_r2.n_unique('topology')}")

    df.with_columns()
    df_r3 = df.with_columns( ((pl.col('nb_aggmaxl_idx')-pl.col('nb_aggmaxp_idx')).abs().map_elements(lambda dt: np.power(np.e,dt/24)).alias('pow_e_dt')))
    df_r3 = df_r3.with_columns( ((pl.col('nb_aggmaxp_val')/pl.col('nb_aggmaxl_val'))*pl.col('pow_e_dt')).alias('rule_3'))
    df_r3 = df_r3.filter( (pl.col('nb_aggmaxp_val')/pl.col('nb_aggmaxl_val'))*pl.col('pow_e_dt') > limit_r3 ).drop(columns=['pow_e_dt','rule_3'])
    print(f"[reduction_path_5]: Rule 3 -> {df_r3.n_unique('topology')}")

    df_ = pl.concat([df_r1,df_r2,df_r3])
    print(f"[reduction_path_5]: Rule 1-3 identified {df_.n_unique('topology')} topologies from {df_.shape[0]}")
    return df_.unique(subset='topology')

def assoc_ami_list(df):
    path = f"{PATH}/../../data/silver/"
    topology_family = {}
    for topology in df.select(pl.col('topology')).to_series().to_list():
        df_ = pl.read_parquet(os.path.join(path, topology))
        topology_family[topology] = df_.unique(subset='meteringPointId').select('meteringPointId').to_series().to_list()
    return topology_family


if __name__ == "__main__":
    # load features table
    df = pl.read_parquet(os.path.join(path,'production'))

    df1 = reduction_path_3(df)

    # numerical reduction method based on AMS data
    df3 = reduction_path_3(df)

    # client input on neighborhoods with issues
    df4 =reduction_path_4(df)

    # kill the duck curve
    df5 = reduction_path_5(df)

    # shortlisted neighborhoods
    df_ = pl.concat([df5,df4]).unique(subset='topology')
    df_a = pl.concat([df3,df4]).unique(subset='topology')

    # get associated ami list to each topology
    topology_family = assoc_ami_list(df_)
    with open('../data/topology_shortlisted.json', 'a+') as fp:
        fp.write(json.dumps(topology_family))

    #df_.select(pl.col('topology')).write_json('shortlisted_topologies')







