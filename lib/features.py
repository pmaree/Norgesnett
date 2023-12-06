from datetime import datetime
import polars as pl
import os, time, requests

from lib.etl import etl_bronze_to_silver
from lib import Logging

PATH = os.path.dirname(__file__)
log = Logging()


def lat_long_to_area_api(latitude: float, longitude: float) -> dict:
    try:
        url = 'https://www.ladeassistent.no/api/price-area'
        headers = {'Content-Type': 'application/json'}
        payload = {'latitude': latitude, 'longitude': longitude}
        response = requests.post(url, headers=headers, json=payload)
        area = response.json()['priceArea']
        return 'NO1' if area is None else area
    except requests.exceptions.RequestException as e:
        print(f"Exception raised in lat_long_to_area_api: {e}")


def preprocess():

    # config for features
    verbose = False
    date_from='2023-03-01T00:00:00'
    date_to='2023-09-01T01:00:00'
    src_path = PATH + f"/../data/bronze/measurements"
    dst_path = PATH + f"/../data/bronze/features"
    usage_points_path = PATH + f"/../data/bronze/usagepoints/2023-11-22"

    # read usagepoints files
    df_usage_points = pl.read_parquet(usage_points_path)

    # build feature table per neighborhood
    t0 = time.time()
    file_list = os.listdir(src_path)
    df_features = pl.DataFrame()
    for index, file_name in enumerate(file_list):

        topology = file_name.split(sep='_2023')[0]
        log.info(f"[{datetime.now().isoformat()}] Compile production feature list for topology {topology} being {index} of {len(file_list)}")

        t0 = time.time()
        df = etl_bronze_to_silver(topology=topology, date_from=date_from, date_to=date_to)

        def get_topology(df: pl.DataFrame)->pl.Utf8:
            return df.select(pl.col('topology').first()).item()
        
        def get_coordinate(df: pl.DataFrame, df_coord: pl.DataFrame)->pl.Utf8:
            latitude = df_coord.filter(pl.col('topology')==get_topology(df)).select(pl.col('latitude').first()).item()
            longitude = df_coord.filter(pl.col('topology')==get_topology(df)).select(pl.col('longitude').first()).item()
            return latitude, longitude
        
    
        def get_data_from(df: pl.DataFrame)->pl.Utf8:
            return df.select('fromTime').min().item().isoformat()

        def get_data_to(df: pl.DataFrame)->pl.Utf8:
            return df.select('toTime').max().item().isoformat()

        def get_sample_cnt(df: pl.DataFrame)->pl.Int64:
            return df.shape[0]

        def get_ami_cnt(df: pl.DataFrame)->pl.Int64:
            return df.n_unique('meteringPointId')

        def get_ami_load_cnt(df: pl.DataFrame)->pl.Int64:
            return df.filter(pl.col('p_load_kwh')>0).n_unique(subset='meteringPointId')

        def get_ami_prod_cnt(df: pl.DataFrame)->pl.Int64:
            return df.filter(pl.col('p_prod_kwh')>0).n_unique(subset='meteringPointId')

        def get_plusskunder_ratio(df: pl.DataFrame)->pl.Float64:
            return round(get_ami_prod_cnt(df)/max(1,get_ami_load_cnt(df))*100,1)

        def get_p_load_max(df: pl.DataFrame)->pl.Float64:
            load_max = df.filter(pl.col('p_load_kwh')>0).select(pl.col('p_load_kwh')).max().item()
            return float() if  load_max is None else round(load_max,1)

        def get_p_prod_max(df: pl.DataFrame)->pl.Float64:
            prod_max =df.filter(pl.col('p_prod_kwh')>0).select(pl.col('p_prod_kwh')).max().item()
            return float() if  prod_max is None else round(prod_max,1)

        def get_net_export_max(df: pl.DataFrame)->pl.Float64:
            export_max = df.select((pl.col('p_prod_kwh')-pl.col('p_load_kwh'))).max().item()
            return float() if export_max is None else round(export_max,1)

        def get_net_export_min(df: pl.DataFrame)->pl.Float64:
            export_min = round(df.select((pl.col('p_prod_kwh')-pl.col('p_load_kwh'))).min().item(),1)
            return float() if export_min is None else round(export_min,1)

        def get_nb_agg_features(df:pl.DataFrame, every: str='1h'):

            df = df.with_columns((pl.col('p_prod_kwh')-pl.col('p_load_kwh')).alias('p_pros_kwh')) # Get net export for each AMI at each time
            df_ = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every=every) # Group all AMI's to same time for neighborhood

            df_ = df_.agg((pl.col('p_pros_kwh').sum()).alias('p_nb_pros_kwh'),
                         (pl.col('p_prod_kwh').sum()).alias('p_nb_prod_kwh'),
                         (pl.col('p_load_kwh').sum()).alias('p_nb_load_kwh'))

            df_ = df_.select(pl.all(),
                      pl.when(pl.col('p_nb_pros_kwh')>0)
                      .then(pl.col('p_nb_pros_kwh'))
                      .otherwise(pl.lit(0))
                      .alias('net_nb_export_kwh'))
            df_ = df_.select(pl.all(),
                       pl.when(pl.col('p_nb_pros_kwh')>0)
                       .then(pl.col('p_nb_prod_kwh')-pl.col('p_nb_pros_kwh'))
                       .otherwise(pl.col('p_nb_prod_kwh'))
                       .alias('net_nb_self_consumption_kwh'))

                    # average aggregated production versus consumption for neighborhood
            return {f"nb_pros_avg": round(df_.select('p_nb_pros_kwh').mean().item(),1),
                    # maximum aggregated production for neighborhood
                    f"nb_prod_max": round(df_.select('p_nb_prod_kwh').max().item(),1),
                    # maximum aggregated consumption for neighborhood
                    f"nb_load_max": round(df_.select('p_nb_load_kwh').max().item(),1),
                    # maximum aggregated net grid export for neighborhood
                    f"nb_ex_max": round(df_.select('net_nb_export_kwh').max().item(),1),
                    # maximum aggregated self consumption for grid
                    f"nb_sc_max": round(df_.select('net_nb_self_consumption_kwh').max().item(),1)}


        def get_agg_duckcurve_profiles(df:pl.DataFrame):

            # group AMI's for neighborhood over {every} and solve for total of group
            df = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1h') \
                .agg(pl.col('p_load_kwh').sum().alias(f"nb_load"),
                     pl.col('p_prod_kwh').sum().alias(f"nb_prod")) \
                .with_columns((pl.col('fromTime').map_elements(lambda datetime: datetime.hour)).alias('hour'))

            # group by the 1h over entry nb and solve for average in the aggregated interval
            df_=df.group_by(by='hour').agg(pl.col(f"nb_load").max().alias(f"nb_load_max"),
                                           pl.col(f"nb_prod").max().alias(f"nb_prod_max")
                                           ).sort(by='hour')

            # get the maximum load hour and also the value
            idx = df_.select(pl.col('nb_load_max')).to_series().arg_max()
            load_max_time =df_.select(pl.col('hour'))[idx].item()
            load_max_val = df_.select(pl.col('nb_load_max'))[idx].item()
            load_avg_val = df_.select(pl.col('nb_load_max')).mean().item()

            # get the maximum prod hour and also the value
            try:
                if df_.filter(pl.col('nb_prod_max')>0).shape[0]:
                    idx = df_.select(pl.col('nb_prod_max')).to_series().arg_max()
                    prod_max_time =df_.select(pl.col('hour'))[idx].item()
                    prod_max_val = df_.select(pl.col('nb_prod_max'))[idx].item()
                else:
                    prod_max_time = 0
                    prod_max_val = 0.0
            except Exception as e:
                print('exception')

            return {'nb_aggmaxl_idx': load_max_time,
                    'nb_aggmaxl_val': load_max_val,
                    'nb_aggavgl_val': load_avg_val,
                    'nb_aggmaxp_idx': prod_max_time,
                    'nb_aggmaxp_val': prod_max_val}

        # compile features list
        aggregate_every = '1h'
        df_feature = pl.DataFrame(
            {**{'topology': get_topology(df),
                'date_from': get_data_from(df),
                'date_to': get_data_to(df),
                'sample_cnt':get_sample_cnt(df),
                'ami_cnt': get_ami_cnt(df),
                'ami_load_cnt': get_ami_load_cnt(df),
                'ami_prod_cnt': get_ami_prod_cnt(df),
                'ami_lp_ratio': get_plusskunder_ratio(df),
                'ami_load_max': get_p_load_max(df),
                'ami_prod_max': get_p_prod_max(df),
                'ami_ex_max': get_net_export_max(df),
                'ami_ex_min': get_net_export_min(df)},
               **get_nb_agg_features(df, every=aggregate_every),
               **get_agg_duckcurve_profiles(df)
             })

        # add coordinates and price area
        latitude, longitude = get_coordinate(df, df_usage_points)

        df_feature = df_feature.with_columns(latitude=pl.lit(latitude),
                                longitude=pl.lit(longitude),
                                price_area=pl.lit(lat_long_to_area_api(latitude=latitude, longitude=longitude)))

        if verbose:
            with pl.Config() as cfg:
                cfg.set_tbl_cols(-1)
                cfg.set_tbl_width_chars(1000)
                print(df_feature)

        # stack features
        try:
            df_features = df_feature if df_features.is_empty() else df_features.vstack(df_feature)
        except Exception as e:
            print(e)

    # save features
    dst_file_path = os.path.join(dst_path, "production")
    log.info(f"[{datetime.now().isoformat()}] Completed feature list construction in {time.time()-t0:.2f} seconds. Write file to {dst_file_path}")
    df_features.write_parquet(os.path.join(dst_file_path))


if __name__ == "__main__":
    preprocess()
