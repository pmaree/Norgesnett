import requests, os
import polars as pl

PATH = os.path.dirname(__file__)

if __name__ == "__main__":
    from_time = '2023-03-01'
    to_time = '2023-09-02'
    file_path = os.path.join(PATH,f'../data/raw/price/euro_2_nok_valuta_{from_time}_{to_time}')

    url = f'https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP?format=sdmx-json&startPeriod={from_time}&endPeriod={to_time}&locale=no'
    response = requests.get(url)
    if response.status_code == 200:
        # get json payload
        data_json = response.json()

        # parse interesting fields
        values = [float(value[0])  for value in data_json['data']['dataSets'][0]['series']['0:0:0:0']['observations'].values()]
        dates = data_json['data']['structure']['dimensions']['observation'][0]['values']

        # transform to polars dataframe
        df = (pl.DataFrame(dates).with_columns(nok_euro=pl.lit(pl.Series(values))).with_columns(
            pl.col('start').str.to_datetime('%Y-%m-%dT%H:%M:%S').alias('timestamp'),
            pl.col('end').str.to_datetime('%Y-%m-%dT%H:%M:%S'),
        ).select(['timestamp','nok_euro']).sort(by='timestamp')
            .upsample(time_column='timestamp',every='1h').fill_null(strategy="forward").write_parquet(file_path))