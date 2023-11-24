import polars as pl
import os, requests

from lib import Logging


PATH = os.path.dirname(__file__)
log = Logging()


def lat_long_to_area_api(lat: float, lon: float) -> str:
    area = None
    try:
        url = 'https://www.ladeassistent.no/api/price-area'
        headers = {'Content-Type': 'application/json'}
        payload = {'latitude': lat, 'longitude': lon}
        response = requests.post(url, headers=headers, json=payload)
        area = response.json()['priceArea']
        log.info(f"Price area {area} for lat,lon={lat, lon}")
    except requests.exceptions.RequestException as e:
        log.exception(f"Exception raised in lat_long_to_area_api: {e}")
    return '' if area is None else area


src_path = PATH + '/../../data/raw/coordinates/usagepoints.csv'
dst_path = PATH + '/../../data/bronze/coordinates/usagepoints'

df = pl.read_csv(src_path)
df = (df.with_columns(pl.struct(['latitude','longitude']).map_elements(lambda x: lat_long_to_area_api(lat=x['latitude'], lon=x['longitude'])).alias('price_area')))
df.write_parquet(dst_path)

