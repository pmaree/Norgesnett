from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timedelta
import yaml, requests, os, json
from yaml.loader import SafeLoader
from typing import List
import polars as pl
from math import floor, ceil
import os

PATH = os.path.dirname(__file__)

SAMPLES_PER_BATCH_LIMIT = 20000
CFG_YAML_PATH = PATH + "/config.yaml"
CONFIG: dict

with open(CFG_YAML_PATH, "r") as cfg_file:
    CONFIG = yaml.load(cfg_file, Loader=SafeLoader)

class Query(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    ami_id: List[str]
    from_date: datetime
    to_date: datetime
    resolution: int = Field(default=1)
    type: int = Field(default=1)
    is_utc: bool = Field(default=True)


class QueryRes:
    def __init__(self, query: Query, df: pl.DataFrame):
        self.query = query
        self.df = df

    @property
    def name(self) -> str:
        return f"{self.query.from_date}_{self.query.to_date}_R{self.query.resolution}_T{self.query.type}"

    @property
    def dara(self) -> pl.DataFrame:
        return self.df


def batch_iterator(query: Query):

    # static batch size allocation
    ami_cnt = len(query.ami_id)
    samples_per_meter = (query.to_date - query.from_date).total_seconds()/3600
    samples_per_query = samples_per_meter*ami_cnt
    number_of_batches = samples_per_query/SAMPLES_PER_BATCH_LIMIT
    deltatime = timedelta(hours=floor(SAMPLES_PER_BATCH_LIMIT/ami_cnt))
    sampled_delta = round(SAMPLES_PER_BATCH_LIMIT/ami_cnt)

    for batch_i in range(0, ceil(number_of_batches)):
        from_date = query.from_date + batch_i*deltatime
        to_date = min(from_date + timedelta(hours=sampled_delta), query.to_date)
        yield Query(ami_id=query.ami_id, from_date=from_date, to_date=to_date, resolution=query.resolution, type=query.type, is_utc=query.is_utc)


def fetch_bulk(query: Query) -> pl.DataFrame:

    for index, batch_i in enumerate(batch_iterator(query)):

        '''
        response = requests.post(url=CONFIG['host_url'] + 'timeseries/bulkgetvalues',
                                 headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'XApiKey': f"{CONFIG['api_key']}"},
                                 params={'FromDate': batch_i.from_date.isoformat(),
                                         'ToDate': batch_i.to_date.isoformat(),
                                         'Type': batch_i.type,
                                         'Resolution': batch_i.resolution,
                                         'isUtc': batch_i.is_utc},
                                 data=json.dumps({"meteringPointIds": batch_i.ami_id})
                                 )
        '''
        class Response:
            def __init__(self, data):
                self.data = data

            @property
            def status_code(self):
                return 200

            def json(self):
                return json.loads(self.data)

        with open(PATH + '/../data/response.json','r') as fp:
            response = Response(fp.read())


        if response.status_code != 200:
            raise Exception(f"Invalid response from API server with code {response.status_code}")

        df = pl.DataFrame(response.json()).explode('timeseries').unnest('timeseries')
        nan_cnt = (df.null_count().select(pl.all()).sum(axis=1).alias('nan')).item()
        if nan_cnt:
            df = df.drop_nulls()

        yield QueryRes(query=batch_i, df=df)
