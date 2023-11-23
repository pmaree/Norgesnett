from requests import Request
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import requests, json
from dotenv import load_dotenv
from typing import List
import polars as pl
from math import floor, ceil
from requests.adapters import HTTPAdapter, Retry
import os

from lib import Logging

log = Logging()

PATH = os.path.dirname(__file__)

SAMPLES_PER_BATCH_LIMIT =100000

class Query(BaseModel):
    topology: str = Field(default='')
    ami_id: List[str]
    from_date: datetime
    to_date: datetime
    resolution: int = Field(default=1)
    type: int = Field(default=1)
    is_utc: bool = Field(default=True)

    @property
    def name(self) -> str:
        return f"{self.from_date}_{self.to_date}_R{self.resolution}_T{self.type}"


class Timeseries(BaseModel):
    fromTime: str
    toTime: str
    value: float
    unit: str
    status: bool


class ValueResponse(BaseModel):
    meteringPointId: str = Field(default='')
    type: int
    timeseries: List[Timeseries]


class BulkResponse(BaseModel):
    data: List[ValueResponse]

    @property
    def to_polars(self) -> pl.DataFrame:
        df = pl.DataFrame(self.model_dump()['data'])

        # check for any possible nulls
        nan_cnt = (df.null_count().select(pl.all()).sum(axis=1).alias('nan')).item()
        if nan_cnt:
            df = df.drop_nulls()

        # flag and remove empty data series
        df = df.with_columns(pl.col('timeseries').apply(lambda x: len(x)).alias('length')).filter(pl.col('length')>0)

        # raise exception if dataframe is empty
        if df.shape[0]:
            df = df.explode('timeseries').unnest('timeseries')
        else:
            raise Exception(f"no measurements are available for topology request")

        return df


class QueryRes:
    def __init__(self, query: Query, df: pl.DataFrame):
        self.query = query
        self.df = df

    @property
    def name(self) -> str:
        return f"{self.query.from_date}_{self.query.to_date}_R{self.query.resolution}_T{self.query.type}"

    @property
    def sample_cnt(self) -> int:
        return self.df.shape[0]


def batch_iterator(query: Query):

    # static batch size allocation
    ami_cnt = len(query.ami_id)
    samples_per_meter = (query.to_date - query.from_date).total_seconds()/3600
    samples_per_query = samples_per_meter*ami_cnt
    number_of_batches = samples_per_query/SAMPLES_PER_BATCH_LIMIT
    batch_timedelta = timedelta(hours=floor(SAMPLES_PER_BATCH_LIMIT/ami_cnt))
    sampled_delta = round(SAMPLES_PER_BATCH_LIMIT/ami_cnt)

    for batch_i in range(0, ceil(number_of_batches)):
        from_date = query.from_date + batch_i*batch_timedelta
        to_date = min(from_date + timedelta(hours=sampled_delta), query.to_date)
        yield Query(topology=query.topology, ami_id=query.ami_id, from_date=from_date, to_date=to_date, resolution=query.resolution, type=query.type, is_utc=query.is_utc)


def fetch_bulk(query: Query) -> QueryRes:
    load_dotenv()
    with requests.Session() as s:

        # retry strategy
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[400, 401, 500], allowed_methods=frozenset(['GET', 'POST']))
        s.mount('https://', HTTPAdapter(max_retries=retries))

        for index, batch_i in enumerate(batch_iterator(query)):

            try:
                # prepare request
                url = os.getenv('HOST_URL')  + 'eNabo/v1/timeseries/bulkgetvalues'
                data = json.dumps({"meteringPointIds": batch_i.ami_id})
                headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'XApiKey': f"{os.getenv('NORGESNETT_API_KEY')}"}
                params={'FromDate': batch_i.from_date.isoformat(),
                        'ToDate': batch_i.to_date.isoformat(),
                        'Type': batch_i.type,
                        'Resolution': batch_i.resolution,
                        'isUtc': batch_i.is_utc}

                # execute query
                req = Request('POST', url=url, data=data, headers=headers, params=params)
                prepped = req.prepare()
                response = s.send(prepped, timeout=1000)
            except Exception as e:
                raise Exception(f"[{datetime.utcnow()}] Failed in the API request with error code {e}. Session will be re-initiated after a 30 minute sleep.")

            if response.status_code == 200:
                # parse response data as polars dataframe
                try:
                    df = BulkResponse(**{'data':response.json()}).to_polars
                    yield QueryRes(query=batch_i, df=df)
                except Exception as e:
                    log.exception(f"[{datetime.utcnow()}] {batch_i.topology} abort parquet write for batch <{batch_i.name}>: {e}")
            else:
                log.warning(f"[{datetime.utcnow()}] {batch_i.topology} received invalid API response {response.status_code} for <{batch_i.name}>")
