from pydantic import BaseModel
from typing import List


class Metric(BaseModel):
    __name__: str
    container: str
    endpoint: str
    instance: str
    job: str
    namespace: str
    pod: str


class Result(BaseModel):
    metric: Metric
    value: List[float | str]


class Data(BaseModel):
    result: List[Result]
    resultType: str


class PromResponse(BaseModel):
    status: str
    data: Data
