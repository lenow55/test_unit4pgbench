from pydantic import BaseModel
from typing import List


class Query(BaseModel):
    query: str
    analyze: str


class TypeQuery(BaseModel):
    percent: List[int]
    query: int


class QueriesTmpl(BaseModel):
    queries: List[Query]
    template: str


class InputTmpl(QueriesTmpl):
    read: TypeQuery
    write: TypeQuery
    connections: List[int]
    size: List[int]


class CombinationsSettings(QueriesTmpl):
    size: int
    connections: int
    read_percent: int
    write_percent: int
    read_query_id: int
    write_query_id: int


class IndexedCombinations(CombinationsSettings):
    index: int
    pgpool: bool
