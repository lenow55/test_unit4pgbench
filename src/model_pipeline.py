from pydantic import BaseModel, Field
from typing import List


class Query(BaseModel):
    query: str
    analyze: str


class ExplainedQuery(Query):
    explain: str = Field(default="")
    explain_file: str = Field(default="")


class QueriesTmpl(BaseModel):
    queries: List[Query]


class QueriesOut(BaseModel):
    queries: List[ExplainedQuery]


class TypeQuery(BaseModel):
    percent: List[int]
    query: int


class InputTmpl(QueriesTmpl):
    read: TypeQuery
    write: TypeQuery
    connections: List[int]
    size: List[int]
    template: str


class CombinationsSettingsBase(BaseModel):
    size: int
    connections: int
    read_percent: int
    write_percent: int
    read_query_id: int
    write_query_id: int
    template: str


class CombinationsSettings(CombinationsSettingsBase, QueriesTmpl):
    pass


class IndexedCombinations(CombinationsSettings):
    index: int
    pgpool: bool


class CombinationsSettingsOut(CombinationsSettingsBase, QueriesOut):
    pass


class IndexedCombinationsOut(CombinationsSettingsOut):
    index: int
    pgpool: bool
    tps: float
    process_file: str = Field(default="")
