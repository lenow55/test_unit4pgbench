import os
from enum import Enum
from typing_extensions import Annotated

from pydantic import Field
from pydantic.functional_validators import BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict

base_dir = "environment"


def convert_str2int(v):
    if isinstance(v, str):
        v = int(v)
    return v


IntMapStr = Annotated[int, BeforeValidator(convert_str2int)]


class EnvironmentOption(Enum):
    DEBUG = "debug"
    PRODUCTION = "production"


# Родительский объект с общими настройками.
class AdvancedBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8", secrets_dir="/var/run/secrets_dir"
    )


class RunSettings(AdvancedBaseSettings):
    environment: EnvironmentOption = Field(default=EnvironmentOption.DEBUG)
    templates_folder: str = Field(default="./data/templates")
    temp_sql_scripts_folder: str = Field(default="./data/temp_sql")
    output_folder: str = Field(default="./data/output")
    combinations_pipeline: str = Field(default="./full_pipeline.json")
    pgpassword: str = Field(default="1234")
    pguser: str = Field(default="app")
    db_name: str = Field(default="app")
    db_rw_host: str = Field(default="cluster-example-rw")
    db_rw_port: IntMapStr = Field(default=5432)
    cluster_name: str = Field(default="cluster-example")
    pgpool_host: str = Field(default="pgpool")
    pgpool_service_port: IntMapStr = Field(default=9999)
    prometheus_host: str = Field(default="prometheus-community-kube-prometheus")
    prometheus_port: IntMapStr = Field(default=9090)
    testing_period: IntMapStr = Field(default=120)  # 5 min
    pgbench_threads: IntMapStr = Field(default=10)  # Количество pgbench потоков

    model_config = SettingsConfigDict(
        # слева на право в порядке приоритетности
        env_file=(
            os.path.join(base_dir, ".env.prod"),
            os.path.join(base_dir, ".env.debug"),
        )
    )
