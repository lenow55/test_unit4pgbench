import requests

from prometheus_models import PromResponse


def check_replication(host: str, port: int, timeout: float = 5.0) -> bool:
    url = f"http://{host}:{port}/api/v1/query"
    params = {"query": "cnpg_pg_replication_lag"}
    response = requests.get(url=url, params=params, timeout=timeout)
    prom_resp = PromResponse(**response.json())
    for result in prom_resp.data.result:
        value = result.value[1]
        if value != "0":
            return False

    return True
