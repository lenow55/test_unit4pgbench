import argparse
import logging
import sys
from kubernetes.client import Configuration, api_client
from kubernetes import client, config
from urllib3 import Retry
from runner import RunnerDaemon

from settings import EnvironmentOption, RunSettings

logger = logging.getLogger(__name__)
fmt = logging.Formatter(
    fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
shell_handler = logging.StreamHandler(sys.stdout)
shell_handler.setLevel(logging.DEBUG)
shell_handler.setFormatter(fmt)
logger.addHandler(shell_handler)
logger.setLevel(logging.DEBUG)


def main():
    settings = RunSettings()
    if settings.environment == EnvironmentOption.DEBUG:
        try:
            config.load_config(config_file="./admin.conf")
        except config.ConfigException:
            config.load_config()
    else:
        config.load_config()
    client_config = Configuration.get_default_copy()
    client_config.retries = Retry(
        total=10, connect=4, read=5, other=4, backoff_factor=1
    )
    # client_config.logger_stream_handler = shell_handler
    if settings.environment == EnvironmentOption.DEBUG:
        client_config.debug = True

    api_client_obj = api_client.ApiClient(configuration=client_config)

    v1 = client.CustomObjectsApi(api_client_obj)

    runner = RunnerDaemon(api=v1, config=settings)
    runner.init_runner()
    runner.start()
    try:
        runner.join()
    except KeyboardInterrupt:
        logger.info("Stop programm")

    return 0


if __name__ == "__main__":
    main()
