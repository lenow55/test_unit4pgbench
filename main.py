import argparse
import logging
import sys
from kubernetes.client import Configuration, api_client
from kubernetes import client, config
from urllib3 import Retry
from runner import RunnerDaemon
from time import sleep

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


# def main(args):
def main():
    config.load_kube_config(config_file="./admin.conf")
    client_config = Configuration.get_default_copy()
    client_config.retries = Retry(
        total=10, connect=4, read=5, other=4, backoff_factor=1
    )
    # client_config.logger_stream_handler = shell_handler
    client_config.debug = True

    api_client_obj = api_client.ApiClient(configuration=client_config)

    v1 = client.CustomObjectsApi(api_client_obj)
    runner = RunnerDaemon(api=v1)
    runner.init_runner()
    runner.start()
    try:
        runner.join()
    except KeyboardInterrupt:
        logger.info("Stop programm")

    return 0


def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument("-q", "--queue", choices=QUEUE_TYPES, default="fifo")
    # parser.add_argument("-p", "--producers", type=int, default=3)
    # parser.add_argument("-c", "--consumers", type=int, default=2)
    # parser.add_argument("-ps", "--producer-speed", type=int, default=1)
    # parser.add_argument("-cs", "--consumer-speed", type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    # main(parse_args())
    main()
