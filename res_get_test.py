from queue import Queue
from threading import Condition
from kubernetes import client, config
from pprint import pprint
from kubernetes.client import Configuration, api_client
from urllib3 import Retry
import time
import logging
import sys
import functools

from utils import K8sObject
from cache_single_object import ObjectCache
from utils import EventResource, SourceEvent

logger = logging.getLogger()
fmt = logging.Formatter(
    fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
shell_handler = logging.StreamHandler(sys.stdout)
shell_handler.setLevel(logging.DEBUG)
shell_handler.setFormatter(fmt)
logger.addHandler(shell_handler)
logger.setLevel(logging.DEBUG)


# Configs can be set in Configuration class directly or using helper utility
config.load_kube_config(config_file="./admin.conf")
client_config = Configuration.get_default_copy()
client_config.retries = Retry(total=10, connect=4, read=5, other=4, backoff_factor=1)
# client_config.logger_stream_handler = shell_handler
client_config.debug = True

api_client_obj = api_client.ApiClient(configuration=client_config)

v1 = client.CustomObjectsApi(api_client_obj)

group = "postgresql.cnpg.io"  # str | the custom resource's group
version = "v1"  # str | the custom resource's version
namespace = "default"  # str | The custom resource's namespace
plural = "clusters"  # str | the custom resource's plural name. For TPRs this would be lowercase plural kind.
name = "cluster-testing"  # str | the custom object's name


count = 1
# TODO: эта штука внутри себя инициализирует ещё один клиент и по какой-то причине
# ломает параметры конфигурации, которую я сам ставил
# новый клиент снова инициализирует дефолтную конфигурацию без ретраев
# да, она как-то умудряется перезаписать мой собственный конфиг

buffer: Queue = Queue()
condition: Condition = Condition()

cluster_func = functools.partial(
    v1.list_namespaced_custom_object,
    group="postgresql.cnpg.io",  # str | the custom resource's group
    version="v1",  # str | the custom resource's version
    namespace="default",  # str | The custom resource's namespace
    plural="clusters",  # str | the custom resource's plural name. For TPRs this would be lowercase plural kind.
)
pgpool_func = functools.partial(
    v1.list_namespaced_custom_object,
    group="apps",  # str | the custom resource's group
    version="v1",  # str | the custom resource's version
    namespace="default",  # str | The custom resource's namespace
    plural="deployments",  # str | the custom resource's plural name. For TPRs this would be lowercase plural kind.
)
cluster_cache: ObjectCache = ObjectCache(
    cluster_func,
    buffer,
    condition,
    name="cluster-testing",
    souce_type=SourceEvent.BD_CLUSTER,
)
pgpool_cache: ObjectCache = ObjectCache(
    pgpool_func, buffer, condition, name="pgpool", souce_type=SourceEvent.PGPOOL
)


def wait_caches(stop_time: float) -> None:
    while not (cluster_cache.is_ready() and pgpool_cache.is_ready()):
        timeout = stop_time - time.time()
        if timeout <= 0:
            raise Exception("Exceeded retry deadline")
        condition.wait(timeout)


cluster = None
pgpool = None
stop_time = time.time() + 3000000.0
try:
    with condition:
        wait_caches(stop_time=stop_time)
        cluster = cluster_cache.get()
        pgpool = pgpool_cache.get()
except Exception:
    logger.exception("get_cluster")
    raise Exception("Kubernetes API is not responding properly")

# pprint(cluster)
# pprint(pgpool)

if not isinstance(cluster, K8sObject):
    raise Exception("Kubernetes API is not responding properly")

instance_statuses = cluster.status.instancesStatus
instance_names = cluster.status.instanceNames
instances_reported_state = cluster.status.instancesReportedState
logger.debug(f"{instance_statuses}, {instance_names}, {instances_reported_state}")

if instance_names != instance_statuses.healthy:
    logger.warn("Cluster in unstable state")
else:
    logger.debug("Cluster ready to use")

if not isinstance(pgpool, K8sObject):
    raise Exception("PgPool not in kubernetes cluster")

pgpool_replicas = pgpool.status.replicas
pgpool_ready_replicas = pgpool.status.readyReplicas

if pgpool_replicas != pgpool_ready_replicas:
    logger.warn("Pgpool in unstable state")
else:
    logger.debug("Pgpool ready to use")

list_ids = [1, 2, 3]

while True:
    try:
        event = buffer.get()
        logger.debug("Event cause: ")
        if isinstance(event, EventResource):
            print(event.type.name, event.source.name)
            pprint(event.status_object)
            if event.source == SourceEvent.BD_CLUSTER:
                new_statuses = event.status_object.instancesStatus
                for inst_name in instance_names:
                    if inst_name not in new_statuses.healthy:
                        logger.error(f"Instance fail: {inst_name}")
            if event.source == SourceEvent.PGPOOL:
                new_ready_replicas = event.status_object.get("readyReplicas")
                if pgpool_replicas != new_ready_replicas:
                    logger.error(f"Pgpool replica fails {event.status_object}")
        buffer.task_done()
    except KeyboardInterrupt:
        break
#
# print("Event: %s %s" % (event["type"], event["object"]["metadata"]["name"]))
# count -= 1
# if not count:
#     w.stop()
#
# obj = v1.get_namespaced_custom_object(group, version, namespace, plural, name)
# pprint(obj)
print("Ended.")
