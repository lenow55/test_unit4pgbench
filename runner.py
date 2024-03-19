from queue import Empty, Queue
from enum import IntEnum
from pprint import pprint
from typing import Tuple
from kubernetes.client import CustomObjectsApi
import time
import logging
import sys
import functools

from utils import K8sObject
from cache_single_object import ObjectCache
from utils import EventResource, SourceEvent
from threading import Condition, Thread

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


class ModeWork(IntEnum):
    SINGLE = 0
    PGPOOL = 1


class RunnerDaemon(Thread):
    def __init__(
        self,
        api: CustomObjectsApi,
    ) -> None:
        super(RunnerDaemon, self).__init__()
        self.daemon = True
        self._is_inited = False
        self._api_client = api
        self._buffer: Queue = Queue()
        self._condition: Condition = Condition()
        self._cluster_func = functools.partial(
            self._api_client.list_namespaced_custom_object,
            group="postgresql.cnpg.io",  # str | the custom resource's group
            version="v1",  # str | the custom resource's version
            namespace="default",  # str | The custom resource's namespace
            plural="clusters",  # str | the custom resource's plural name. For TPRs this would be lowercase plural kind.
        )
        self._pgpool_func = functools.partial(
            self._api_client.list_namespaced_custom_object,
            group="apps",  # str | the custom resource's group
            version="v1",  # str | the custom resource's version
            namespace="default",  # str | The custom resource's namespace
            plural="deployments",  # str | the custom resource's plural name. For TPRs this would be lowercase plural kind.
        )
        self._cluster_cache: ObjectCache = ObjectCache(
            self._cluster_func,
            self._buffer,
            self._condition,
            name="cluster-testing",
            souce_type=SourceEvent.BD_CLUSTER,
        )
        self._pgpool_cache: ObjectCache = ObjectCache(
            self._pgpool_func,
            self._buffer,
            self._condition,
            name="pgpool",
            souce_type=SourceEvent.PGPOOL,
        )

    def start(self):
        if not self._is_inited:
            raise ValueError("Not correct init")
        super().start()

    def init_runner(self):
        logger.debug("Init complete")
        self._mode = ModeWork.SINGLE
        self._is_inited = True

    def run(self) -> None:
        result = True
        self._wait_resource_bee_healthy()
        while True:
            if result:
                result = False
            try:
                event = self._buffer.get()
                logger.debug("Event cause: ")
                if isinstance(event, EventResource):
                    result = self._process_event(event)
                self._buffer.task_done()
            except KeyboardInterrupt:
                break
            except Exception as e:
                raise e

    def run_command_with_watch(self, command) -> Tuple[bool, int, str, str]:
        pool_interval = 10.0
        processing_flag = True
        logger.debug(f"Create process with command: {command}")
        while processing_flag:
            try:
                event = self._buffer.get(timeout=pool_interval)
                if isinstance(event, EventResource):
                    if self._process_event(event):
                        # TODO: переопределить свой класс исключения, чтобы отлавливать эту ситуацию
                        raise Exception("Resources broken")
                self._buffer.task_done()
            except Empty:
                logger.debug("Check process alive")
                results = "results"
                processing_flag = False
                return True, 0, results, ""
            except Exception as e:
                raise e

        return True, 1, "", ""

    def run_command(self, command) -> Tuple[int, str, str]:
        self._load_resources_statuses()
        if self._check_healthy():
            logger.info("Run command without watch\n" f"command: {command}")
            return 0, "", ""
        return 1, "", ""

    def _process_cluster_event(self, event: EventResource) -> bool:
        if not isinstance(self._cluster_obj, K8sObject):
            raise Exception("Cluster not inited")
        instance_names = self._cluster_obj.status.instanceNames
        instances_reported_state = self._cluster_obj.status.instancesReportedState
        new_statuses = event.status_object.instancesStatus
        new_rep_state = event.status_object.instancesReportedState
        if self._mode == ModeWork.PGPOOL:
            for inst_name in instance_names:
                if inst_name not in new_statuses.healthy:
                    logger.error(f"Instance fail: {inst_name}")
                    return True

        if self._mode == ModeWork.SINGLE:
            # Может выбрасывать исключения, когда после failover
            # добавляется реплика
            for name, value in instances_reported_state.to_dict().items():
                current_primary = value.isPrimary
                instance_state = new_rep_state.get(name)

                if isinstance(instance_state, K8sObject):
                    if current_primary and instance_state.isPrimary:
                        return False
            logger.error(f"Master failed {instances_reported_state} to {new_rep_state}")
            return True

        return False

    def _process_pgpool_event(self, event: EventResource) -> bool:
        pgpool_replicas = event.status_object.get("replicas")
        ready_replicas = event.status_object.get("readyReplicas")
        if pgpool_replicas != ready_replicas or pgpool_replicas is None:
            logger.error(f"Pgpool replica fails {event.status_object}")
            if self._mode == ModeWork.PGPOOL:
                return True

        return False

    def _process_event(self, event: EventResource) -> bool:
        """
        Функция обрабатывает событие с очереди и смотрит на ресурсы,
        которые мы должны использовать

        Args:
            event (EventResource): объект события

        Returns:
            bool: True - ресурсы повреждены, False - продолжаем работу
        """
        result = False
        if event.source == SourceEvent.BD_CLUSTER:
            result = self._process_cluster_event(event)
        if event.source == SourceEvent.PGPOOL:
            result = self._process_pgpool_event(event)

        return result

    def _load_resources_statuses(self):
        stop_time = time.time() + 3000000.0
        try:
            with self._condition:
                self._wait_caches(stop_time=stop_time)
                if self._mode == ModeWork.SINGLE:
                    self._cluster_obj = self._cluster_cache.get()
                if self._mode == ModeWork.PGPOOL:
                    self._pgpool_obj = self._pgpool_cache.get()
        except Exception:
            logger.exception("get_cluster")
            raise Exception("Kubernetes API is not responding properly")
        logger.debug("loaded resources")

    def _wait_caches(self, stop_time: float) -> None:
        if self._mode == ModeWork.SINGLE:
            while not self._cluster_cache.is_ready():
                timeout = stop_time - time.time()
                if timeout <= 0:
                    raise Exception("Exceeded retry deadline")
                self._condition.wait(timeout)
        if self._mode == ModeWork.PGPOOL:
            while not (
                self._cluster_cache.is_ready() and self._pgpool_cache.is_ready()
            ):
                timeout = stop_time - time.time()
                if timeout <= 0:
                    raise Exception("Exceeded retry deadline")
                self._condition.wait(timeout)

    def _wait_resource_bee_healthy(self):
        logger.debug("Wait resources bee healthy")
        self._load_resources_statuses()
        start_timeout = 60.0
        float_timeout = start_timeout
        stop_time = time.time() + start_timeout
        while True:
            logger.debug("start for")
            try:
                event = self._buffer.get(timeout=float_timeout)
                if isinstance(event, EventResource):
                    if (
                        self._mode == ModeWork.SINGLE
                        and event.source == SourceEvent.BD_CLUSTER
                    ):
                        self._load_resources_statuses()
                        stop_time = time.time() + start_timeout
                        logger.debug(f"Reload BD_CLUSTER {stop_time}")
                        continue
                    if (
                        self._mode == ModeWork.PGPOOL
                        and event.source == SourceEvent.PGPOOL
                    ):
                        self._load_resources_statuses()
                        stop_time = time.time() + start_timeout
                        logger.debug(f"Reload PGPOOL {stop_time}")
                        continue
                float_timeout = stop_time - time.time()
                logger.debug(f"event throw {float_timeout}")
                if float_timeout <= 0:
                    raise Empty

            except Empty:
                logger.info(f"Check Healthy: {self._check_healthy()}")
                if self._check_healthy():
                    break
                else:
                    stop_time = time.time() + start_timeout
                    float_timeout = start_timeout
        logger.info("Resources healthy")

    def _check_healthy(self) -> bool:
        """
        Вызывается после обновления кэшей и проверяет их на живость

        True - всё норм
        False - что-то не так
        """
        if not isinstance(self._cluster_obj, K8sObject):
            raise Exception("Cluster not inited")
        instance_names = self._cluster_obj.status.instanceNames
        instances_reported_state = self._cluster_obj.status.instancesReportedState
        instances_statuses = self._cluster_obj.status.instancesStatus

        if self._mode == ModeWork.SINGLE:
            for name, value in instances_reported_state.to_dict().items():
                try:
                    if value.isPrimary and name in instances_statuses.healthy:
                        return True
                except Exception as e:
                    logger.error(e)
            return False

        if self._mode == ModeWork.PGPOOL:
            if not isinstance(self._pgpool_obj, K8sObject):
                raise Exception("Pgpool not inited")
            for inst_name in instance_names:
                if inst_name not in instances_statuses.healthy:
                    logger.error(f"Cluster not healthy: {inst_name} - fail")
                    return False
            pgpool_replicas = self._pgpool_obj.status.get("replicas")
            ready_replicas = self._pgpool_obj.status.get("readyReplicas")
            if pgpool_replicas != ready_replicas or pgpool_replicas is None:
                logger.error(
                    f"Pgpool replica not healthy {pgpool_replicas}: {ready_replicas}"
                )
                return False

        return True
