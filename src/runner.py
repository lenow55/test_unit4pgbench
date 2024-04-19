import asyncio
import functools
from io import TextIOWrapper
import logging
import os
import re
import subprocess
import sys
import time
from enum import IntEnum
from queue import Empty, Queue
from threading import Condition, Thread
from typing import Iterator, List, Tuple

import pandas as pd
from jinja2 import Environment, FileSystemLoader
from kubernetes.client import CustomObjectsApi
from aiogram import Bot
from aiogram import Dispatcher


from cache_single_object import ObjectCache
from model_pipeline import (
    ExplainedQuery,
    IndexedCombinations,
    IndexedCombinationsOut,
    QueriesOut,
)
from prometheus_utils import check_replication
from settings import EnvironmentOption, RunSettings
from templating import gen_template
from utils import EventResource, K8sObject, RuntimeLoopException, SourceEvent

from requests.exceptions import Timeout
from pydantic import ValidationError
from utils import ResouceException


class ModeWork(IntEnum):
    SINGLE = 0
    PGPOOL = 1


class RunnerDaemon(Thread):
    def __init__(self, api: CustomObjectsApi, config: RunSettings) -> None:
        super(RunnerDaemon, self).__init__()
        self.daemon = True
        self._is_inited = False
        self._api_client = api
        self._config = config
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
            name=self._config.cluster_name,
            souce_type=SourceEvent.BD_CLUSTER,
        )
        self._pgpool_cache: ObjectCache = ObjectCache(
            self._pgpool_func,
            self._buffer,
            self._condition,
            name="pgpool",
            souce_type=SourceEvent.PGPOOL,
        )

    def set_logger(self):
        self._logger = logging.getLogger(__name__)
        if self._config.environment == EnvironmentOption.DEBUG:
            level = logging.DEBUG
        else:
            level = logging.INFO
        fmt = logging.Formatter(
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        shell_handler = logging.StreamHandler(sys.stdout)
        shell_handler.setLevel(level)
        shell_handler.setFormatter(fmt)
        self._logger.addHandler(shell_handler)
        self._logger.setLevel(level)

    def start(self):
        if not self._is_inited:
            raise ValueError("Not correct init")
        super().start()

    def init_runner(self):
        self.set_logger()
        self._logger.info("Init complete")
        self._mode = ModeWork.SINGLE
        self._bot = Bot(token=self._config.bot_token)
        self._loop_bot = asyncio.get_event_loop()
        self._dp = Dispatcher()
        try:
            self._environment = Environment(
                loader=FileSystemLoader(self._config.templates_folder)
            )
            if not os.path.exists(self._config.temp_sql_scripts_folder):
                os.mkdir(self._config.temp_sql_scripts_folder)
            else:
                self._logger.info(
                    f"out folder exists {self._config.temp_sql_scripts_folder}"
                )
            if not os.path.exists(self._config.output_folder):
                os.mkdir(self._config.output_folder)
            else:
                self._logger.info(f"out folder exists {self._config.output_folder}")
        except Exception as e:
            self._logger.error(f"Can't init runner: {e}")
            raise e
        self._last_size = 0
        self._is_inited = True

    def _get_pgbench_script(self, combination: IndexedCombinations) -> str:
        template = gen_template(self._environment, combination)
        filename_split = os.path.splitext(combination.template)
        filename_name = os.path.splitext(filename_split[0])
        filename = filename_name[0] + f"_{combination.index}" + filename_name[1]
        file_path = os.path.join(self._config.temp_sql_scripts_folder, filename)
        with open(
            file_path,
            mode="w",
            encoding="utf-8",
        ) as message:
            message.write(template)
        return file_path

    def _send_message(self, message: str):
        try:
            self._loop_bot.run_until_complete(
                self._bot.send_message(chat_id=self._config.bot_admin_id, text=message)
            )
        except Exception as e:
            self._logger.error(e, exc_info=True)

    def run(self) -> None:
        continue_iteration: bool = True
        combination_gen = self._get_next_item(
            self._config.combinations_pipeline,
            os.path.join(self._config.output_folder, "state"),
            os.path.join(self._config.output_folder, "lockfile"),
        )
        need_init_db: bool = True
        try:
            combination = next(combination_gen)
        except StopIteration:
            self._logger.info("No init combination in iterator")
            self._send_message("No init combination in iterator")
            return

        self._send_message("Start working")
        self._send_message(
            f"Would bee processed {self._df_configs.shape[0] - combination.index} combination"
        )

        counter: int = 0
        while continue_iteration:
            try:
                if combination.pgpool:
                    self._mode = ModeWork.PGPOOL
                if not combination.pgpool:
                    self._mode = ModeWork.SINGLE
                self._logger.info(f"Work at mode {self._mode.name}")
                self._logger.info(f"Combination index is {combination.index}")
                self._wait_resource_bee_healthy(5.0)
                self._logger.debug(combination.model_dump_json(indent=2))
                if self._check_need_init_db(combination) and need_init_db:
                    self._logger.info("Need init db")
                    init_cmd = self._gen_init_command(combination=combination)
                    flag_com, code, _, err = self.run_command_with_watch(init_cmd)
                    if code != 0:
                        self._logger.error(f"Base not init; code:{code}, retry ")
                        self._logger.error(err)
                        raise RuntimeLoopException("Base init failed: retry")
                    self._logger.info("Base inited")
                    self._send_message(f"Base reinited with {combination.size}")
                if self._mode == ModeWork.PGPOOL:
                    self._send_message("Wait replication")
                    self._wait_replication(30.0)

                self._last_size = combination.size
                need_init_db = False

                self._logger.info("Start explain")
                explained_queries = self._explain_queries(combination)
                self._logger.info("End explain")
                self._logger.info("Start testing")
                sql_script_file = self._get_pgbench_script(combination=combination)
                test_command = self._gen_test_command(
                    combination=combination, sql_script_file=sql_script_file
                )
                flag_com, code, res, err = self.run_command_with_watch(
                    test_command, float(self._config.testing_period * 2)
                )
                if code != 0:
                    self._logger.error(f"Test failed; code:{code}, retry ")
                    # self._logger.error(err)
                    filename_error_test = os.path.join(
                        self._config.output_folder,
                        f"test_error_id_{combination.index}.txt",
                    )
                    with open(filename_error_test, "w") as file:
                        file.write(res)
                        file.write("\n\n")
                        file.write(err)
                        raise RuntimeLoopException("Test failed: retry")

                self._logger.info("End testing")
                self._logger.debug(res)
                filename_out_test = os.path.join(
                    self._config.output_folder,
                    f"test_res_id_{combination.index}.txt",
                )
                with open(filename_out_test, "w") as file:
                    file.write(res)
                    file.write("\n\n")
                    file.write(err)  # надо, так как вывод по 10 сек пишется в stderr

                tps = self._extract_tps(res)
                out_combination = IndexedCombinationsOut(
                    queries=explained_queries.queries,
                    process_file=filename_out_test,
                    tps=tps,
                    **combination.model_dump(exclude={"queries"}),
                )
                filename_processed_base = os.path.join(
                    self._config.output_folder,
                    "processed_combinations.json",
                )
                with open(filename_processed_base, mode="a") as file:
                    file.write(out_combination.model_dump_json(indent=2))

                combination = next(combination_gen)
                # break
                need_init_db = True
                counter = counter + 1
                if counter % 3 == 0 or counter == 1:
                    self._send_message(f"At processing {combination.index} id")

            except StopIteration:
                continue_iteration = True
                break
            except KeyboardInterrupt:
                self._send_message("program shutdown")
                break
            except RuntimeLoopException as e:
                self._send_message(f"Runtime exception occure {str(e)}")
            except Exception as e:
                self._logger.error(e, exc_info=True)
                self._send_message(f"Exception occure {str(e)}")

        self._logger.info("Runner exit")
        self._send_message("End working")

    def run_command_with_watch(
        self, command: List[str], timeout_wait: float = -1
    ) -> Tuple[bool, int, str, str]:
        pool_interval = 10.0
        processing_flag = True
        self._logger.debug(f"Create subprocess with command: {command}")
        pass_env = os.environ.copy()
        pass_env["PGPASSWORD"] = f"{self._config.pgpassword}"
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=pass_env,
            text=True,
        )
        stop_wait_time = time.time() + timeout_wait
        while processing_flag:
            try:
                event = self._buffer.get(timeout=pool_interval)
                if isinstance(event, EventResource):
                    if self._process_event(event):
                        raise ResouceException("Resources broken")
                self._buffer.task_done()
            except Empty:
                self._logger.debug("Check process alive")
                code = process.poll()
                if code is None:
                    self._logger.debug("Process alive")
                    if timeout_wait != -1 and (stop_wait_time - time.time() <= 0):
                        self._logger.error(f"Process working so long {timeout_wait}")
                        process.kill()
                    continue
                self._logger.debug(f"Process complete with code {code}")
                processing_flag = False
                result = ""
                if isinstance(process.stdout, TextIOWrapper):
                    result = process.stdout.read()
                res_errors = ""
                if isinstance(process.stderr, TextIOWrapper):
                    res_errors = process.stderr.read()
                if code == 0:
                    return True, code, result, res_errors
                else:
                    return False, code, result, res_errors
            except Exception as e:
                raise e

        return True, 1, "", ""

    def run_command(self, command) -> Tuple[int, str, str]:
        self._load_resources_statuses()
        if self._check_healthy():
            self._logger.info("Run command without watch\n" f"command: {command}")
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
                    self._logger.error(f"Instance fail: {inst_name}")
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
            self._logger.error(
                f"Master failed {instances_reported_state} to {new_rep_state}"
            )
            return True

        return False

    def _process_pgpool_event(self, event: EventResource) -> bool:
        pgpool_replicas = event.status_object.get("replicas")
        ready_replicas = event.status_object.get("readyReplicas")
        if pgpool_replicas != ready_replicas or pgpool_replicas is None:
            self._logger.error(f"Pgpool replica fails {event.status_object}")
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
        stop_time = time.time() + 60.0
        stop_time = time.time() + 6000.0
        try:
            with self._condition:
                self._wait_caches(stop_time=stop_time)
                self._cluster_obj = self._cluster_cache.get()
                if self._mode == ModeWork.PGPOOL:
                    self._pgpool_obj = self._pgpool_cache.get()
        except Exception:
            self._logger.exception("get_cluster")
            raise Exception("Kubernetes API is not responding properly")
        self._logger.debug("loaded resources")

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

    def _wait_resource_bee_healthy(
        self, start_timeout: float = 60, timeout_wait: float = -1
    ):
        self._logger.debug("Wait resources bee healthy")
        self._load_resources_statuses()
        float_timeout = start_timeout
        stop_time = time.time() + start_timeout
        stop_wait_time = time.time() + timeout_wait
        while True:
            self._logger.debug("start for")
            try:
                event = self._buffer.get(timeout=float_timeout)
                if isinstance(event, EventResource):
                    if event.source == SourceEvent.BD_CLUSTER:
                        self._load_resources_statuses()
                        stop_time = time.time() + start_timeout
                        self._logger.debug(f"Reload BD_CLUSTER {stop_time}")
                        continue
                    if self._mode == ModeWork.PGPOOL:
                        if event.source == SourceEvent.PGPOOL:
                            self._load_resources_statuses()
                            stop_time = time.time() + start_timeout
                            self._logger.debug(f"Reload PGPOOL {stop_time}")
                            continue
                float_timeout = stop_time - time.time()
                self._logger.debug(f"event throw {float_timeout}")
                if float_timeout <= 0:
                    raise Empty

            except Empty:
                self._logger.info(f"Check Healthy: {self._check_healthy()}")
                if self._check_healthy():
                    break
                else:
                    if timeout_wait != -1 and (stop_wait_time - time.time() <= 0):
                        raise Exception(
                            f"Resouces not healthy in timeout {timeout_wait}"
                        )
                    stop_time = time.time() + start_timeout
                    float_timeout = start_timeout
        self._logger.info("Resources healthy")

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
                    self._logger.error(e)
            return False

        if self._mode == ModeWork.PGPOOL:
            if not isinstance(self._pgpool_obj, K8sObject):
                raise Exception("Pgpool not inited")
            for inst_name in instance_names:
                if inst_name not in instances_statuses.healthy:
                    self._logger.error(f"Cluster not healthy: {inst_name} - fail")
                    return False
            pgpool_replicas = self._pgpool_obj.status.get("replicas")
            ready_replicas = self._pgpool_obj.status.get("readyReplicas")
            if pgpool_replicas != ready_replicas or pgpool_replicas is None:
                self._logger.error(
                    f"Pgpool replica not healthy {pgpool_replicas}: {ready_replicas}"
                )
                return False

        return True

    def _get_next_item(
        self, combinations_file: str, state_file: str, lockfile: str
    ) -> Iterator[IndexedCombinations]:
        try:
            self._df_configs = pd.read_json(combinations_file)
        except Exception as e:
            raise e

        if os.path.exists(lockfile):
            self._logger.error("All tests already passed")
            time.sleep(3000)
            raise StopIteration()

        start_index = 0
        if os.path.exists(state_file):
            try:
                with open(state_file) as f:
                    start_index = int(f.read())
            except Exception as e:
                self._logger.error(e)
                start_index = 0

        for i in range(start_index, self._df_configs.shape[0]):
            config_dict = self._df_configs.iloc[i].to_dict()
            config = IndexedCombinations(**config_dict)
            with open(state_file, mode="w") as f:
                count_simbols = f.write(str(config.index))
                if count_simbols == 0:
                    raise Exception(f"Can't write state file {state_file}")
            yield config

        os.remove(state_file)

        with open(lockfile, mode="w") as f:
            f.write("tests passed")

    def _check_need_init_db(self, combination: IndexedCombinations) -> bool:
        if combination.index == 0:
            return True
        if combination.size != self._last_size:
            return True
        return False

    def _gen_init_command(self, combination: IndexedCombinations) -> List[str]:
        command = [
            "pgbench",
            "-i",
            "-s",
            f"{combination.size}",
            "--host",
            f"{self._config.db_rw_host}",
            "-p",
            f"{self._config.db_rw_port}",
            "-U",
            f"{self._config.pguser}",
            f"{self._config.db_name}",
        ]
        return command

    def _gen_test_command(
        self, combination: IndexedCombinations, sql_script_file: str
    ) -> List[str]:
        if self._mode == ModeWork.PGPOOL:
            host = self._config.pgpool_host
            port = self._config.pgpool_service_port
        else:
            host = self._config.db_rw_host
            port = self._config.db_rw_port
        command = [
            "pgbench",
            "--host",
            f"{host}",
            "-p",
            f"{port}",
            "-U",
            f"{self._config.pguser}",
            "-c",
            f"{combination.connections}",
            "-j",
            f"{self._config.pgbench_threads}",
            "-T",
            f"{self._config.testing_period}",
            "-P",
            "10",
            "-f",
            f"{sql_script_file}",
            f"{self._config.db_name}",
        ]
        return command

    def _gen_commands_explain(
        self, combination: IndexedCombinations
    ) -> Iterator[Tuple[int, List[str]]]:
        list_explain_queryes = []
        if combination.read_percent > 0:
            list_explain_queryes.append(
                (
                    combination.read_query_id,
                    combination.queries[combination.read_query_id].analyze,
                )
            )
        if combination.write_percent > 0:
            list_explain_queryes.append(
                (
                    combination.write_query_id,
                    combination.queries[combination.write_query_id].analyze,
                )
            )

        for id, query_str in list_explain_queryes:
            command = [
                "psql",
                "--host",
                f"{self._config.db_rw_host}",
                "-p",
                f"{self._config.db_rw_port}",
                "-U",
                f"{self._config.pguser}",
                "-x",
                "-c",
                f"EXPLAIN ANALYZE {query_str}",
            ]
            yield id, command

    def _explain_queries(self, combination: IndexedCombinations) -> QueriesOut:
        explained_list = []
        for id, cmd in self._gen_commands_explain(combination):
            flag_com, code, res, err = self.run_command_with_watch(cmd)
            if code != 0:
                self._logger.error("Query not explain")
                self._logger.debug(err)
                raise Exception("Can't explain query")
            self._logger.debug(res)
            filename = os.path.join(
                self._config.output_folder,
                f"explain_id_{combination.index}_query_id_{id}.txt",
            )
            explain: str = ""
            first_match = re.search(r"\((.*?)\)", res)

            # Если есть совпадение, выводим его
            if first_match:
                explain = first_match.group(1)
            else:
                self._logger.error("Can't find explain in stdout")
            with open(filename, "w") as file:
                file.write(res)
            explained_list.append(
                ExplainedQuery(
                    **combination.queries[id].model_dump(),
                    explain=explain,
                    explain_file=filename,
                )
            )
        return QueriesOut(queries=explained_list)

    def _wait_replication(self, start_timeout: float = 60, timeout_wait: float = -1):
        self._logger.debug("Wait replication complete")
        self._load_resources_statuses()
        float_timeout = start_timeout
        stop_time = time.time() + start_timeout
        stop_wait_time = time.time() + timeout_wait
        while True:
            try:
                event = self._buffer.get(timeout=float_timeout)
                if isinstance(event, EventResource):
                    if event.source == SourceEvent.BD_CLUSTER:
                        if self._process_cluster_event(event):
                            # TODO: переопределить свой класс исключения, чтобы отлавливать эту ситуацию
                            raise ResouceException(
                                "Resources broken, Replication can't continue"
                            )
                self._buffer.task_done()
                float_timeout = stop_time - time.time()
                self._logger.debug(f"event throw {float_timeout}")
                if float_timeout <= 0:
                    raise Empty

            except Empty:
                try:
                    check = check_replication(
                        self._config.prometheus_host, self._config.prometheus_port
                    )
                    if check:
                        self._logger.debug("Still replicating")
                        break
                except Timeout as e:
                    self._logger.error(f"Check Healthy: {e}")
                except ValidationError as e:
                    self._logger.error(f"Check Healthy: {e.json()}")
                if timeout_wait != -1 and (stop_wait_time - time.time() <= 0):
                    raise Exception(f"Resouces not healthy in timeout {timeout_wait}")
                stop_time = time.time() + start_timeout
                float_timeout = start_timeout

        self._logger.info("Replication complete")

    def _extract_tps(self, raw: str) -> float:
        match = re.search(r"tps = (\d+\.\d+)", raw)

        # Если есть совпадение, выводим значение
        if match:
            tps_value = float(match.group(1))
        else:
            tps_value = -1
        return tps_value
