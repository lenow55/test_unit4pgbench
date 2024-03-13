import logging
import socket
import time
from queue import Queue
from threading import Condition, Lock, Thread
from typing import Any, Callable, Dict, Optional, Tuple, Union

import urllib3

from utils import (
    EventResource,
    EventType,
    K8sObject,
    SourceEvent,
    iter_response_objects,
)

logger = logging.getLogger(__name__)


class ObjectCache(Thread):
    def __init__(
        self,
        func: Callable[..., Any],
        queue: Queue,
        condition: Condition,
        name: str,
        souce_type: SourceEvent,
    ) -> None:
        super(ObjectCache, self).__init__()
        self.daemon = True
        self._queue = queue
        self._func = func
        self._condition = condition
        self._name = name
        self._source = souce_type
        self._is_ready = False
        self._response_lock = (
            Lock()
        )  # protect the `self._response` from concurrent access
        self._object_cache: K8sObject | None
        self._object_cache_lock = Lock()
        self.start()

    def _list(self) -> K8sObject:
        try:
            return K8sObject(self._func())
        except Exception:
            time.sleep(1)
            raise

    def _watch(self, resource_version: str) -> urllib3.HTTPResponse:
        return self._func(
            _request_timeout=5.0,
            _preload_content=False,
            watch=True,
            resource_version=resource_version,
        )

    def set(self, value: K8sObject) -> Tuple[bool, Optional[K8sObject]]:
        with self._object_cache_lock:
            old_value = self._object_cache
            ret = not old_value or int(old_value.metadata.resource_version) < int(
                value.metadata.resource_version
            )
            if ret:
                self._object_cache = value
        return ret, old_value

    def delete(self, resource_version: str) -> Tuple[bool, Optional[K8sObject]]:
        with self._object_cache_lock:
            old_value = self._object_cache
            ret = old_value and int(old_value.metadata.resource_version) < int(
                resource_version
            )
            if ret:
                self._object_cache = None
        return bool(not old_value or ret), old_value

    def get(self) -> Optional[K8sObject]:
        with self._object_cache_lock:
            return self._object_cache

    def _process_event(
        self, event: Dict[str, Union[Any, Dict[str, Union[Any, Dict[str, Any]]]]]
    ) -> None:
        ev_type = event["type"]
        obj = event["object"]
        name = obj["metadata"]["name"]

        # Пропускаем те объекты, которые не имеют нужного имени
        if name != self._name:
            return

        if ev_type in ("ADDED", "MODIFIED"):
            obj = K8sObject(obj)
            success, _ = self.set(obj)
            if success:
                if ev_type == "ADDED":
                    event4queue = EventResource(
                        type=EventType.ADDED,
                        source=self._source,
                        status_object=obj.status,
                    )
                    self._queue.put_nowait(event4queue)
                if ev_type == "MODIFIED":
                    event4queue = EventResource(
                        type=EventType.MODIFIED,
                        source=self._source,
                        status_object=obj.status,
                    )
                    self._queue.put_nowait(event4queue)
        elif ev_type == "DELETED":
            success, _ = self.delete(obj["metadata"]["resourceVersion"])
            if success:
                event4queue = EventResource(
                    type=EventType.DELETED, source=self._source, status_object={}
                )
                self._queue.put_nowait(event4queue)
        else:
            return logger.warning("Unexpected event type: %s", ev_type)

    @staticmethod
    def _finish_response(response: urllib3.HTTPResponse) -> None:
        try:
            response.close()
        finally:
            response.release_conn()

    def _do_watch(self, resource_version: str) -> None:
        with self._response_lock:
            self._response = None
        response = self._watch(resource_version)
        with self._response_lock:
            if self._response is None:
                self._response = response

        if not self._response:
            return self._finish_response(response)

        for event in iter_response_objects(response):
            if event["object"].get("code") == 410:
                break
            self._process_event(event)

    def _build_cache(self) -> None:
        objects = self._list()
        # Получаем только тот объект, имя которого совпадает с нашим желанным
        target_object_list = [
            item for item in objects.items if item.metadata.name == self._name
        ]
        if len(target_object_list) == 0:
            time.sleep(1)
            raise Exception(f"No object with name {self._name}, objects {objects}")

        target_object = target_object_list[0]

        with self._object_cache_lock:
            self._object_cache = target_object
        with self._condition:
            self._is_ready = True
            self._condition.notify()

        try:
            self._do_watch(target_object.metadata.resource_version)
        finally:
            with self._condition:
                self._is_ready = False
            with self._response_lock:
                response, self._response = self._response, None
            if isinstance(response, urllib3.HTTPResponse):
                self._finish_response(response)

    def kill_stream(self) -> None:
        sock = None
        with self._response_lock:
            if isinstance(self._response, urllib3.HTTPResponse):
                try:
                    sock = (
                        self._response.connection.sock
                        if self._response.connection
                        else None
                    )
                except Exception:
                    sock = None
            else:
                self._response = False
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except Exception as e:
                logger.debug("Error on socket.shutdown: %r", e)

    def run(self) -> None:
        while True:
            try:
                self._build_cache()
            except Exception as e:
                logger.error("ObjectCache.run %r", e)

    def is_ready(self) -> bool:
        """Must be called only when holding the lock on `_condition`"""
        return self._is_ready
