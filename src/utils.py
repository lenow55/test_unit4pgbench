import json
import re
from dataclasses import dataclass
from enum import IntEnum
from json import JSONDecoder
from typing import Any, Dict, Iterator, Optional, List

from urllib3.response import HTTPResponse

WHITESPACE_RE = re.compile(r"[ \t\n\r]*", re.VERBOSE | re.MULTILINE | re.DOTALL)


def iter_response_objects(response: HTTPResponse) -> Iterator[Dict[str, Any]]:
    """Iterate over the chunks of a :class:`~urllib3.response.HTTPResponse` and yield each JSON document that is found.

    :param response: the HTTP response from which JSON documents will be retrieved.

    :yields: current JSON document.
    """
    prev = ""
    decoder = JSONDecoder()
    for chunk in response.read_chunked(decode_content=False):
        chunk = prev + chunk.decode("utf-8")

        length = len(chunk)
        # ``chunk`` is analyzed in parts. ``idx`` holds the position of the first character in the current part that is
        # neither space nor tab nor line-break, or in other words, the position in the ``chunk`` where it is likely
        # that a JSON document begins
        idx = WHITESPACE_RE.match(chunk, 0).end()  # pyright: ignore [reportOptionalMemberAccess]
        while idx < length:
            try:
                # Get a JSON document from the chunk. ``message`` is a dictionary representing the JSON document, and
                # ``idx`` becomes the position in the ``chunk`` where the retrieved JSON document ends
                message, idx = decoder.raw_decode(chunk, idx)
            except ValueError:  # malformed or incomplete JSON, unlikely to happen
                break
            else:
                yield message
                idx = WHITESPACE_RE.match(chunk, idx).end()  # pyright: ignore [reportOptionalMemberAccess]
        # It is not usual that a ``chunk`` would contain more than one JSON document, but we handle that just in case
        prev = chunk[idx:]


class EventType(IntEnum):
    ADDED = 0
    MODIFIED = 1
    DELETED = 2
    UNEXPECTED = -1


class SourceEvent(IntEnum):
    BD_CLUSTER = 0
    PGPOOL = 1


# this function does the same mapping of snake_case => camelCase for > 97% of cases as autogenerated swagger code
def to_camel_case(value: str) -> str:
    reserved = {
        "api",
        "apiv3",
        "cidr",
        "cpu",
        "csi",
        "id",
        "io",
        "ip",
        "ipc",
        "pid",
        "tls",
        "uri",
        "url",
        "uuid",
    }
    words = value.split("_")
    return words[0] + "".join(
        w.upper() if w in reserved else w.title() for w in words[1:]
    )


class K8sObject(object):
    def __init__(self, kwargs: Dict[str, Any]) -> None:
        self._dict = {k: self._wrap(k, v) for k, v in kwargs.items()}

    def get(self, name: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._dict.get(name, default)

    def __getattr__(self, name: str) -> Any:
        return self.get(to_camel_case(name))

    @classmethod
    def _wrap(cls, parent: Optional[str], value: Any) -> Any:
        if isinstance(value, dict):
            data_dict: Dict[str, Any] = value
            # we know that `annotations` and `labels` are dicts and therefore don't want to convert them into K8sObject
            return (
                data_dict
                if parent in {"annotations", "labels"}
                and all(isinstance(v, str) for v in data_dict.values())
                else cls(data_dict)
            )
        elif isinstance(value, list):
            data_list: List[Any] = value
            return [cls._wrap(None, v) for v in data_list]
        else:
            return value

    def to_dict(self) -> Dict[str, Any]:
        return self._dict

    def __repr__(self) -> str:
        return json.dumps(self, indent=4, default=lambda o: o.to_dict())


@dataclass()
class EventResource:
    type: EventType
    source: SourceEvent
    status_object: K8sObject

    def __str__(self):
        return f"{self.type.name}"


class ResouceException(Exception):
    def __init__(self, value: Any) -> None:
        self.value = value


class RuntimeLoopException(Exception):
    def __init__(self, value: Any) -> None:
        self.value = value
