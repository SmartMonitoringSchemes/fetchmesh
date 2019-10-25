import datetime as dt
import json
import logging
import random
from collections import defaultdict
from traceback import print_exc
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")
U = TypeVar("U")


def getLogger(instance):
    cls = instance.__class__
    return logging.getLogger(f"{cls.__module__}.{cls.__name__}")


def countby(elements: List[T], key: Callable[[T], U]) -> Dict[U, int]:
    groups = groupby(elements, key)
    counts = {k: len(v) for k, v in groups.items()}
    return counts


def groupby(elements: List[T], key: Callable[[T], U]) -> Dict[U, List[T]]:
    groups: Dict[U, List[T]] = defaultdict(list)
    for el in elements:
        groups[key(el)].append(el)
    return dict(groups)


# Iterator or Iterable ?
def groupby_stream(
    elements: Iterator[T], key: Callable[[T], U], size: int
) -> Iterator[Tuple[U, List[T]]]:
    groups: Dict[U, List[T]] = defaultdict(list)
    for i, el in enumerate(elements):
        groups[key(el)].append(el)
        if (i + 1) % size == 0:
            for item in groups.items():
                yield item
            groups = defaultdict(list)
    for item in groups.items():
        yield item


def groupby_pairs(
    pairs: List[Tuple[T, T]], key: Callable[[T], U]
) -> Dict[U, List[Tuple[T, T]]]:
    groups: Dict[U, List[Tuple[T, T]]] = defaultdict(list)
    for a, b in pairs:
        ka, kb = key(a), key(b)
        if ka == kb:
            groups[ka].append((a, b))
    return dict(groups)


def sample_groups(population: List[List[T]], k: Union[float, int]) -> List[List[T]]:
    if isinstance(k, float) and (k < 0.0 or k > 1.0):
        raise ValueError("Ratio must be between 0.0 and 1.0")
    if isinstance(k, float):
        sizes = [int(len(group) * k) for group in population]
    else:
        sizes = [min(len(group), k) for group in population]
    return [random.sample(group, size) for group, size in zip(population, sizes)]


def parsetimestamp(x: Any) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromtimestamp(int(x))
    except TypeError:
        return None


def totimestamp(x: dt.datetime) -> int:
    return int(x.timestamp())


# TODO: Rename to Some(...) ?
def unwrap(x: Optional[T]) -> T:
    if not x:
        raise ValueError(f"Unwrapped None value")
    return x


def tryfunc(f: Callable, default=None):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            print_exc()
            return default

    return wrapper


# TODO: Custom `json` module instead?
json_trydumps = tryfunc(json.dumps, default="")
json_tryloads = tryfunc(json.loads)


def daterange(
    start: dt.datetime, stop: dt.datetime, step: dt.timedelta
) -> Iterator[dt.datetime]:
    curr = start
    while curr < stop:
        yield curr
        curr += step
