from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

# A filter is expected to be frozen, it should not
# remember state from what it has seen.

T = TypeVar("T")


class BaseRepr:
    def __repr__(self):
        args = ", ".join(f"{k}={v}" for k, v in self.__dict__.items())
        name = self.__class__.__name__
        return f"<{name}({args})>"


class BatchFilter(ABC, BaseRepr, Generic[T]):
    @abstractmethod
    def filter(self, data: List[T]) -> List[T]:
        ...

    def __call__(self, data: List[T]) -> List[T]:
        return self.filter(data)


class StreamFilter(ABC, BaseRepr, Generic[T]):
    @abstractmethod
    def keep(self, data: T) -> bool:
        ...

    def filter(self, data: List[T], key=lambda x: x) -> List[T]:
        return [x for x in data if self.keep(key(x))]

    def __call__(self, data: List[T], key=lambda x: x) -> List[T]:
        return self.filter(data, key)
