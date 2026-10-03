from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from queuelib.queue import _clear

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterator

    from queuelib.queue import BaseQueue


@dataclass(frozen=True)
class RoundRobinCheckpoint:
    """A scheduling checkpoint of a :class:`RoundRobinQueue`.

    ``keys`` holds the active keys, i.e. those whose internal queue was not
    empty when the queue was closed, in rotation order: ``keys[0]`` is the
    key of the next internal queue that would be popped.

    Keys are stored as-is, it is up to the caller to encode them for storage
    and to decode them back before building the checkpoint again.
    """

    keys: tuple[Hashable, ...] = ()

    def __iter__(self) -> Iterator[Hashable]:
        return iter(self.keys)

    def __len__(self) -> int:
        return len(self.keys)


def _checkpoint_keys(checkpoint: RoundRobinCheckpoint) -> tuple[Hashable, ...]:
    """Validate the keys of a checkpoint and return them as a tuple."""
    keys = checkpoint.keys
    if isinstance(keys, (str, bytes)) or not isinstance(keys, Iterable):
        raise TypeError(f"checkpoint keys must be a sequence of keys, got {keys!r}")
    seen: set[Hashable] = set()
    for key in keys:
        try:
            hash(key)
        except TypeError as exc:
            raise TypeError(f"unhashable key in checkpoint: {key!r}") from exc
        if key in seen:
            raise ValueError(f"duplicate key in checkpoint: {key!r}")
        seen.add(key)
    return tuple(keys)


class RoundRobinQueue:
    """A round robin queue implemented using multiple internal queues (typically,
    FIFO queues). The internal queue must implement the following methods:
        * push(obj)
        * pop()
        * peek()
        * close()
        * __len__()

    Internal queues may also implement clear(); those that do not are emptied
    by popping.

    The constructor receives a qfactory argument, which is a callable used to
    instantiate a new (internal) queue when a new key is allocated. The
    qfactory function is called with the key number as first and only argument.
    start_domains is a sequence of domains to initialize the queue with. If the
    queue was previously closed leaving some domain buckets non-empty, those
    domains should be passed in start_domains.

    The queue maintains a fifo queue of keys. The key that went last is popped
    first and the next queue for that key is then popped.

    close() returns a RoundRobinCheckpoint that also records the rotation
    position of every active key. Passing that checkpoint back as
    start_domains (or as the checkpoint argument) restores the queue so that
    popping continues the original round robin cycle instead of restarting
    it. Passing a plain sequence of domains as start_domains keeps the
    historical behavior of starting the cycle from the last domain.
    """

    def __init__(
        self,
        qfactory: Callable[[Hashable], BaseQueue],
        start_domains: Iterable[Hashable] | RoundRobinCheckpoint = (),
        checkpoint: RoundRobinCheckpoint | None = None,
    ) -> None:
        self.queues: dict[Hashable, BaseQueue] = {}
        self.qfactory = qfactory
        if checkpoint is None and isinstance(start_domains, RoundRobinCheckpoint):
            checkpoint = start_domains
            start_domains = ()
        if checkpoint is not None:
            if not isinstance(checkpoint, RoundRobinCheckpoint):
                raise TypeError(
                    f"checkpoint must be a RoundRobinCheckpoint, got {checkpoint!r}"
                )
            if tuple(start_domains):
                raise ValueError("pass either start_domains or checkpoint, not both")
            keys = _checkpoint_keys(checkpoint)
            self._open_queues(keys)
            self.key_queue = deque(reversed(keys))
        else:
            self._open_queues(start_domains)
            self.key_queue = deque(self.queues)

    def _open_queues(self, keys: Iterable[Hashable]) -> None:
        try:
            for key in keys:
                self.queues[key] = self.qfactory(key)
        except BaseException:
            for q in self.queues.values():
                with suppress(Exception):
                    q.close()
            self.queues.clear()
            raise

    def push(self, obj: Any, key: Hashable) -> None:
        if key not in self.key_queue:
            self.queues[key] = self.qfactory(key)
            self.key_queue.appendleft(key)  # it's new, might as well pop first
        q = self.queues[key]
        q.push(obj)  # this may fail (eg. serialization error)

    def peek(self) -> Any | None:
        try:
            key = self.key_queue[-1]
        except IndexError:
            return None
        return self.queues[key].peek()

    def clear(self) -> None:
        for q in self.queues.values():
            _clear(q)
            q.close()
        self.queues.clear()
        self.key_queue.clear()

    def pop(self) -> Any | None:
        # pop until we find a valid object, closing necessary queues
        while True:
            try:
                key = self.key_queue.pop()
            except IndexError:
                return None

            q = self.queues[key]
            m = q.pop()

            if len(q) == 0:
                del self.queues[key]
                q.close()
            else:
                self.key_queue.appendleft(key)

            if m is not None:
                return m

    def close(self) -> RoundRobinCheckpoint:
        """Close all internal queues and return a scheduling checkpoint.

        The checkpoint lists the keys whose queue was not empty, in rotation
        order. If closing any internal queue fails, the remaining queues are
        still closed and the first exception is raised, so that no seemingly
        complete checkpoint can be stored by mistake.
        """
        active = []
        error: Exception | None = None
        for key in reversed(self.key_queue):
            q = self.queues[key]
            if len(q):
                active.append(key)
            try:
                q.close()
            except Exception as exc:  # pylint: disable=broad-except
                if error is None:
                    error = exc
        if error is not None:
            raise error
        return RoundRobinCheckpoint(tuple(active))

    def __len__(self) -> int:
        return sum(len(x) for x in self.queues.values()) if self.queues else 0
