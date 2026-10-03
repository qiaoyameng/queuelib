from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Hashable

from queuelib.queue import _clear

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from queuelib.queue import BaseQueue


@dataclass(frozen=True)
class RoundRobinCheckpoint:
    """An immutable snapshot of :class:`RoundRobinQueue` scheduling state.

    ``keys`` lists every key whose subqueue is non-empty, in round-robin
    serving order: after a restart, the key at ``position`` is served first,
    followed by the rest of the keys in order, and then wrapping around.

    Keys are kept exactly as passed by the caller; queuelib does not encode
    them or force them to be :class:`str`. A checkpoint with duplicate keys is
    rejected.
    """

    keys: tuple[Hashable, ...]
    position: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.keys, (str, bytes, bytearray)):
            raise ValueError(
                "checkpoint keys must be a sequence of keys, not a single "
                f"{type(self.keys).__name__}"
            )
        try:
            keys = tuple(self.keys)
        except TypeError as exc:
            raise ValueError("checkpoint keys must be iterable") from exc
        for key in keys:
            try:
                hash(key)
            except TypeError as exc:
                raise ValueError(f"checkpoint keys must be hashable: {key!r}") from exc
        if len(set(keys)) != len(keys):
            raise ValueError(f"checkpoint keys must not be duplicated: {list(keys)!r}")
        object.__setattr__(self, "keys", keys)
        if isinstance(self.position, bool) or not isinstance(self.position, int):
            raise ValueError("checkpoint position must be an integer")
        if keys:
            if not 0 <= self.position < len(keys):
                raise ValueError(
                    f"checkpoint position {self.position} is out of range "
                    f"for {len(keys)} keys"
                )
        elif self.position != 0:
            raise ValueError("checkpoint position must be 0 when there are no keys")

    @classmethod
    def from_value(
        cls, value: RoundRobinCheckpoint | Mapping[str, Any] | Sequence[Hashable]
    ) -> RoundRobinCheckpoint:
        """Build a checkpoint from a checkpoint, a mapping, or a key sequence.

        A mapping must contain ``keys`` and may contain ``position``. Key
        sequences start with ``position`` 0. Strings and bytes are never
        treated as key sequences.
        """
        if isinstance(value, RoundRobinCheckpoint):
            return value
        if isinstance(value, Mapping):
            try:
                keys = value["keys"]
            except KeyError as exc:
                raise ValueError("checkpoint mapping must contain 'keys'") from exc
            return cls(keys, value.get("position", 0))
        return cls(value, 0)


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
    domains should be passed in start_domains. This legacy argument does not
    restore the round-robin rotation position.

    checkpoint is a :class:`RoundRobinCheckpoint` (or an equivalent mapping or
    sequence of keys) that restores both the active keys and the round-robin
    rotation position saved by :meth:`close`. Corrupt checkpoints (duplicated
    keys, an out-of-range position, ...) raise :class:`ValueError`.

    The queue maintains a fifo queue of keys. The key that went last is popped
    first and the next queue for that key is then popped.
    """

    def __init__(
        self,
        qfactory: Callable[[Hashable], BaseQueue],
        start_domains: Iterable[Hashable] = (),
        *,
        checkpoint: (
            RoundRobinCheckpoint | Mapping[str, Any] | Sequence[Hashable] | None
        ) = None,
    ) -> None:
        self.qfactory = qfactory
        domains = tuple(start_domains)
        if domains and checkpoint is not None:
            raise ValueError(
                "start_domains and checkpoint cannot be passed at the same time"
            )
        if checkpoint is None:
            self.queues = {}
            for key in domains:
                self.queues[key] = self.qfactory(key)
            self.key_queue = deque(self.queues)
            return
        snapshot = RoundRobinCheckpoint.from_value(checkpoint)
        ordered_keys = deque(snapshot.keys)
        ordered_keys.rotate(-snapshot.position)
        opened: dict[Hashable, BaseQueue] = {}
        for key in ordered_keys:
            try:
                opened[key] = self.qfactory(key)
            except BaseException:
                for queue in opened.values():
                    with suppress(Exception):
                        queue.close()
                raise
        self.queues = opened
        # key_queue stores keys oldest-first; the rightmost key is served next,
        # so reverse the serving order.
        self.key_queue = deque(reversed(ordered_keys))

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
                try:
                    q.close()
                except BaseException:
                    # The subqueue may still hold data on disk; keep the key
                    # scheduled instead of silently dropping its position.
                    self.queues[key] = q
                    self.key_queue.append(key)
                    raise
            else:
                self.key_queue.appendleft(key)

            if m is not None:
                return m

    def close(
        self, checkpoint: bool = False
    ) -> list[Hashable] | RoundRobinCheckpoint:
        """Close every subqueue and report the keys that remain non-empty.

        By default, the active keys are returned as a list. With
        ``checkpoint=True`` a :class:`RoundRobinCheckpoint` is returned, in
        serving order: its first key is the one that would be popped next.
        The checkpoint is only built after every subqueue has been closed
        successfully, so it always matches the data actually persisted on
        disk; a qfactory or subqueue failure never produces one.
        """
        scheduled = tuple(
            key for key in reversed(self.key_queue) if len(self.queues[key])
        )
        active = []
        for key, q in self.queues.items():
            if len(q):
                active.append(key)
            q.close()
        if checkpoint:
            return RoundRobinCheckpoint(scheduled, 0)
        return active

    def __len__(self) -> int:
        return sum(len(x) for x in self.queues.values()) if self.queues else 0
