from pathlib import Path

import pytest

from queuelib.queue import (
    FifoDiskQueue,
    FifoMemoryQueue,
    FifoSQLiteQueue,
    LifoDiskQueue,
    LifoMemoryQueue,
    LifoSQLiteQueue,
)
from queuelib.rrqueue import RoundRobinCheckpoint, RoundRobinQueue
from queuelib.tests import DummyQueue, QueuelibTestCase, track_closed


class RRQueueTestMixin:
    def setUp(self):
        super().setUp()
        self.q = RoundRobinQueue(self.qfactory)

    def qfactory(self, key):
        raise NotImplementedError

    def test_len_nonzero(self):
        assert not self.q
        assert len(self.q) == 0
        self.q.push(b"a", "3")
        assert self.q
        self.q.push(b"b", "1")
        self.q.push(b"c", "2")
        self.q.push(b"d", "1")
        assert len(self.q) == 4
        self.q.pop()
        self.q.pop()
        self.q.pop()
        self.q.pop()
        assert not self.q
        assert len(self.q) == 0

    def test_close(self):
        self.q.push(b"a", "3")
        self.q.push(b"b", "1")
        self.q.push(b"c", "2")
        self.q.push(b"d", "1")
        iqueues = self.q.queues.values()
        assert sorted(self.q.close()) == ["1", "2", "3"]
        assert all(q.closed for q in iqueues)

    def test_clear(self):
        self.q.clear()
        assert len(self.q) == 0
        self.q.push(b"a", "3")
        self.q.push(b"b", "1")
        iqueues = list(self.q.queues.values())
        self.q.clear()
        assert len(self.q) == 0
        assert self.q.peek() is None
        assert self.q.pop() is None
        assert all(q.closed for q in iqueues)
        self.q.push(b"c", "2")
        assert self.q.pop() == b"c"
        assert not self.q.close()

    def test_close_return_active(self):
        self.q.push(b"b", "1")
        self.q.push(b"c", "2")
        self.q.push(b"a", "3")
        self.q.pop()
        assert sorted(self.q.close()) == ["2", "3"]

    def test_pop_returns_falsy_items(self):
        self.q.push(b"", "a")
        assert len(self.q) == 1
        assert self.q.pop() == b""
        assert len(self.q) == 0
        assert self.q.pop() is None

    def test_falsy_items_keep_round_robin_order(self):
        self.q.push(b"one", "a")
        self.q.push(b"", "b")
        self.q.push(b"two", "c")
        assert [self.q.pop(), self.q.pop(), self.q.pop()] == [b"one", b"", b"two"]


class FifoTestMixin:
    def test_push_pop_peek_key(self):
        assert self.q.peek() is None
        self.q.push(b"a", "1")
        self.q.push(b"b", "1")
        self.q.push(b"c", "2")
        self.q.push(b"d", "2")
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() is None
        assert self.q.pop() is None


class LifoTestMixin:
    def test_push_pop_peek_key(self):
        assert self.q.peek() is None
        self.q.push(b"a", "1")
        self.q.push(b"b", "1")
        self.q.push(b"c", "2")
        self.q.push(b"d", "2")
        assert self.q.peek() == b"b"
        assert self.q.pop() == b"b"
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() == b"a"
        assert self.q.pop() == b"a"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() is None
        assert self.q.pop() is None


class FifoMemoryRRQueueTest(RRQueueTestMixin, FifoTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        return track_closed(FifoMemoryQueue)()


class LifoMemoryRRQueueTest(RRQueueTestMixin, LifoTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        return track_closed(LifoMemoryQueue)()


class DiskTestMixin:
    def test_nonserializable_object_one(self):
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, "0")
        assert not self.q.close()

    def test_nonserializable_object_many_close(self):
        self.q.push(b"a", "3")
        self.q.push(b"b", "1")
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, "0")
        self.q.push(b"c", "2")
        assert self.q.pop() == b"a"
        assert sorted(self.q.close()) == ["1", "2"]

    def test_nonserializable_object_many_pop(self):
        self.q.push(b"a", "3")
        self.q.push(b"b", "1")
        with pytest.raises(TypeError):
            self.q.push(lambda x: x, "0")
        self.q.push(b"c", "2")
        assert self.q.pop() == b"a"
        assert self.q.pop() == b"b"
        assert self.q.pop() == b"c"
        assert self.q.pop() is None
        assert not self.q.close()


class FifoDiskRRQueueTest(
    RRQueueTestMixin, FifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(FifoDiskQueue)(path)


class LifoDiskRRQueueTest(
    RRQueueTestMixin, LifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(LifoDiskQueue)(path)


class FifoSQLiteRRQueueTest(
    RRQueueTestMixin, FifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(FifoSQLiteQueue)(path)


class LifoSQLiteRRQueueTest(
    RRQueueTestMixin, LifoTestMixin, DiskTestMixin, QueuelibTestCase
):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(LifoSQLiteQueue)(path)


class RRQueueStartDomainsTestMixin:
    def setUp(self):
        super().setUp()
        self.q = RoundRobinQueue(self.qfactory, start_domains=["1", "2"])

    def qfactory(self, key):
        raise NotImplementedError

    def test_push_pop_peek_key(self):
        self.q.push(b"c", "1")
        self.q.push(b"d", "2")
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() is None
        assert self.q.pop() is None

    def test_push_pop_peek_key_reversed(self):
        self.q.push(b"d", "2")
        self.q.push(b"c", "1")
        assert self.q.peek() == b"d"
        assert self.q.pop() == b"d"
        assert self.q.peek() == b"c"
        assert self.q.pop() == b"c"
        assert self.q.peek() is None
        assert self.q.pop() is None


class FifoMemoryRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        return track_closed(FifoMemoryQueue)()


class LifoMemoryRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        return track_closed(LifoMemoryQueue)()


class FifoDiskRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(FifoDiskQueue)(path)


class LifoDiskRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(LifoDiskQueue)(path)


class FifoSQLiteRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(FifoSQLiteQueue)(path)


class LifoSQLiteRRQueueStartDomainsTest(RRQueueStartDomainsTestMixin, QueuelibTestCase):
    def qfactory(self, key):
        path = Path(self.qdir, str(key))
        return track_closed(LifoSQLiteQueue)(path)


class DummyRRQueueTest(QueuelibTestCase):
    def test_clear(self):
        q = RoundRobinQueue(lambda key: DummyQueue())
        q.push(b"a", "1")
        q.push(b"b", "2")
        q.clear()
        assert len(q) == 0
        assert q.pop() is None
        assert not q.close()


@pytest.mark.parametrize("domains", [[], ["a", "b", "c"]])
def test_start_domains_iterator(domains):
    queues = {}
    for domain in domains:
        queue = FifoMemoryQueue()
        queue.push(domain)
        queues[domain] = queue
    queue = RoundRobinQueue(queues.__getitem__, iter(domains))
    assert len(queue) == len(domains)
    assert [queue.pop() for _ in domains] == list(reversed(domains))
    assert queue.pop() is None
    assert not queue.close()


class SharedStoreQueue:
    """FIFO queue backed by shared storage, to simulate process restarts."""

    def __init__(self, store, key):
        self._items = store.setdefault(key, [])
        self.closed = False

    def push(self, obj):
        self._items.append(obj)

    def pop(self):
        return self._items.pop(0) if self._items else None

    def peek(self):
        return self._items[0] if self._items else None

    def close(self):
        self.closed = True

    def __len__(self):
        return len(self._items)


def make_store_factory(store):
    def factory(key):
        return SharedStoreQueue(store, key)

    return factory


RESTART_OPS = [
    ("push", b"a1", "a"),
    ("push", b"b1", "b"),
    ("push", b"c1", "c"),
    ("pop",),
    ("push", b"a2", "a"),
    ("pop",),
    ("push", b"b2", "b"),
    ("pop",),
    ("pop",),
    ("push", b"d1", "d"),
    ("push", b"a3", "a"),
    ("pop",),
    ("pop",),
    ("pop",),
    ("pop",),
    ("pop",),
    ("pop",),
]


def run_ops(q, ops):
    popped = []
    for op in ops:
        if op[0] == "push":
            q.push(op[1], op[2])
        else:
            popped.append(q.pop())
    return popped


@pytest.mark.parametrize("restart_at", range(len(RESTART_OPS) + 1))
@pytest.mark.parametrize("restore", ["positional", "start_domains", "checkpoint"])
def test_restart_continues_rotation(restart_at, restore):
    expected = run_ops(
        RoundRobinQueue(make_store_factory({})), RESTART_OPS
    )

    store = {}
    factory = make_store_factory(store)
    q = RoundRobinQueue(factory)
    popped = run_ops(q, RESTART_OPS[:restart_at])
    checkpoint = q.close()
    if restore == "positional":
        q = RoundRobinQueue(factory, checkpoint)
    elif restore == "start_domains":
        q = RoundRobinQueue(factory, start_domains=checkpoint)
    else:
        q = RoundRobinQueue(factory, checkpoint=checkpoint)
    popped += run_ops(q, RESTART_OPS[restart_at:])
    assert popped == expected
    assert not q.close()


def test_checkpoint_rotation_order():
    q = RoundRobinQueue(lambda key: FifoMemoryQueue())
    q.push(b"a1", "1")
    q.push(b"a2", "1")
    q.push(b"b1", "2")
    q.push(b"c1", "3")
    assert q.pop() == b"a1"
    checkpoint = q.close()
    assert checkpoint.keys == ("2", "3", "1")
    assert list(checkpoint) == ["2", "3", "1"]
    assert len(checkpoint) == 3
    assert checkpoint
    assert RoundRobinCheckpoint() == RoundRobinCheckpoint(())


def test_checkpoint_restore_pop_order():
    store = {}
    factory = make_store_factory(store)
    q = RoundRobinQueue(factory)
    q.push(b"a1", "1")
    q.push(b"a2", "1")
    q.push(b"b1", "2")
    q.push(b"c1", "3")
    assert q.pop() == b"a1"
    checkpoint = q.close()
    q = RoundRobinQueue(factory, checkpoint)
    assert [q.pop(), q.pop(), q.pop()] == [b"b1", b"c1", b"a2"]
    assert q.pop() is None
    assert not q.close()


def test_checkpoint_after_emptied_domain_gets_new_tasks():
    store = {}
    factory = make_store_factory(store)
    q = RoundRobinQueue(factory)
    q.push(b"a", "x")
    q.push(b"b", "y")
    assert q.pop() == b"a"  # "x" becomes empty and is dropped
    q.push(b"c", "x")  # "x" joins the rotation again
    checkpoint = q.close()
    assert checkpoint.keys == ("y", "x")
    q = RoundRobinQueue(factory, checkpoint)
    assert q.pop() == b"b"
    assert q.pop() == b"c"
    assert q.pop() is None


def test_checkpoint_does_not_coerce_keys_to_str():
    seen = []
    store = {}

    def factory(key):
        seen.append(key)
        return SharedStoreQueue(store, key)

    q = RoundRobinQueue(factory)
    q.push(b"a", 1)
    q.push(b"b", (2, 3))
    checkpoint = q.close()
    assert checkpoint.keys == (1, (2, 3))
    q = RoundRobinQueue(factory, checkpoint)
    assert q.pop() == b"a"
    assert q.pop() == b"b"
    assert seen == [1, (2, 3), 1, (2, 3)]


def test_checkpoint_duplicate_keys_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        RoundRobinQueue(
            lambda key: FifoMemoryQueue(), RoundRobinCheckpoint(("a", "a"))
        )


@pytest.mark.parametrize("keys", ["abc", b"abc", 123, None])
def test_checkpoint_invalid_keys_rejected(keys):
    with pytest.raises(TypeError, match="sequence of keys"):
        RoundRobinQueue(lambda key: FifoMemoryQueue(), RoundRobinCheckpoint(keys))


def test_checkpoint_unhashable_key_rejected():
    with pytest.raises(TypeError, match="unhashable"):
        RoundRobinQueue(
            lambda key: FifoMemoryQueue(), RoundRobinCheckpoint((["a"],))
        )


def test_checkpoint_wrong_type_rejected():
    with pytest.raises(TypeError, match="RoundRobinCheckpoint"):
        RoundRobinQueue(lambda key: FifoMemoryQueue(), checkpoint=["a", "b"])


def test_start_domains_and_checkpoint_rejected():
    with pytest.raises(ValueError, match="both"):
        RoundRobinQueue(
            lambda key: FifoMemoryQueue(),
            start_domains=["a"],
            checkpoint=RoundRobinCheckpoint(("b",)),
        )


def test_qfactory_failure_closes_already_opened_queues():
    opened = []

    def factory(key):
        if key == "bad":
            raise RuntimeError("cannot open queue")
        q = track_closed(FifoMemoryQueue)()
        opened.append(q)
        return q

    checkpoint = RoundRobinCheckpoint(("good1", "bad", "good2"))
    with pytest.raises(RuntimeError, match="cannot open queue"):
        RoundRobinQueue(factory, checkpoint)
    assert len(opened) == 1
    assert all(q.closed for q in opened)


def test_qfactory_failure_with_start_domains_closes_already_opened_queues():
    opened = []

    def factory(key):
        if key == "bad":
            raise RuntimeError("cannot open queue")
        q = track_closed(FifoMemoryQueue)()
        opened.append(q)
        return q

    with pytest.raises(RuntimeError, match="cannot open queue"):
        RoundRobinQueue(factory, start_domains=["good1", "bad", "good2"])
    assert len(opened) == 1
    assert all(q.closed for q in opened)


def test_close_failure_raises_instead_of_returning_checkpoint():
    class FailingQueue(FifoMemoryQueue):
        def close(self):
            raise RuntimeError("cannot close")

    queues = {"good": track_closed(FifoMemoryQueue)(), "bad": FailingQueue()}
    q = RoundRobinQueue(queues.__getitem__)
    q.push(b"a", "good")
    q.push(b"b", "bad")
    with pytest.raises(RuntimeError, match="cannot close"):
        q.close()
    assert queues["good"].closed
