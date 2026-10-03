from __future__ import annotations

import multiprocessing
import sqlite3
import traceback
from abc import abstractmethod
from pathlib import Path
from unittest import mock

import pytest

from queuelib import (
    FifoDiskQueue,
    FifoMemoryQueue,
    FifoSQLiteQueue,
    LifoDiskQueue,
    LifoMemoryQueue,
    LifoSQLiteQueue,
)
from queuelib.queue import BaseQueue
from queuelib.tests import DummyQueue, QueuelibTestCase


def _shared_queue_consumer(
    path: str,
    lifo: bool,
    start: multiprocessing.synchronize.Barrier,
    rounds: int,
    round_barrier: multiprocessing.synchronize.Barrier,
    drain_done: multiprocessing.synchronize.Barrier,
    closed: multiprocessing.synchronize.Event,
    can_push: multiprocessing.synchronize.Event,
    results: multiprocessing.Queue,
) -> None:
    """Shared-mode partner that pops concurrently and then keeps writing."""
    try:
        cls = LifoSQLiteQueue if lifo else FifoSQLiteQueue
        q = cls(path, shared=True)
        start.wait()
        items: list[bytes] = []
        for _ in range(rounds):
            round_barrier.wait()
            item = q.pop()
            assert item is not None
            items.append(item)
        drain_done.wait()
        assert q.pop() is None
        closed.wait()
        assert Path(path).exists()
        can_push.wait()
        q.push(b"extra")
        assert q.pop() == b"extra"
        q.push(b"")
        assert q.pop() == b""
        assert q.pop() is None
        q.close()
        results.put(("items", items))
    except BaseException:  # noqa: BLE001
        results.put(("error", traceback.format_exc()))


def _shared_queue_closer(
    path: str,
    lifo: bool,
    start: multiprocessing.synchronize.Barrier,
    rounds: int,
    round_barrier: multiprocessing.synchronize.Barrier,
    drain_done: multiprocessing.synchronize.Barrier,
    closed: multiprocessing.synchronize.Event,
    can_push: multiprocessing.synchronize.Event,
    results: multiprocessing.Queue,
) -> None:
    """Shared-mode partner that pops concurrently and then closes while empty."""
    try:
        cls = LifoSQLiteQueue if lifo else FifoSQLiteQueue
        q = cls(path, shared=True)
        start.wait()
        items: list[bytes] = []
        for _ in range(rounds):
            round_barrier.wait()
            item = q.pop()
            assert item is not None
            items.append(item)
        drain_done.wait()
        q.close()
        assert Path(path).exists()
        closed.set()
        can_push.set()
        results.put(("items", items))
    except BaseException:  # noqa: BLE001
        results.put(("error", traceback.format_exc()))


class InterfaceTest(QueuelibTestCase):
    def test_queue(self):
        queue = BaseQueue()
        with pytest.raises(NotImplementedError):
            queue.push(b"")
        with pytest.raises(NotImplementedError):
            queue.peek()
        with pytest.raises(NotImplementedError):
            queue.pop()
        with pytest.raises(NotImplementedError):
            len(queue)
        queue.close()

    def test_issubclass(self):
        assert not issubclass(list, BaseQueue)
        assert not issubclass(int, BaseQueue)
        assert not issubclass(QueuelibTestCase, BaseQueue)
        assert issubclass(DummyQueue, BaseQueue)
        assert issubclass(FifoMemoryQueue, BaseQueue)
        assert issubclass(LifoMemoryQueue, BaseQueue)
        assert issubclass(FifoDiskQueue, BaseQueue)
        assert issubclass(LifoDiskQueue, BaseQueue)
        assert issubclass(FifoSQLiteQueue, BaseQueue)
        assert issubclass(LifoSQLiteQueue, BaseQueue)

    def test_isinstance(self):
        assert not isinstance(1, BaseQueue)
        assert not isinstance([], BaseQueue)
        assert isinstance(DummyQueue(), BaseQueue)
        assert isinstance(FifoMemoryQueue(), BaseQueue)
        assert isinstance(LifoMemoryQueue(), BaseQueue)
        for cls in [FifoDiskQueue, LifoDiskQueue, FifoSQLiteQueue, LifoSQLiteQueue]:
            queue = cls(self.tempfilename())
            assert isinstance(queue, BaseQueue)
            queue.close()


class QueueTestMixin:
    @abstractmethod
    def queue(self) -> BaseQueue:
        raise NotImplementedError

    def test_empty(self):
        """Empty queue test"""
        q = self.queue()
        assert q.pop() is None
        q.close()

    def test_single_pushpop(self):
        q = self.queue()
        q.push(b"a")
        assert q.pop() == b"a"
        q.close()

    def test_binary_element(self):
        elem = (
            b"\x80\x02}q\x01(U\x04bodyq\x02U\x00U\t_encodingq\x03U\x05utf-"
            b"8q\x04U\x07cookiesq\x05}q\x06U\x04metaq\x07}q\x08U\x07header"
            b"sq\t}U\x03urlq\nX\x15\x00\x00\x00file:///tmp/tmphDJYsgU\x0bd"
            b"ont_filterq\x0b\x89U\x08priorityq\x0cK\x00U\x08callbackq\rNU"
            b"\x06methodq\x0eU\x03GETq\x0fU\x07errbackq\x10Nu."
        )
        q = self.queue()
        q.push(elem)
        assert q.pop() == elem
        q.close()

    def test_len(self):
        q = self.queue()
        assert len(q) == 0
        q.push(b"a")
        assert len(q) == 1
        q.push(b"b")
        q.push(b"c")
        assert len(q) == 3
        q.pop()
        q.pop()
        q.pop()
        assert len(q) == 0
        q.close()

    def test_clear(self):
        q = self.queue()
        q.clear()
        assert len(q) == 0
        q.push(b"a")
        q.push(b"b")
        q.clear()
        assert len(q) == 0
        assert q.peek() is None
        assert q.pop() is None
        q.push(b"c")
        assert q.pop() == b"c"
        q.close()

    def test_peek_one_element(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() is None
        q.close()


class FifoTestMixin:
    def test_push_pop1(self):
        """Basic push/pop test"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.pop() == b"a"
        assert q.pop() == b"b"
        assert q.pop() == b"c"
        assert q.pop() is None
        q.close()

    def test_push_pop2(self):
        """Test interleaved push and pops"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        assert q.pop() == b"a"
        assert q.pop() == b"b"
        q.push(b"e")
        assert q.pop() == b"c"
        assert q.pop() == b"d"
        assert q.pop() == b"e"
        q.close()

    def test_peek_fifo(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.peek() == b"a"
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() == b"b"
        assert q.peek() == b"b"
        assert q.pop() == b"b"
        assert q.peek() == b"c"
        assert q.peek() == b"c"
        assert q.pop() == b"c"
        assert q.peek() is None
        q.close()

    def test_push_after_peek(self):
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        assert q.peek() == b"a"
        q.push(b"c")
        assert q.pop() == b"a"
        assert q.pop() == b"b"
        assert q.pop() == b"c"
        assert q.pop() is None
        q.close()


class LifoTestMixin:
    def test_push_pop1(self):
        """Basic push/pop test"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.pop() == b"c"
        assert q.pop() == b"b"
        assert q.pop() == b"a"
        assert q.pop() is None
        q.close()

    def test_push_pop2(self):
        """Test interleaved push and pops"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        assert q.pop() == b"d"
        assert q.pop() == b"c"
        q.push(b"e")
        assert q.pop() == b"e"
        assert q.pop() == b"b"
        assert q.pop() == b"a"
        q.close()

    def test_peek_lifo(self):
        q = self.queue()
        assert q.peek() is None
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        assert q.peek() == b"c"
        assert q.peek() == b"c"
        assert q.pop() == b"c"
        assert q.peek() == b"b"
        assert q.peek() == b"b"
        assert q.pop() == b"b"
        assert q.peek() == b"a"
        assert q.peek() == b"a"
        assert q.pop() == b"a"
        assert q.peek() is None
        q.close()

    def test_push_after_peek(self):
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        assert q.peek() == b"b"
        q.push(b"c")
        assert q.pop() == b"c"
        assert q.pop() == b"b"
        assert q.pop() == b"a"
        assert q.pop() is None
        q.close()


class PersistentTestMixin:
    chunksize = 100000

    # Scrapy SerializableQueue doesn't raise TypeError for non-bytes objects,
    # so this test fails when run for it.
    @pytest.mark.xfail(
        reason="Reenable once Scrapy.squeues stops extending from this testsuite"
    )
    def test_non_bytes_raises_typeerror(self):
        q = self.queue()
        try:
            with pytest.raises(TypeError):
                q.push(0)
            with pytest.raises(TypeError):
                q.push("")
            with pytest.raises(TypeError):
                q.push(None)
            with pytest.raises(TypeError):
                q.push(lambda x: x)
        finally:
            q.close()

    def test_text_in_windows(self):
        e1 = b"\r\n"
        q = self.queue()
        q.push(e1)
        q.close()
        q = self.queue()
        e2 = q.pop()
        assert e1 == e2
        q.close()

    def test_close_open(self):
        """Test closing and re-opening keeps state"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.push(b"c")
        q.push(b"d")
        q.pop()
        q.pop()
        q.close()
        del q

        q = self.queue()
        assert len(q) == 2
        q.push(b"e")
        q.pop()
        q.pop()
        q.close()
        del q

        q = self.queue()
        assert q.pop() is not None
        assert len(q) == 0
        q.close()

    def test_cleanup(self):
        """Test queue dir is removed if queue is empty"""
        q = self.queue()
        values = [b"0", b"1", b"2", b"3", b"4"]
        assert self.qpath.exists()
        for x in values:
            q.push(x)

        for _ in values:
            q.pop()
        q.close()
        assert not self.qpath.exists()


class DirectoryTestMixin:
    def test_cleanup_unclean(self):
        """Test queue dir is not removed if it contains unexpected content"""
        q = self.queue()
        values = [b"0", b"1", b"2", b"3", b"4"]
        assert self.qpath.exists()
        for x in values:
            q.push(x)

        unexpected_file = self.qpath / "unexpected"
        unexpected_file.touch()

        for _ in values:
            q.pop()
        q.close()
        assert self.qpath.exists()
        assert unexpected_file.exists()


class FifoMemoryQueueTest(FifoTestMixin, QueueTestMixin, QueuelibTestCase):
    def queue(self):
        return FifoMemoryQueue()


class LifoMemoryQueueTest(LifoTestMixin, QueueTestMixin, QueuelibTestCase):
    def queue(self):
        return LifoMemoryQueue()


class FifoDiskQueueTest(
    FifoTestMixin,
    DirectoryTestMixin,
    PersistentTestMixin,
    QueueTestMixin,
    QueuelibTestCase,
):
    def queue(self):
        return FifoDiskQueue(self.qpath, chunksize=self.chunksize)

    def test_not_szhdr(self):
        q = self.queue()
        q.push(b"something")
        with (
            self.tempfilename().open("w+") as empty_file,
            mock.patch.object(q, "tailf", empty_file),
        ):
            assert q.peek() is None
            assert q.pop() is None
        q.close()

    def test_chunks(self):
        """Test chunks are created and removed"""
        values = [b"0", b"1", b"2", b"3", b"4"]
        q = self.queue()
        for x in values:
            q.push(x)

        chunks = list(self.qpath.glob("q*"))
        assert len(chunks) == 5 // self.chunksize + 1
        for _ in values:
            q.pop()

        chunks = list(self.qpath.glob("q*"))
        assert len(chunks) == 1
        q.close()

    def test_clear_removes_chunks(self):
        q = self.queue()
        for x in [b"0", b"1", b"2", b"3", b"4"]:
            q.push(x)
        q.clear()
        assert list(self.qpath.glob("q*")) == [self.qpath / "q00000"]
        q.close()


class ChunkSize1FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 1


class ChunkSize2FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 2


class ChunkSize3FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 3


class ChunkSize4FifoDiskQueueTest(FifoDiskQueueTest):
    chunksize = 4


class LifoDiskQueueTest(
    LifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return LifoDiskQueue(self.qpath)

    def test_file_size_shrinks(self):
        """Test size of queue file shrinks when popping items"""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.close()
        size = self.qpath.stat().st_size
        q = self.queue()
        q.pop()
        q.close()
        assert self.qpath.stat().st_size, size


class FifoSQLiteQueueTest(
    FifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return FifoSQLiteQueue(self.qpath)


class LifoSQLiteQueueTest(
    LifoTestMixin, PersistentTestMixin, QueueTestMixin, QueuelibTestCase
):
    def queue(self):
        return LifoSQLiteQueue(self.qpath)


class SharedSQLiteTestMixin:
    """Tests for SQLite queues opened by more than one connection."""

    lifo = False
    item_count = 100

    def shared_queue(self, **kwargs):
        cls = LifoSQLiteQueue if self.lifo else FifoSQLiteQueue
        return cls(self.qpath, shared=True, **kwargs)

    def test_shared_close_keeps_empty_file(self):
        """close() in shared mode never deletes the database file."""
        q = self.shared_queue()
        q.push(b"a")
        q.pop()
        q.close()
        assert self.qpath.exists()
        q2 = self.shared_queue()
        assert q2.pop() is None
        q2.close()
        assert self.qpath.exists()

    def test_empty_bytes_distinct_from_no_record(self):
        q = self.shared_queue()
        assert q.pop() is None
        q.push(b"")
        assert q.peek() == b""
        assert q.pop() == b""
        assert q.pop() is None
        q.close()
        assert self.qpath.exists()

    def test_two_connections_pop_each_item_once(self):
        rounds = self.item_count // 2
        q = self.shared_queue()
        for index in range(self.item_count):
            q.push(str(index).encode())

        other = self.shared_queue()
        try:
            first = []
            second = []
            for _ in range(rounds):
                first.append(q.pop())
                second.append(other.pop())
        finally:
            other.close()
        popped = [item for item in first + second if item is not None]
        assert len(popped) == self.item_count
        assert len(set(popped)) == self.item_count
        assert q.pop() is None
        q.close()

    def test_lock_timeout_raises_not_none(self):
        """A busy database raises instead of looking like an empty queue."""
        q = self.shared_queue(lock_timeout=0.1)
        q.push(b"a")
        holder = sqlite3.connect(self.qpath, timeout=0)
        holder.execute("BEGIN EXCLUSIVE")
        try:
            with pytest.raises(sqlite3.OperationalError):
                q.pop()
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert q.pop() == b"a"
        q.close()

    def test_failed_commit_keeps_item(self):
        """If committing a pop fails, the item is not handed out or lost."""
        q = self.shared_queue(lock_timeout=0.1)
        q.push(b"a")
        holder = sqlite3.connect(self.qpath, timeout=0)
        holder.execute("BEGIN EXCLUSIVE")
        try:
            with pytest.raises(sqlite3.OperationalError):
                q.pop()
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert q.pop() == b"a"
        assert q.pop() is None
        q.close()

    def test_shared_across_processes(self):
        """Two processes pop under a barrier; one closes, the other keeps on.

        Every input marker must be returned exactly once, closing an empty
        shared queue must keep the file, and the temporary database file must
        be removable explicitly at the end.
        """
        rounds = self.item_count // 2
        q = self.shared_queue()
        for index in range(self.item_count):
            q.push(str(index).encode())
        q.close()

        ctx = multiprocessing.get_context("spawn")
        start = ctx.Barrier(2)
        round_barrier = ctx.Barrier(2)
        drain_done = ctx.Barrier(2)
        closed = ctx.Event()
        can_push = ctx.Event()
        results = ctx.Queue()
        closer = ctx.Process(
            target=_shared_queue_closer,
            args=(
                str(self.qpath),
                self.lifo,
                start,
                rounds,
                round_barrier,
                drain_done,
                closed,
                can_push,
                results,
            ),
        )
        consumer = ctx.Process(
            target=_shared_queue_consumer,
            args=(
                str(self.qpath),
                self.lifo,
                start,
                rounds,
                round_barrier,
                drain_done,
                closed,
                can_push,
                results,
            ),
        )
        closer.start()
        consumer.start()
        closer.join(60)
        consumer.join(60)

        assert closer.exitcode == 0
        assert consumer.exitcode == 0

        batches = []
        while not results.empty():
            kind, value = results.get()
            assert kind == "items", value
            batches.append(value)
        assert len(batches) == 2

        popped = sorted(int(item) for batch in batches for item in batch)
        assert len(popped) == self.item_count
        assert len(set(popped)) == self.item_count
        assert popped == list(range(self.item_count))

        # The closer closed an empty shared queue: the file must survive and
        # the database must be reusable by a fresh queue object.
        assert self.qpath.exists()
        q = self.shared_queue()
        assert len(q) == 0
        assert q.pop() is None
        q.close()

        # Shared mode never removes the file, so clean it up explicitly.
        self.qpath.unlink()
        assert not self.qpath.exists()


class FifoSQLiteSharedQueueTest(SharedSQLiteTestMixin, QueuelibTestCase):
    lifo = False

    def test_shared_fifo_ordering(self):
        closer = self.shared_queue()
        other = self.shared_queue()
        for item in [b"a", b"b", b"c", b"d"]:
            closer.push(item)
        assert closer.pop() == b"a"
        assert other.pop() == b"b"
        assert closer.pop() == b"c"
        assert other.pop() == b"d"
        closer.close()
        other.close()


class LifoSQLiteSharedQueueTest(SharedSQLiteTestMixin, QueuelibTestCase):
    lifo = True

    def test_shared_lifo_ordering(self):
        closer = self.shared_queue()
        other = self.shared_queue()
        for item in [b"a", b"b", b"c", b"d"]:
            closer.push(item)
        assert closer.pop() == b"d"
        assert other.pop() == b"c"
        assert closer.pop() == b"b"
        assert other.pop() == b"a"
        closer.close()
        other.close()
