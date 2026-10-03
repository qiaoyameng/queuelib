from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import textwrap
from abc import abstractmethod
from pathlib import Path
from unittest import mock

import pytest

from queuelib import (
    FifoDiskQueue,
    FifoDiskQueueError,
    FifoMemoryQueue,
    FifoSQLiteQueue,
    LifoDiskQueue,
    LifoMemoryQueue,
    LifoSQLiteQueue,
)
from queuelib.queue import BaseQueue
from queuelib.tests import DummyQueue, QueuelibTestCase


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


class DurableFifoDiskQueueTest(
    FifoTestMixin,
    DirectoryTestMixin,
    PersistentTestMixin,
    QueueTestMixin,
    QueuelibTestCase,
):
    chunksize = 100000

    def queue(self):
        return FifoDiskQueue(self.qpath, chunksize=self.chunksize, durable=True)

    def _reopen(self):
        return FifoDiskQueue(self.qpath, chunksize=self.chunksize, durable=True)

    def _drain(self, q):
        values = []
        while True:
            value = q.pop()
            if value is None:
                break
            values.append(value)
        return values

    def test_commit_without_close_persists(self):
        """A successful commit survives reopening with no close() call."""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.commit()
        q.pop()
        q.commit()
        del q

        q = self._reopen()
        assert len(q) == 1
        assert q.pop() == b"b"
        assert q.pop() is None
        q.close()

    def test_uncommitted_push_is_rolled_back(self):
        """Nothing pushed after the last commit may survive a crash."""
        q = self.queue()
        q.push(b"committed")
        q.commit()
        q.push(b"uncommitted1")
        q.push(b"uncommitted2")
        del q

        q = self._reopen()
        assert self._drain(q) == [b"committed"]
        q.close()

    def test_uncommitted_pop_is_rolled_back(self):
        """A record whose pop was not committed must never be lost."""
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.commit()
        assert q.pop() == b"a"
        del q

        q = self._reopen()
        assert self._drain(q) == [b"a", b"b"]
        q.close()

    def test_repeated_commit_and_reopen(self):
        q = self.queue()
        for i in range(7):
            q.push(f"item{i}".encode())
            q.commit()
            q = self._reopen()
        for i in range(6):
            assert q.pop() == f"item{i}".encode()
            q.commit()
            q = self._reopen()
        assert q.pop() == b"item6"
        q.commit()
        q = self._reopen()
        assert q.pop() is None
        q.close()

    def test_commit_empty_is_safe(self):
        q = self.queue()
        q.commit()
        q.commit()
        q.push(b"x")
        q.commit()
        q.pop()
        q.commit()
        q = self._reopen()
        assert q.pop() is None
        q.close()

    def test_clear_is_durable(self):
        q = self.queue()
        for value in [b"a", b"b", b"c"]:
            q.push(value)
        q.commit()
        q.clear()
        del q

        q = self._reopen()
        assert q.pop() is None
        assert len(q) == 0
        q.push(b"after-clear")
        q.commit()
        q = self._reopen()
        assert q.pop() == b"after-clear"
        q.close()

    def test_chunk_switch_boundary_is_committed_atomically(self):
        q = self.queue()
        q.push(b"a")
        q.push(b"b")
        q.commit()
        assert q.pop() == b"a"
        q.commit()
        # b is the last record of chunk 0; the next push rolls over.
        q.push(b"c")
        del q

        q = self._reopen()
        assert self._drain(q) == [b"b"]
        q.close()

    def test_old_tail_chunk_kept_until_metadata_published(self):
        """A fully consumed chunk must exist on disk while metadata still
        references it."""
        q = self.queue()
        q.push(b"a")
        assert q.pop() == b"a"
        chunk0 = self.qpath / "q00000"
        assert chunk0.exists()
        del q

        q = self._reopen()
        assert q.pop() is None
        q.close()

    def test_consumed_boundary_chunk_is_not_reaped_before_commit(self):
        """Popping the last record of a chunk advances the tail in memory,
        but the old chunk must survive a crash until that pop is committed."""
        q = self.queue()
        for i in range(self.chunksize + 1):
            q.push(f"v{i}".encode())
        q.commit()
        for _ in range(self.chunksize):
            assert q.pop() is not None
        # The tail now points at the next chunk only in memory; no commit.
        assert (self.qpath / "q00000").exists()
        del q

        q = self._reopen()
        values = self._drain(q)
        assert values == [f"v{i}".encode() for i in range(self.chunksize + 1)]
        q.close()

    def test_inconsistent_head_tail_metadata_raises(self):
        q = self.queue()
        q.push(b"a")
        q.commit()
        info = json.loads((self.qpath / "info.json").read_text())
        info["head"] = [0, 0]
        info["tail"] = [1, 0, 0]
        (self.qpath / "info.json").write_text(json.dumps(info))
        del q

        with pytest.raises(FifoDiskQueueError):
            self._reopen()

    def test_non_dict_metadata_raises(self):
        q = self.queue()
        q.push(b"a")
        q.commit()
        (self.qpath / "info.json").write_text("1234")
        del q

        with pytest.raises(FifoDiskQueueError):
            self._reopen()

    def test_old_tail_chunk_removed_after_commit(self):
        q = self.queue()
        for i in range(self.chunksize):
            q.push(f"v{i}".encode())
        for _ in range(self.chunksize):
            q.pop()
        q.commit()
        assert not (self.qpath / "q00000").exists()
        assert (self.qpath / "q00001").exists()
        q = self._reopen()
        assert q.pop() is None
        q.close()

    def test_reclaim_failure_after_metadata_keeps_queue_consistent(self):
        """Metadata is published before reaping; a failed directory flush
        after reaping leaves both metadata and on-disk chunks consistent."""
        q = self.queue()
        for i in range(self.chunksize):
            q.push(f"v{i}".encode())
        for _ in range(self.chunksize):
            q.pop()

        original_fsync_dir = q._fsync_dir
        calls = {"n": 0}

        def failing_on_second():
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("directory flush failed after reaping")
            original_fsync_dir()

        q._fsync_dir = failing_on_second  # type: ignore[method-assign]
        with pytest.raises(OSError):
            q.commit()
        del q

        q = self._reopen()
        assert q.pop() is None
        assert len(q) == 0
        q.close()

    def test_data_flush_failure_publishes_nothing(self):
        """If the data flush fails, metadata must not move forward."""
        q = self.queue()
        q.push(b"already-committed")
        q.commit()
        q.push(b"not-durable")

        def fail_flush(_fileobj):
            raise OSError("data flush failed")

        q._fsync_file = fail_flush  # type: ignore[method-assign]
        with pytest.raises(OSError):
            q.commit()
        del q

        q = self._reopen()
        assert self._drain(q) == [b"already-committed"]
        q.close()

    def test_metadata_publish_failure_publishes_nothing(self):
        """If metadata publication fails, the old info.json stays valid and
        the sealed head chunk is not lost."""
        q = self.queue()
        q.push(b"old")
        q.commit()

        original_publish = q._publish_info

        def fail_once(info):
            fail_once.calls += 1
            if fail_once.calls == 1:
                raise OSError("rename failed")
            original_publish(info)

        fail_once.calls = 0
        q._publish_info = fail_once  # type: ignore[method-assign]
        q.push(b"new")
        with pytest.raises(OSError):
            q.commit()
        # Retrying after the transient failure commits normally.
        q.commit()
        del q

        q = self._reopen()
        assert self._drain(q) == [b"old", b"new"]
        q.close()

    def test_torn_tail_record_is_never_read(self):
        """A length header claiming more bytes than exist must not be read
        back as data, and must not wipe the committed record before it."""
        q = self.queue()
        q.push(b"good")
        q.commit()
        head_chunk = max(self.qpath.glob("q*"))
        with head_chunk.open("ab") as fh:
            fh.write(struct.pack(">L", 5) + b"abc")
        del q

        q = self._reopen()
        assert len(q) == 1
        assert q.peek() == b"good"
        assert q.pop() == b"good"
        assert q.peek() is None
        assert q.pop() is None
        q.close()

    def test_torn_length_header_is_never_read(self):
        q = self.queue()
        q.push(b"good")
        q.commit()
        head_chunk = max(self.qpath.glob("q*"))
        with head_chunk.open("ab") as fh:
            fh.write(b"\x00\x01")
        del q

        q = self._reopen()
        assert q.pop() == b"good"
        assert q.pop() is None
        q.close()

    def test_corrupt_interior_chunk_raises_and_keeps_data(self):
        """Damage in a non-head chunk is an error, not an empty queue."""
        q = self.queue()
        q.push(b"x")
        q.push(b"y")
        q.commit()
        chunk = self.qpath / "q00000"
        data = chunk.read_bytes()
        chunk.write_bytes(data[:-1])
        del q

        with pytest.raises(FifoDiskQueueError):
            self._reopen()
        assert chunk.exists()
        assert chunk.read_bytes() == data[:-1]

    def test_missing_interior_chunk_raises(self):
        q = self.queue()
        q.push(b"x")
        q.push(b"y")
        q.commit()
        (self.qpath / "q00000").unlink()
        del q

        with pytest.raises(FifoDiskQueueError):
            self._reopen()

    def test_corrupt_info_raises_instead_of_wiping(self):
        q = self.queue()
        q.push(b"x")
        q.commit()
        chunk = self.qpath / "q00000"
        assert chunk.exists()
        (self.qpath / "info.json").write_text("{not json")
        del q

        with pytest.raises(FifoDiskQueueError):
            self._reopen()
        assert chunk.exists()

    def test_stale_orphan_chunks_are_reaped(self):
        """A chunk left behind by an uncommitted head rollover is garbage
        on the next open and is removed, while committed data is untouched."""
        q = self.queue()
        q.push(b"a")
        q.commit()
        head_number = max(int(path.name[1:]) for path in self.qpath.glob("q*"))
        # Simulate a process that started a rollover but never committed it.
        for number in (head_number + 2, head_number + 3):
            (self.qpath / f"q{number:05d}").write_bytes(b"\x00\x00\x00\x01z")
        del q

        q = self._reopen()
        assert self._drain(q) == [b"a"]
        for number in (head_number + 2, head_number + 3):
            assert not (self.qpath / f"q{number:05d}").exists()
        q.push(b"after-reap")
        q.commit()
        q = self._reopen()
        assert q.pop() == b"after-reap"
        q.close()

    def test_leftover_temp_metadata_is_ignored(self):
        q = self.queue()
        q.push(b"a")
        q.commit()
        (self.qpath / "info.json.tmp").write_text("{partial")
        del q

        q = self._reopen()
        assert q.pop() == b"a"
        assert not (self.qpath / "info.json.tmp").exists()
        q.close()

    def test_non_durable_mode_still_supported(self):
        """Without durable=True the class keeps its historical behaviour:
        close() is what persists the position."""
        q = FifoDiskQueue(self.qpath, chunksize=self.chunksize)
        q.push(b"a")
        q.close()
        q = FifoDiskQueue(self.qpath, chunksize=self.chunksize)
        assert q.pop() == b"a"
        q.close()


class ChunkSize1DurableFifoDiskQueueTest(DurableFifoDiskQueueTest):
    chunksize = 1


class ChunkSize2DurableFifoDiskQueueTest(DurableFifoDiskQueueTest):
    chunksize = 2


class ChunkSize3DurableFifoDiskQueueTest(DurableFifoDiskQueueTest):
    chunksize = 3



    def test_hard_process_exit_keeps_commit(self):
        """Use a subprocess that exits with os._exit() (no cleanup, no
        close) to prove a successful commit is truly durable."""
        repo_root = Path(__file__).resolve().parents[2]
        env = dict(os.environ)
        env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")

        for phase, expected in (("push", [b"p1", b"p2"]), ("pop", [b"p2"])):
            qpath = self.qpath / f"hardexit-{phase}"
            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    textwrap.dedent(
                        """
                        import os, sys
                        from queuelib import FifoDiskQueue
                        path, phase = sys.argv[1], sys.argv[2]
                        chunksize = int(sys.argv[3])
                        q = FifoDiskQueue(path, chunksize=chunksize, durable=True)
                        q.push(b"p1")
                        q.push(b"p2")
                        q.commit()
                        if phase == "pop":
                            assert q.pop() == b"p1"
                            q.commit()
                        os._exit(0)
                        """
                    ),
                    str(qpath),
                    phase,
                    str(self.chunksize),
                ],
                check=True,
                env=env,
            )
            q = FifoDiskQueue(qpath, chunksize=self.chunksize, durable=True)
            assert self._drain(q) == expected, phase
            q.close()



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
