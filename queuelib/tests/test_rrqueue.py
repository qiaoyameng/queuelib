import gc
import shutil
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
        assert self.q.close() == []

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
        assert self.q.close() == []


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



class CheckpointValidationTest(QueuelibTestCase):
    def test_valid(self):
        assert RoundRobinCheckpoint(("a",)) == RoundRobinCheckpoint(("a",), 0)
        cp = RoundRobinCheckpoint(("a", "b", "c"), 2)
        assert cp.keys == ("a", "b", "c")
        assert cp.position == 2

    def test_empty(self):
        cp = RoundRobinCheckpoint(())
        assert cp.keys == ()
        assert cp.position == 0

    def test_duplicate_keys_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(("a", "b", "a"))

    def test_position_out_of_range_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(("a", "b"), 2)
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(("a",), -1)

    def test_position_of_empty_checkpoint_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint((), 1)

    def test_non_integer_position_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(("a",), "0")
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(("a",), True)

    def test_scalar_keys_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint("abc")
        with pytest.raises(ValueError):
            RoundRobinCheckpoint(b"abc")

    def test_unhashable_key_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint((["a"],))

    def test_frozen(self):
        cp = RoundRobinCheckpoint(("a",))
        with pytest.raises(Exception):
            cp.position = 1  # type: ignore[misc]

    def test_from_value_default_position(self):
        assert RoundRobinCheckpoint.from_value(["a"]).position == 0

    def test_from_value_mapping_without_keys_rejected(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint.from_value({"position": 1})

    def test_from_value_validates(self):
        with pytest.raises(ValueError):
            RoundRobinCheckpoint.from_value({"keys": ("a", "a")})


@pytest.mark.parametrize(
    "value,expected_position",
    [
        (RoundRobinCheckpoint(("a", "b"), 1), 1),
        ({"keys": ("a", "b"), "position": 1}, 1),
        ({"keys": ["a", "b"]}, 0),
        (("a", "b"), 0),
        (["a", "b"], 0),
    ],
)
def test_checkpoint_from_value(value, expected_position):
    cp = RoundRobinCheckpoint.from_value(value)
    assert cp.keys == ("a", "b")
    assert cp.position == expected_position


class CheckpointMemoryRestartMixin:
    """Restart simulation for memory backends: subqueues survive in a shared
    buckets dict while the RoundRobinQueue itself is rebuilt from a checkpoint.
    """

    queue_cls = FifoMemoryQueue
    expected_sequence = [b"1", b"2", b"3", b"4", b"5", b"6"]
    multiple_restart_sequence = [b"1", b"2", b"4", b"3", b"5", b"6"]

    def make_store(self):
        buckets = {}

        def factory(key):
            return buckets.setdefault(key, track_closed(self.queue_cls)())

        return factory, buckets

    def reference_sequence(self, operations):
        factory, _ = self.make_store()
        q = RoundRobinQueue(factory)
        for key, item in operations:
            q.push(item, key)
        reference = []
        while True:
            item = q.pop()
            if item is None:
                break
            reference.append(item)
        assert q.close() == []
        return reference

    def test_restart_preserves_rotation(self):
        operations = [
            ("a", b"1"),
            ("b", b"2"),
            ("c", b"3"),
            ("a", b"4"),
            ("b", b"5"),
            ("c", b"6"),
        ]
        assert self.reference_sequence(operations) == self.expected_sequence
        for cut in range(7):
            factory, buckets = self.make_store()
            q = RoundRobinQueue(factory)
            for key, item in operations:
                q.push(item, key)
            before = [q.pop() for _ in range(cut)]
            checkpoint = q.close(checkpoint=True)
            # The checkpoint is consistent with the close() result: it lists
            # exactly the subqueues that still hold items.
            active_on_disk = {
                key for key, subqueue in buckets.items() if len(subqueue)
            }
            assert set(checkpoint.keys) == active_on_disk
            rebuilt = RoundRobinQueue(factory, checkpoint=checkpoint)
            after = []
            while True:
                item = rebuilt.pop()
                if item is None:
                    break
                after.append(item)
            assert before + after == self.expected_sequence, cut
            assert rebuilt.close(checkpoint=True) == RoundRobinCheckpoint(())

    def test_checkpoint_first_key_is_served_next(self):
        for cut in range(6):
            factory, buckets = self.make_store()
            q = RoundRobinQueue(factory)
            q.push(b"1", "a")
            q.push(b"2", "a")
            q.push(b"3", "b")
            q.push(b"4", "b")
            q.push(b"5", "c")
            q.push(b"6", "c")
            [q.pop() for _ in range(cut)]
            checkpoint = q.close(checkpoint=True)
            if checkpoint.keys:
                next_item = buckets[checkpoint.keys[0]].peek()
                rebuilt = RoundRobinQueue(factory, checkpoint=checkpoint)
                assert rebuilt.peek() == next_item
                assert rebuilt.pop() == next_item

    def test_checkpoint_keys_match_close_active(self):
        factory, _ = self.make_store()
        q = RoundRobinQueue(factory)
        q.push(b"1", "a")
        q.push(b"2", "b")
        q.pop()
        q.push(b"3", "c")
        q.push(b"4", "a")
        active = q.close()
        # Reopen with start_domains just to drive an identical close path.
        q2 = RoundRobinQueue(factory, start_domains=active)
        checkpoint = q2.close(checkpoint=True)
        assert set(checkpoint.keys) == set(active)

    def test_explicit_position_restores_rotation(self):
        factory, _ = self.make_store()
        q = RoundRobinQueue(
            factory, checkpoint=RoundRobinCheckpoint(("a", "b", "c"), 1)
        )
        q.push(b"1", "a")
        q.push(b"2", "b")
        q.push(b"3", "c")
        assert [q.pop(), q.pop(), q.pop()] == [b"2", b"3", b"1"]
        assert q.pop() is None

    def test_checkpoint_mapping_input(self):
        factory, _ = self.make_store()
        q = RoundRobinQueue(
            factory, checkpoint={"keys": ("a", "b"), "position": 1}
        )
        q.push(b"1", "a")
        q.push(b"2", "b")
        assert [q.pop(), q.pop()] == [b"2", b"1"]

    def test_keys_are_not_coerced_to_str(self):
        factory, buckets = self.make_store()
        seen = []

        def spy_factory(key):
            seen.append(key)
            return factory(key)

        q = RoundRobinQueue(spy_factory, checkpoint=RoundRobinCheckpoint((1, 2)))
        assert seen == [1, 2]
        q.push(b"x", 1)
        q.push(b"y", 2)
        q.push(b"z", 3)
        checkpoint = q.close(checkpoint=True)
        assert all(isinstance(key, int) for key in checkpoint.keys)
        assert set(checkpoint.keys) == {1, 2, 3}
        assert all(isinstance(key, int) for key in buckets)

    def test_start_domains_and_checkpoint_together_rejected(self):
        factory, _ = self.make_store()
        with pytest.raises(ValueError):
            RoundRobinQueue(
                factory,
                start_domains=["a"],
                checkpoint=RoundRobinCheckpoint(("b",)),
            )

    def test_corrupt_checkpoint_rejected_before_opening_queues(self):
        calls = []

        def factory(key):
            calls.append(key)
            return track_closed(FifoMemoryQueue)()

        with pytest.raises(ValueError):
            RoundRobinQueue(factory, checkpoint=["a", "a"])
        assert calls == []

    def test_qfactory_failure_on_restore_leaves_no_queue(self):
        class Boom(Exception):
            pass

        opened = []

        def factory(key):
            if key == "b":
                raise Boom(key)
            queue = track_closed(FifoMemoryQueue)()
            opened.append(queue)
            return queue

        with pytest.raises(Boom):
            RoundRobinQueue(
                factory, checkpoint=RoundRobinCheckpoint(("a", "b", "c"))
            )
        assert all(queue.closed for queue in opened)

    def test_qfactory_failure_on_push_leaves_clean_state(self):
        class Boom(Exception):
            pass

        factory, _ = self.make_store()
        q = RoundRobinQueue(factory)
        q.qfactory = lambda key: (_ for _ in ()).throw(Boom(key))
        with pytest.raises(Boom):
            q.push(b"x", "z")
        assert q.pop() is None
        assert q.close(checkpoint=True) == RoundRobinCheckpoint(())

    def test_popped_empty_then_pushed_again(self):
        factory, _ = self.make_store()
        q = RoundRobinQueue(factory)
        q.push(b"a", "1")
        q.push(b"b", "2")
        assert q.pop() == b"a"
        assert "1" not in q.queues
        q.push(b"c", "1")
        checkpoint = q.close(checkpoint=True)
        assert checkpoint == RoundRobinCheckpoint(("2", "1"))
        rebuilt = RoundRobinQueue(factory, checkpoint=checkpoint)
        assert [rebuilt.pop(), rebuilt.pop()] == [b"b", b"c"]

    def test_subqueue_close_failure_aborts_checkpoint(self):
        class FailClose(FifoMemoryQueue):
            def close(self):
                raise OSError("failed to close")

        q = RoundRobinQueue(
            lambda key: FailClose()
            if key == "bad"
            else track_closed(FifoMemoryQueue)()
        )
        q.push(b"a", "good")
        q.push(b"b", "bad")
        with pytest.raises(OSError):
            q.close(checkpoint=True)

    def test_subqueue_close_failure_on_pop_keeps_position(self):
        class FailClose(FifoMemoryQueue):
            def close(self):
                raise OSError("failed to close")

        q = RoundRobinQueue(lambda key: FailClose())
        q.push(b"a", "x")
        with pytest.raises(OSError):
            q.pop()
        assert "x" in q.queues
        assert "x" in q.key_queue

    def test_multiple_restarts(self):
        operations = [
            ("a", b"1"),
            ("b", b"2"),
            ("a", b"3"),
            ("c", b"4"),
            ("b", b"5"),
            ("c", b"6"),
        ]
        factory, _ = self.make_store()
        q = RoundRobinQueue(factory)
        for key, item in operations:
            q.push(item, key)
        drained = []
        checkpoint = None
        for _ in range(3):
            drained.append(q.pop())
            checkpoint = q.close(checkpoint=True)
            q = RoundRobinQueue(factory, checkpoint=checkpoint)
        remaining = []
        while True:
            item = q.pop()
            if item is None:
                break
            remaining.append(item)
        assert drained + remaining == self.multiple_restart_sequence
        assert q.close(checkpoint=True) == RoundRobinCheckpoint(())


class FifoMemoryCheckpointTest(CheckpointMemoryRestartMixin, QueuelibTestCase):
    queue_cls = FifoMemoryQueue


class LifoMemoryCheckpointTest(CheckpointMemoryRestartMixin, QueuelibTestCase):
    queue_cls = LifoMemoryQueue
    expected_sequence = [b"4", b"5", b"6", b"1", b"2", b"3"]
    multiple_restart_sequence = [b"3", b"5", b"6", b"1", b"2", b"4"]


class CheckpointDiskRestartMixin:
    """Real restart parity for disk backends: each restart opens fresh
    subqueues from the same directory using a mapping checkpoint, the way a
    caller would reload one from JSON.
    """

    queue_cls = FifoDiskQueue
    expected_sequence = [b"1", b"2", b"3", b"4", b"5", b"6"]

    def make_queue(self, key):
        return track_closed(self.queue_cls)(Path(self.qdir, str(key)))

    def test_restart_at_each_position(self):
        operations = [
            ("a", b"1"),
            ("b", b"2"),
            ("c", b"3"),
            ("a", b"4"),
            ("b", b"5"),
            ("c", b"6"),
        ]
        for cut in range(7):
            shutil.rmtree(self.qdir)
            Path(self.qdir).mkdir()
            q = RoundRobinQueue(self.make_queue)
            for key, item in operations:
                q.push(item, key)
            before = [q.pop() for _ in range(cut)]
            checkpoint = q.close(checkpoint=True)
            state = {
                "keys": list(checkpoint.keys),
                "position": checkpoint.position,
            }
            gc.collect()  # release file handles before reopening
            rebuilt = RoundRobinQueue(self.make_queue, checkpoint=state)
            after = []
            while True:
                item = rebuilt.pop()
                if item is None:
                    break
                after.append(item)
            assert before + after == self.expected_sequence, cut
            assert rebuilt.close(checkpoint=True) == RoundRobinCheckpoint(())


class FifoDiskCheckpointTest(CheckpointDiskRestartMixin, QueuelibTestCase):
    queue_cls = FifoDiskQueue


class LifoDiskCheckpointTest(CheckpointDiskRestartMixin, QueuelibTestCase):
    queue_cls = LifoDiskQueue
    expected_sequence = [b"4", b"5", b"6", b"1", b"2", b"3"]


class FifoSQLiteCheckpointTest(CheckpointDiskRestartMixin, QueuelibTestCase):
    queue_cls = FifoSQLiteQueue


class LifoSQLiteCheckpointTest(CheckpointDiskRestartMixin, QueuelibTestCase):
    queue_cls = LifoSQLiteQueue
    expected_sequence = [b"4", b"5", b"6", b"1", b"2", b"3"]


class LegacyStartDomainsTest(QueuelibTestCase):
    def test_legacy_behavior_unchanged(self):
        q = RoundRobinQueue(
            lambda key: track_closed(FifoMemoryQueue)(), start_domains=["1", "2"]
        )
        q.push(b"c", "1")
        q.push(b"d", "2")
        assert [q.pop(), q.pop()] == [b"d", b"c"]
        assert q.close() == []
