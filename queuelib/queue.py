from __future__ import annotations

import json
import os
import sqlite3
import struct
import sys
from abc import abstractmethod
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Any, BinaryIO, Literal, cast


def _clear(queue: Any) -> None:
    """Remove every item from *queue*, popping them one by one if it does not
    implement ``clear()``."""
    clear = getattr(queue, "clear", None)
    if clear is not None:
        clear()
        return
    while queue.pop() is not None:
        pass


class _BaseQueueMeta(type):
    """
    Metaclass to check queue classes against the necessary interface
    """

    def __instancecheck__(cls, instance: Any) -> bool:
        return cls.__subclasscheck__(  # pylint: disable=no-value-for-parameter
            type(instance)
        )

    def __subclasscheck__(cls, subclass: Any) -> bool:
        return (
            hasattr(subclass, "push")
            and callable(subclass.push)
            and hasattr(subclass, "pop")
            and callable(subclass.pop)
            and hasattr(subclass, "peek")
            and callable(subclass.peek)
            and hasattr(subclass, "close")
            and callable(subclass.close)
            and hasattr(subclass, "__len__")
            and callable(subclass.__len__)
        )


class BaseQueue(metaclass=_BaseQueueMeta):
    @abstractmethod
    def push(self, obj: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def pop(self) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def peek(self) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        pass


class FifoMemoryQueue:
    """In-memory FIFO queue, API compliant with FifoDiskQueue."""

    def __init__(self) -> None:
        self.q: deque[Any] = deque()

    def push(self, obj: Any) -> None:
        self.q.append(obj)

    def pop(self) -> Any | None:
        return self.q.popleft() if self.q else None

    def peek(self) -> Any | None:
        return self.q[0] if self.q else None

    def clear(self) -> None:
        self.q.clear()

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self.q)


class LifoMemoryQueue(FifoMemoryQueue):
    """In-memory LIFO queue, API compliant with LifoDiskQueue."""

    def pop(self) -> Any | None:
        return self.q.pop() if self.q else None

    def peek(self) -> Any | None:
        return self.q[-1] if self.q else None


class FifoDiskQueueError(Exception):
    """Raised when the on-disk state of a :class:`FifoDiskQueue` is corrupt.

    The queue is never cleared automatically when this is raised: data that
    can be proven valid is left in place for inspection or manual recovery.
    """


class FifoDiskQueue:
    """Persistent FIFO queue.

    Records are stored length-prefixed (4-byte big-endian unsigned length
    header followed by the payload) in numbered chunk files (``q00000``,
    ``q00001``, ...), each holding up to *chunksize* records. ``info.json``
    records the current head and tail positions.

    By default the position metadata is only persisted by :meth:`close`, as
    in earlier releases. Pass ``durable=True`` to opt into explicit
    durability: every record appended before a successful call to
    :meth:`commit` is durable, and every record whose pop has been committed
    can never come back, even if the process is killed immediately after.

    A successful :meth:`commit` provides the following guarantees, enforced by
    ordering the on-disk operations as
    data write -> data flush -> atomic metadata publication -> chunk reaping:

    1. Every ``push()`` whose record is fully on disk is listed as committed
       by ``info.json`` only after the record (and, on a chunk switch, the
       sealed chunk) has been flushed to disk.
    2. A fully consumed tail chunk is deleted only after metadata advancing the
       tail past it has been published, so a chunk that recovery may still
       need is never removed early.
    3. A torn (short) record at the tail of a chunk is never returned. On open,
       only the uncommitted tail of the head chunk is truncated; corruptions
       elsewhere raise :class:`FifoDiskQueueError` instead of dropping data.
    """

    szhdr_format = ">L"
    szhdr_size = struct.calcsize(szhdr_format)
    _info_name = "info.json"
    _tmp_info_name = "info.json.tmp"

    def __init__(
        self,
        path: str | os.PathLike[str],
        chunksize: int = 100000,
        durable: bool = False,
    ) -> None:
        self.path = str(path)
        if not Path(path).exists():
            Path(path).mkdir(parents=True)
        self.durable = bool(durable)
        self.info = self._loadinfo(chunksize)
        self.chunksize = int(self.info["chunksize"])
        if self.chunksize <= 0:
            raise FifoDiskQueueError(
                f"Invalid chunksize {self.chunksize} in {self._infopath()}"
            )
        self._reclaimed: list[int] = []
        self._recover()
        self.headf = self._openchunk(self.info["head"][0], "ab+")
        self.tailf = self._openchunk(self.info["tail"][0])
        os.lseek(self.tailf.fileno(), self.info["tail"][2], os.SEEK_SET)

    def push(self, string: bytes) -> None:
        if not isinstance(string, bytes):
            raise TypeError(f"Unsupported type: {type(string).__name__}")
        hnum, hpos = self.info["head"]
        hpos += 1
        szhdr = struct.pack(self.szhdr_format, len(string))
        self._write_all(self.headf.fileno(), szhdr + string)
        if hpos == self.chunksize:
            if self.durable:
                # The sealed chunk must be durable before metadata points the
                # tail to it as a full, readable chunk.
                self._fsync_file(self.headf)
            hpos = 0
            hnum += 1
            self.headf.close()
            self.headf = self._openchunk(hnum, "ab+")
            if self.durable:
                self._fsync_dir()
        self.info["size"] += 1
        self.info["head"] = [hnum, hpos]

    def _openchunk(self, number: int, mode: Literal["rb", "ab+"] = "rb") -> BinaryIO:
        return Path(self.path, self._chunkname(number)).open(mode)

    @staticmethod
    def _chunkname(number: int) -> str:
        return f"q{number:05d}"

    @staticmethod
    def _is_chunkname(name: str) -> bool:
        return len(name) == 6 and name[0] == "q" and name[1:].isdigit()

    @staticmethod
    def _write_all(fd: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]

    @staticmethod
    def _read_exact(fd: int, size: int) -> bytes | None:
        """Read exactly *size* bytes, or return ``None`` on a clean EOF.

        A short read (some bytes available, then EOF) returns the short
        prefix instead, letting callers detect a torn record rather than
        treating it as valid data.
        """
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining == size:
            return None
        return b"".join(chunks)

    def pop(self) -> bytes | None:
        tnum, tcnt, toffset = self.info["tail"]
        if [tnum, tcnt] >= self.info["head"]:
            return None
        tfd = self.tailf.fileno()
        szhdr = self._read_exact(tfd, self.szhdr_size)
        if szhdr is None:
            return None
        if len(szhdr) < self.szhdr_size:
            os.lseek(tfd, toffset, os.SEEK_SET)
            return None
        (size,) = struct.unpack(self.szhdr_format, szhdr)
        data = self._read_exact(tfd, size)
        if data is None or len(data) < size:
            # Torn record: never consume or return it.
            os.lseek(tfd, toffset, os.SEEK_SET)
            return None
        tcnt += 1
        toffset += self.szhdr_size + size
        if tcnt == self.chunksize and tnum <= self.info["head"][0]:
            oldnum = tnum
            tcnt = toffset = 0
            tnum += 1
            self.tailf.close()
            if self.durable:
                # Recovery still needs this chunk until metadata advancing
                # past it has been published; reap it in commit().
                self._reclaimed.append(oldnum)
            else:
                Path(self.path, self._chunkname(oldnum)).unlink()
            self.tailf = self._openchunk(tnum)
        self.info["size"] -= 1
        self.info["tail"] = [tnum, tcnt, toffset]
        return data

    def peek(self) -> bytes | None:
        tnum, tcnt, _ = self.info["tail"]
        if [tnum, tcnt] >= self.info["head"]:
            return None
        tfd = self.tailf.fileno()
        tfd_initial_pos = os.lseek(tfd, 0, os.SEEK_CUR)
        szhdr = self._read_exact(tfd, self.szhdr_size)
        if szhdr is None or len(szhdr) < self.szhdr_size:
            os.lseek(tfd, tfd_initial_pos, os.SEEK_SET)
            return None
        (size,) = struct.unpack(self.szhdr_format, szhdr)
        data = self._read_exact(tfd, size)
        if data is None or len(data) < size:
            os.lseek(tfd, tfd_initial_pos, os.SEEK_SET)
            return None
        os.lseek(tfd, tfd_initial_pos, os.SEEK_SET)
        return data

    def clear(self) -> None:
        self.headf.close()
        self.tailf.close()
        for chunk in self._iter_chunk_paths():
            chunk.unlink()
        if self.durable:
            self._fsync_dir()
        self.info["size"] = 0
        self.info["tail"] = [0, 0, 0]
        self.info["head"] = [0, 0]
        self._reclaimed = []
        self.headf = self._openchunk(0, "ab+")
        self.tailf = self._openchunk(0)
        if self.durable:
            self.commit()

    def commit(self) -> None:
        """Persist the current push/pop progress.

        In durable mode this flushes the head chunk, publishes ``info.json``
        atomically (write-to-temp, flush, rename, flush the directory) and
        only then deletes tail chunks that the committed tail no longer
        needs. After this returns without error, every committed push survives
        a hard process exit and every committed pop stays consumed.

        In non-durable mode this is a best-effort save of the position, with
        no flush guarantees; closing the queue remains the normal way to
        persist it.
        """
        if self.durable:
            self._fsync_file(self.headf)
        self._publish_info(self.info)
        if self.durable:
            self._remove_reclaimed()

    def close(self) -> None:
        self.commit()
        self.headf.close()
        self.tailf.close()
        if len(self) == 0:
            self._cleanup()

    def __len__(self) -> int:
        return cast("int", self.info["size"])

    def _iter_chunk_paths(self) -> list[Path]:
        return [
            path
            for path in Path(self.path).iterdir()
            if self._is_chunkname(path.name)
        ]

    def _fsync_file(self, fileobj: BinaryIO) -> None:
        os.fsync(fileobj.fileno())

    def _fsync_dir(self) -> None:
        """Flush directory entry changes (best effort on Windows)."""
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        fd = os.open(self.path, flags)
        try:
            try:
                os.fsync(fd)
            except OSError:
                # Directories cannot be fsynced on Windows.
                if sys.platform != "win32":
                    raise
        finally:
            os.close(fd)

    def _remove_reclaimed(self) -> None:
        for number in self._reclaimed:
            path = Path(self.path, self._chunkname(number))
            with suppress(FileNotFoundError):
                path.unlink()
        if self._reclaimed:
            self._reclaimed = []
            self._fsync_dir()

    def _loadinfo(self, chunksize: int) -> dict[str, Any]:
        infopath = self._infopath()
        if infopath.exists():
            try:
                info = cast("dict[str, Any]", json.loads(infopath.read_text()))
            except (json.JSONDecodeError, OSError) as exc:
                raise FifoDiskQueueError(
                    f"Unreadable queue metadata {infopath}: {exc}"
                ) from exc
            try:
                info["chunksize"] = int(info["chunksize"])
                info["size"] = int(info["size"])
                tail = info["tail"]
                head = info["head"]
                (tnum, tcnt, toffset) = (int(tail[0]), int(tail[1]), int(tail[2]))
                (hnum, hcnt) = (int(head[0]), int(head[1]))
            except (KeyError, TypeError, ValueError, IndexError) as exc:
                raise FifoDiskQueueError(
                    f"Malformed queue metadata {infopath}: {exc}"
                ) from exc
            if (
                info["chunksize"] <= 0
                or info["size"] < 0
                or tnum < 0
                or tcnt < 0
                or toffset < 0
                or hnum < tnum
                or (hnum == tnum and hcnt < tcnt)
                or not 0 <= hcnt < info["chunksize"]
                or not 0 <= tcnt < info["chunksize"]
            ):
                raise FifoDiskQueueError(
                    f"Invalid queue metadata {infopath}: {info!r}"
                )
            info["tail"] = [tnum, tcnt, toffset]
            info["head"] = [hnum, hcnt]
        else:
            info = {
                "chunksize": chunksize,
                "size": 0,
                "tail": [0, 0, 0],
                "head": [0, 0],
            }
        return info

    def _publish_info(self, info: dict[str, Any]) -> None:
        payload = json.dumps(info)
        tmp = Path(self.path, self._tmp_info_name)
        with tmp.open("w") as fh:
            fh.write(payload)
            fh.flush()
            if self.durable:
                os.fsync(fh.fileno())
        os.replace(tmp, self._infopath())
        if self.durable:
            self._fsync_dir()

    def _saveinfo(self, info: dict[str, Any]) -> None:
        """Direct metadata write, kept for backwards compatibility."""
        self._infopath().write_text(json.dumps(info))

    def _infopath(self) -> Path:
        return Path(self.path, self._info_name)

    def _scan_chunk(
        self, number: int
    ) -> tuple[list[int], int, int] | None:
        """Walk complete records in a chunk file.

        Returns ``(offsets, end, trailing)`` where *offsets* holds the byte
        offset of every complete record, *end* is the offset just past the
        last complete record, and *trailing* is the number of bytes of an
        incomplete final record at EOF (a torn write). Returns ``None`` if the
        chunk file does not exist.
        """
        path = Path(self.path, self._chunkname(number))
        if not path.exists():
            return None
        offsets: list[int] = []
        with path.open("rb") as fh:
            fd = fh.fileno()
            while True:
                offset = os.lseek(fd, 0, os.SEEK_CUR)
                szhdr = self._read_exact(fd, self.szhdr_size)
                if szhdr is None:
                    return offsets, offset, 0
                if len(szhdr) < self.szhdr_size:
                    return offsets, offset, len(szhdr)
                (size,) = struct.unpack(self.szhdr_format, szhdr)
                data = self._read_exact(fd, size)
                if data is None:
                    return offsets, offset, self.szhdr_size
                if len(data) < size:
                    return offsets, offset, self.szhdr_size + len(data)
                offsets.append(offset)

    def _recover(self) -> None:
        """Validate on-disk state against the published metadata.

        Stale chunks (outside the metadata's [tail, head] range) and leftover
        temporary metadata files are removed. Every chunk the metadata still
        references must contain only complete records; the head chunk is
        truncated to its committed record count, dropping an uncommitted tail
        (including a torn final record). Any inconsistency that could mean
        committed data is lost raises :class:`FifoDiskQueueError` instead of
        silently clearing the queue.
        """
        tmpinfo = Path(self.path, self._tmp_info_name)
        if tmpinfo.exists():
            tmpinfo.unlink()

        tnum, tcnt, toffset = self.info["tail"]
        hnum, hcnt = self.info["head"]

        stale: list[Path] = []
        for path in self._iter_chunk_paths():
            number = int(path.name[1:])
            if number < tnum or number > hnum:
                stale.append(path)
        for path in stale:
            path.unlink()
        if stale and self.durable:
            self._fsync_dir()

        for number in range(tnum, hnum + 1):
            scanned = self._scan_chunk(number)
            if scanned is None:
                if number == hnum and hcnt == 0:
                    # The empty head chunk is (re)created by __init__.
                    continue
                raise FifoDiskQueueError(
                    f"Missing chunk {self._chunkname(number)} in {self.path}"
                )
            offsets, end, trailing = scanned
            count = len(offsets)
            if number < hnum:
                if count != self.chunksize:
                    raise FifoDiskQueueError(
                        f"Chunk {self._chunkname(number)} in {self.path} "
                        f"holds {count} record(s), expected {self.chunksize}"
                    )
                if trailing:
                    raise FifoDiskQueueError(
                        "Torn record in non-head chunk "
                        f"{self._chunkname(number)} in {self.path}"
                    )
            if number == tnum and tcnt:
                if tcnt > count:
                    raise FifoDiskQueueError(
                        f"Tail points past chunk {self._chunkname(tnum)} "
                        f"in {self.path}"
                    )
                # Offset just past the last consumed record.
                consumed_end = offsets[tcnt] if count > tcnt else end
                if consumed_end != toffset:
                    raise FifoDiskQueueError(
                        f"Tail offset mismatch in chunk "
                        f"{self._chunkname(tnum)} in {self.path}"
                    )
            if number == hnum:
                if count < hcnt:
                    raise FifoDiskQueueError(
                        f"Head chunk {self._chunkname(hnum)} in {self.path} "
                        f"holds {count} record(s), metadata expects {hcnt}"
                    )
                # Offset just past the last committed record.
                committed_end = offsets[hcnt] if count > hcnt else end
                size = Path(self.path, self._chunkname(number)).stat().st_size
                if committed_end != size:
                    # Drop only the uncommitted head tail, never committed data.
                    with Path(self.path, self._chunkname(number)).open("r+b") as fh:
                        fh.truncate(committed_end)

        committed = hnum * self.chunksize + hcnt - (tnum * self.chunksize + tcnt)
        if self.info["size"] != committed:
            raise FifoDiskQueueError(
                f"Size mismatch in {self._infopath()}: metadata says "
                f"{self.info['size']}, chunks account for {committed}"
            )

    def _cleanup(self) -> None:
        for x in Path(self.path).glob("q*"):
            x.unlink()
        with suppress(FileNotFoundError):
            Path(self.path, self._info_name).unlink()
        with suppress(FileNotFoundError):
            Path(self.path, self._tmp_info_name).unlink()
        with suppress(OSError):
            Path(self.path).rmdir()


class LifoDiskQueue:
    """Persistent LIFO queue."""

    SIZE_FORMAT = ">L"
    SIZE_SIZE = struct.calcsize(SIZE_FORMAT)

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.size: int
        self.path = str(path)
        if Path(path).exists():
            self.f = Path(path).open("rb+")  # noqa: SIM115
            qsize = self.f.read(self.SIZE_SIZE)
            (self.size,) = struct.unpack(self.SIZE_FORMAT, qsize)
            self.f.seek(0, os.SEEK_END)
        else:
            self.f = Path(path).open("wb+")  # noqa: SIM115
            self.f.write(struct.pack(self.SIZE_FORMAT, 0))
            self.size = 0

    def push(self, string: bytes) -> None:
        if not isinstance(string, bytes):
            raise TypeError(f"Unsupported type: {type(string).__name__}")
        self.f.write(string)
        ssize = struct.pack(self.SIZE_FORMAT, len(string))
        self.f.write(ssize)
        self.size += 1

    def pop(self) -> bytes | None:
        if not self.size:
            return None
        self.f.seek(-self.SIZE_SIZE, os.SEEK_END)
        (size,) = struct.unpack(self.SIZE_FORMAT, self.f.read())
        self.f.seek(-size - self.SIZE_SIZE, os.SEEK_END)
        data = self.f.read(size)
        self.f.seek(-size, os.SEEK_CUR)
        self.f.truncate()
        self.size -= 1
        return data

    def peek(self) -> bytes | None:
        if not self.size:
            return None
        self.f.seek(-self.SIZE_SIZE, os.SEEK_END)
        (size,) = struct.unpack(self.SIZE_FORMAT, self.f.read())
        self.f.seek(-size - self.SIZE_SIZE, os.SEEK_END)
        data = self.f.read(size)
        self.f.seek(0, os.SEEK_END)
        return data

    def clear(self) -> None:
        self.f.seek(0)
        self.f.write(struct.pack(self.SIZE_FORMAT, 0))
        self.f.truncate()
        self.size = 0

    def close(self) -> None:
        if self.size:
            self.f.seek(0)
            self.f.write(struct.pack(self.SIZE_FORMAT, self.size))
        self.f.close()
        if not self.size:
            Path(self.path).unlink()

    def __len__(self) -> int:
        return self.size


class FifoSQLiteQueue:
    _sql_create = "CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY AUTOINCREMENT, item BLOB)"
    _sql_size = "SELECT COUNT(*) FROM queue"
    _sql_push = "INSERT INTO queue (item) VALUES (?)"
    _sql_pop = "SELECT id, item FROM queue ORDER BY id LIMIT 1"
    _sql_del = "DELETE FROM queue WHERE id = ?"
    _sql_clear = "DELETE FROM queue"

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path).resolve()
        self._db = sqlite3.Connection(self._path, timeout=60)
        self._db.text_factory = bytes
        with self._db as conn:
            conn.execute(self._sql_create)

    def push(self, item: bytes) -> None:
        if not isinstance(item, bytes):
            raise TypeError(f"Unsupported type: {type(item).__name__}")
        with self._db as conn:
            conn.execute(self._sql_push, (item,))

    def pop(self) -> bytes | None:
        with self._db as conn:
            for id_, item in conn.execute(self._sql_pop):
                conn.execute(self._sql_del, (id_,))
                return cast("bytes", item)
        return None

    def peek(self) -> bytes | None:
        with self._db as conn:
            for _, item in conn.execute(self._sql_pop):
                return cast("bytes", item)
        return None

    def clear(self) -> None:
        with self._db as conn:
            conn.execute(self._sql_clear)

    def close(self) -> None:
        size = len(self)
        self._db.close()
        if not size:
            self._path.unlink()

    def __len__(self) -> int:
        with self._db as conn:
            return cast("int", next(conn.execute(self._sql_size))[0])


class LifoSQLiteQueue(FifoSQLiteQueue):
    _sql_pop = "SELECT id, item FROM queue ORDER BY id DESC LIMIT 1"
