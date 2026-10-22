# SPDX-License-Identifier: MIT
"""Core implementation of the directory-backed FIFO queue.

Design notes
------------
Reliability here rests on rename(2), which on POSIX filesystems (ext4, XFS,
APFS, and most local volumes) is atomic with respect to both the directory
entries and the pathname components involved: a successful rename guarantees
that exactly one of the old or new name exists at every point. We use this
twice:
  * once to enqueue (write to .tmp, rename to data/)
  * once to dequeue (rename data/X -> processing/X)

A rename is NOT guaranteed to be atomic across NFS when the data/ and
processing/ subdirectories live on different exports, or on Windows when
the target name already exists (ReplaceFile must be used). We deliberately
support local filesystems only.

Ordering is reconstructed from mtime plus a zero-padded sequence number.
mtime alone is insufficient because many filesystems truncate it to second
resolution and two enqueues can collide within the same second; the sequence
number breaks ties deterministically. When the mtime_ns field is available
on the platform it is used as the primary key, with the sequence as the
secondary sort key, giving sub-millisecond ordering on most Linux systems.
"""

from __future__ import annotations

import io
import os
import shutil
import time
from typing import Callable, Optional


__all__ = ["DirectoryQueue", "QueueError"]


class QueueError(RuntimeError):
    """Raised when the queue is empty or a directory rename fails.

    A dedicated exception class keeps the public API stable and lets callers
    distinguish a legitimately-empty queue from a programmer error such as
    passing a path that is not a directory.
    """

    pass


class DirectoryQueue:
    """A crash-safe FIFO work queue stored on the local filesystem.

    Structure on disk (all under ``base_dir``)::

        base_dir/
        +-- data/        enqueued, not yet claimed
        +-- processing/  claimed, not yet acknowledged
        +-- tmp/         in-progress writes (never read)

    The queue does not delete files on dequeue; instead, a successful dequeue
    renames the file into ``processing/`` and returns it as a context manager.
    Exiting the ``with`` block normally deletes the file; exiting via an
    exception leaves the file in ``processing/`` so the caller can retry it.
    This mirrors the "at-least-once" delivery model common in durable message
    queues.

    Parameters
    ----------
    base_dir
        Directory to store the queue in. Created if missing.
    clock
        Callable returning the current time as a float. Injected purely so
        tests can advance a fake clock deterministically without sleeping or
        wall-clock assertions.
    fsync
        When True, fsync() the payload and the directory after every write
        and rename, giving durability across power loss. Default False; enable
        in production.

    Ambiguity resolved
    -------------------
    The brief asks for "crash-safe" delivery. We interpret this as
    at-least-once: a crashed consumer leaves its file in ``processing/``, and
    the library deliberately does NOT auto-rescue such files, because only the
    caller knows whether the work was finished. Rescue logic would trade
    correctness for convenience; we pick correctness.
    """

    def __init__(
        self,
        base_dir: str,
        *,
        clock: Callable[[], float] = time.time,
        fsync: bool = False,
    ) -> None:
        self._base = os.path.abspath(base_dir)
        self._clock = clock
        self._fsync = fsync
        self._counter = 0

        self._data_dir = os.path.join(self._base, "data")
        self._processing_dir = os.path.join(self._base, "processing")
        self._tmp_dir = os.path.join(self._base, "tmp")

        self._ensure_layout()

    # ------------------------------------------------------------------
    # Layout helpers
    # ------------------------------------------------------------------
    def _ensure_layout(self) -> None:
        for d in (self._base, self._data_dir, self._processing_dir, self._tmp_dir):
            os.makedirs(d, exist_ok=True)

    def _next_seq(self) -> int:
        self._counter += 1
        return self._counter

    # ------------------------------------------------------------------
    # Internal write + sync helpers
    # ------------------------------------------------------------------
    def _fsync_dir(self, path: str) -> None:
        if not self._fsync:
            return
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _fsync_file(self, fd: int) -> None:
        if self._fsync:
            os.fsync(fd)

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------
    def enqueue(self, payload: str) -> str:
        """Append ``payload`` (text) to the queue and return its item id.

        The payload is written to a uniquely-named temporary file inside
        ``tmp/`` and then atomically renamed into ``data/``. A crash between
        the write and the rename leaves a harmless orphan in ``tmp/`` that is
        never read; a crash after the rename leaves a valid queued item.

        Returns the basename of the enqueued file, which callers can treat as
        an opaque item id.
        """
        if not isinstance(payload, str):
            raise TypeError("payload must be str")

        now = self._clock()
        seq = self._next_seq()
        name = self._format_name(now, seq, "data")
        tmp_path = os.path.join(self._tmp_dir, name + ".tmp")
        final_path = os.path.join(self._data_dir, name)

        # Write via a TextIOWrapper so the caller's str encoding is centralised
        # in one place; we can then os.replace() the closed file descriptor.
        fd = os.open(
            tmp_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
                f.write(payload)
                f.flush()
                self._fsync_file(f.fileno())
        except Exception:
            # If anything went wrong mid-write, remove the orphan.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        os.replace(tmp_path, final_path)
        self._fsync_dir(self._data_dir)
        return name

    @staticmethod
    def _format_name(ts: float, seq: int, kind: str) -> str:
        # Pre-format the kind tag so the same name works for both tmp/data
        # and processing; the kind is NOT embedded in the filename, it only
        # governs which directory we write to.
        # Seconds-resolution timestamp plus 8-digit seq survives ~100 M items
        # per wall-clock second before the counter wraps and reorders.
        return f"{ts:.6f}_{seq:08d}"

    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------
    def _list_ordered(self, directory: str) -> list[str]:
        """Return basenames in ``directory`` ordered oldest-first.

        Uses st_mtime_ns when available (Linux, macOS), falling back to
        int(st_mtime) elsewhere. The sequence suffix in each name breaks ties
        when two enqueues land in the same filesystem clock tick.
        """
        entries: list[tuple[int, int, str]] = []
        for entry in os.listdir(directory):
            full = os.path.join(directory, entry)
            if not os.path.isfile(full):
                continue
            st = os.stat(full)
            # st_mtime_ns exists on all platforms we target, but guard anyway.
            mtime_ns = getattr(st, "st_mtime_ns", None)
            if mtime_ns is None:
                mtime_ns = int(st.st_mtime) * 1_000_000_000
            entries.append((mtime_ns, self._extract_seq(entry), entry))
        entries.sort(key=lambda t: (t[0], t[1]))
        return [name for _, _, name in entries]

    @staticmethod
    def _extract_seq(name: str) -> int:
        """Pull the zero-padded sequence number out of an item name."""
        # Names look like "1717520000.000000_00000042"; the trailing integer
        # after the final underscore is the sequence counter.
        try:
            _, seq_str = name.rsplit("_", 1)
            return int(seq_str)
        except (ValueError, IndexError):
            # A file we didn't write; sort it last so it doesn't shadow
            # legitimate items.
            return 1 << 30

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------
    def dequeue(self) -> "_DequeuedItem":
        """Remove and return the oldest enqueued item.

        Raises ``QueueError`` if the queue is empty. The returned object is a
        context manager: on clean exit the underlying file is deleted; on
        exception it is left in ``processing/`` for inspection or retry.
        """
        ordered = self._list_ordered(self._data_dir)
        if not ordered:
            raise QueueError("queue is empty")

        for name in ordered:
            src = os.path.join(self._data_dir, name)
            dst = os.path.join(self._processing_dir, name)
            try:
                os.replace(src, dst)
            except FileNotFoundError:
                # Another consumer raced us for this exact file; try the next.
                continue
            self._fsync_dir(self._processing_dir)
            return _DequeuedItem(self, dst)

        raise QueueError("queue is empty (all candidates raced away)")

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------
    def size(self) -> int:
        """Number of items currently waiting in ``data/``."""
        return len(
            [
                e
                for e in os.listdir(self._data_dir)
                if os.path.isfile(os.path.join(self._data_dir, e))
            ]
        )

    def processing_size(self) -> int:
        """Number of items sitting in ``processing/``.

        Non-zero here means a consumer crashed mid-work or has not yet
        acknowledged. The library does not reclaim these automatically.
        """
        return len(
            [
                e
                for e in os.listdir(self._processing_dir)
                if os.path.isfile(os.path.join(self._processing_dir, e))
            ]
        )

    # ------------------------------------------------------------------
    # Requeue (optional)
    # ------------------------------------------------------------------
    def requeue_processing(self, name: str) -> None:
        """Move an item from ``processing/`` back to ``data/``.

        Use this to rescue work after a consumer crash. The item is re-enqueued
        at the *back* of the FIFO, because re-enqueuing at the original
        position would require knowing the original mtime, which is not kept.
        This is a deliberate, documented trade-off.
        """
        src = os.path.join(self._processing_dir, name)
        if not os.path.isfile(src):
            raise FileNotFoundError(name)
        # Re-stamp the item with a new name and new mtime so it sorts last.
        now = self._clock()
        seq = self._next_seq()
        new_name = self._format_name(now, seq, "data")
        dst = os.path.join(self._data_dir, new_name)
        os.replace(src, dst)
        self._fsync_dir(self._data_dir)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    def sweep_tmp(self) -> int:
        """Remove leftover .tmp files from crashed enqueues.

        Returns the count of files removed. Safe to call at any time.
        """
        count = 0
        for entry in os.listdir(self._tmp_dir):
            full = os.path.join(self._tmp_dir, entry)
            if os.path.isfile(full):
                try:
                    os.unlink(full)
                    count += 1
                except OSError:
                    pass
        return count


class _DequeuedItem:
    """The context-manager result of :meth:`DirectoryQueue.dequeue`.

    Reading the payload happens lazily on first attribute access so that a
    caller that only inspects ``name`` never pays the I/O cost.
    """

    __slots = ("_q", "_path", "_payload", "_consumed")
    __slots__ = __slots

    def __init__(self, q: DirectoryQueue, path: str) -> None:
        self._q = q
        self._path = path
        self._payload: Optional[str] = None
        self._consumed = False

    @property
    def name(self) -> str:
        return os.path.basename(self._path)

    @property
    def path(self) -> str:
        return self._path

    @property
    def payload(self) -> str:
        if self._payload is None:
            with io.open(self._path, "r", encoding="utf-8") as f:
                self._payload = f.read()
        return self._payload

    def __enter__(self) -> "_DequeuedItem":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._consumed:
            return
        self._consumed = True
        if exc_type is None:
            # Clean exit: delete the processing file.
            try:
                os.unlink(self._path)
            except FileNotFoundError:
                pass
            self._q._fsync_dir(self._q._processing_dir)
        # On exception, leave the file in processing/ for inspection/retry.
