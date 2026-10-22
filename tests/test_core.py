# SPDX-License-Identifier: MIT
"""Unit tests for fifo_directory_queue_utils.core."""

import os
import shutil
import tempfile
import unittest

from fifo_directory_queue_utils import DirectoryQueue, QueueError


class _FakeClock:
    """A deterministic clock that advances on every call.

    Using a fake clock means we never assert on wall-clock time and never need
    to sleep; ordering tests stay reproducible on any machine.
    """

    def __init__(self, start: float = 1_000_000.0, step: float = 0.1) -> None:
        self._now = start
        self._step = step

    def __call__(self) -> float:
        n = self._now
        self._now += self._step
        return n


class _StaticClock:
    """A clock that always returns the same value.

    Used to force two enqueues into the same filesystem second so we can test
    that the sequence suffix breaks ties correctly.
    """

    def __init__(self, value: float) -> None:
        self._value = value

    def __call__(self) -> float:
        return self._value


class TestDirectoryQueueLayout(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_creates_subdirectories(self) -> None:
        DirectoryQueue(self._tmp, clock=_FakeClock())
        for sub in ("data", "processing", "tmp"):
            self.assertTrue(os.path.isdir(os.path.join(self._tmp, sub)))


class TestEnqueueDequeue(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")
        self.clock = _FakeClock()
        self.q = DirectoryQueue(self._tmp, clock=self.clock)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_enqueue_returns_name_and_creates_file(self) -> None:
        name = self.q.enqueue("hello")
        self.assertIsInstance(name, str)
        self.assertTrue(os.path.isfile(os.path.join(self._tmp, "data", name)))

    def test_dequeue_returns_oldest_payload(self) -> None:
        a = self.q.enqueue("first")
        b = self.q.enqueue("second")
        # Sanity: b's name sorts after a's name lexicographically too, because
        # we use a float timestamp + 8-digit counter; both advance.
        self.assertLess(a, b)
        with self.q.dequeue() as item:
            self.assertEqual(item.payload, "first")
        with self.q.dequeue() as item:
            self.assertEqual(item.payload, "second")

    def test_dequeue_empty_raises_queue_error(self) -> None:
        with self.assertRaises(QueueError):
            self.q.dequeue()

    def test_dequeue_is_fifo_under_same_second(self) -> None:
        # Force both enqueues to share a clock tick.
        static = _StaticClock(1_500_000.0)
        q = DirectoryQueue(self._tmp, clock=static)
        q.enqueue("a")
        q.enqueue("b")
        q.enqueue("c")
        got = []
        for _ in range(3):
            with q.dequeue() as item:
                got.append(item.payload)
        self.assertEqual(got, ["a", "b", "c"])

    def test_dequeue_file_moved_to_processing_then_deleted(self) -> None:
        self.q.enqueue("payload")
        before = self.q.size()
        self.assertEqual(before, 1)
        with self.q.dequeue() as item:
            self.assertEqual(item.payload, "payload")
            # While inside the context, the file lives in processing/.
            self.assertEqual(self.q.size(), 0)
            self.assertEqual(self.q.processing_size(), 1)
            self.assertTrue(os.path.isfile(item.path))
        # Clean exit deletes the processing file.
        self.assertEqual(self.q.processing_size(), 0)
        self.assertFalse(os.path.exists(item.path))

    def test_exception_in_with_block_keeps_processing_file(self) -> None:
        self.q.enqueue("work")
        saved_path = None
        with self.assertRaises(RuntimeError):
            with self.q.dequeue() as item:
                saved_path = item.path
                raise RuntimeError("boom")
        assert saved_path is not None
        self.assertTrue(os.path.isfile(saved_path))
        self.assertEqual(self.q.processing_size(), 1)

    def test_name_property_returns_basename(self) -> None:
        self.q.enqueue("x")
        with self.q.dequeue() as item:
            self.assertEqual(item.name, os.path.basename(item.path))

    def test_enqueue_non_str_raises_type_error(self) -> None:
        with self.assertRaises(TypeError):
            self.q.enqueue(b"bytes-not-allowed")  # type: ignore[arg-type]

    def test_size_counts_only_files_in_data(self) -> None:
        self.q.enqueue("one")
        self.q.enqueue("two")
        self.assertEqual(self.q.size(), 2)
        self.q.enqueue("three")
        self.assertEqual(self.q.size(), 3)

    def test_size_ignores_directories_in_data(self) -> None:
        # Someone (or a buggy tool) left a subdirectory in data/; size should
        # not count it.
        os.makedirs(os.path.join(self._tmp, "data", "stray_dir"))
        self.q.enqueue("real")
        self.assertEqual(self.q.size(), 1)


class TestSweepTmp(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")
        self.q = DirectoryQueue(self._tmp, clock=_FakeClock())

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_sweep_removes_orphan_tmp_files(self) -> None:
        # Simulate a crashed enqueue: write a .tmp file directly into tmp/.
        stray = os.path.join(self._tmp, "tmp", "stray.tmp")
        with open(stray, "w", encoding="utf-8") as f:
            f.write("unfinished")
        # data/ is empty; dequeue should fail.
        with self.assertRaises(QueueError):
            self.q.dequeue()
        removed = self.q.sweep_tmp()
        self.assertEqual(removed, 1)
        self.assertFalse(os.path.exists(stray))

    def test_sweep_on_empty_tmp_returns_zero(self) -> None:
        self.assertEqual(self.q.sweep_tmp(), 0)


class TestRequeueProcessing(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")
        self.clock = _FakeClock(start=1_700_000.0, step=0.5)
        self.q = DirectoryQueue(self._tmp, clock=self.clock)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_requeue_moves_processing_to_back_of_data(self) -> None:
        # Enqueue three items, dequeue the first so it's in processing/.
        self.q.enqueue("a")
        self.q.enqueue("b")
        self.q.enqueue("c")
        first_name = None
        try:
            with self.q.dequeue() as item:
                self.assertEqual(item.payload, "a")
                first_name = item.name
                raise RuntimeError("simulate crash")
        except RuntimeError:
            pass
        # The file should still exist in processing/.
        self.assertEqual(self.q.processing_size(), 1)

        # Requeue it.
        self.q.requeue_processing(first_name)
        self.assertEqual(self.q.processing_size(), 0)
        self.assertEqual(self.q.size(), 3)

        # The requeued item should be dequeued LAST, because requeue stamps a
        # new, later name.
        got = []
        for _ in range(3):
            with self.q.dequeue() as item:
                got.append(item.payload)
        self.assertEqual(got, ["b", "c", "a"])

    def test_requeue_missing_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.q.requeue_processing("no_such_item")


class TestFsyncOption(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_fsync_true_round_trip(self) -> None:
        q = DirectoryQueue(self._tmp, clock=_FakeClock(), fsync=True)
        q.enqueue("durable")
        with q.dequeue() as item:
            self.assertEqual(item.payload, "durable")


class TestNameFormat(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="fdq_")

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_names_are_unique_per_enqueue(self) -> None:
        static = _StaticClock(1.0)
        q = DirectoryQueue(self._tmp, clock=static)
        names = {q.enqueue(str(i)) for i in range(50)}
        self.assertEqual(len(names), 50)


if __name__ == "__main__":
    unittest.main()
