# fifo_directory_queue_utils

A small, dependency-free Python library that implements a crash-safe FIFO work queue on the local filesystem. Items are enqueued by writing a temporary file and renaming it into `data/`, and dequeued by renaming the oldest file from `data/` into `processing/` and returning it as a context manager that deletes the file on clean exit.

## Usage

```python
from fifo_directory_queue_utils import DirectoryQueue, QueueError

q = DirectoryQueue("./myqueue")

q.enqueue("do thing one")
q.enqueue("do thing two")

try:
    with q.dequeue() as item:
        print(item.name)      # e.g. "1717520000.000000_00000001"
        print(item.payload)   # "do thing one"
        raise RuntimeError("simulate a crash")
except RuntimeError:
    pass

# Inspect queue depth.
print(q.size())             # items waiting in data/
print(q.processing_size())  # items left in processing/ by crashed consumers

# Rescue an item left in processing/ after a crash; re-enqueues it at the back.
q.requeue_processing(item.name)

# Tidy up orphaned .tmp files from interrupted enqueues.
q.sweep_tmp()
```

## Why this exists

The problem is the boring one: you have a long-running producer and one or more consumer processes on the same machine, and you want durable, ordered handoff between them without standing up Redis, RabbitMQ, or SQLite. A directory of files is the natural data structure; the only question is how to make the operations safe.

The trade-off is explicit and narrow. We rely on `rename(2)` being atomic on POSIX local filesystems (ext4, XFS, APFS). That gives us crash-safe enqueue and dequeue: a failure at any point leaves either the old state or the new state, never a half-written file visible to consumers. In exchange, the library does not support NFS, cross-filesystem renames, or Windows in-place replacement. If you need those, use a database.

Ordering is reconstructed from `st_mtime_ns` (falling back to integer seconds) plus a zero-padded per-process sequence number embedded in the filename. The sequence is necessary because many filesystems truncate mtime to second resolution, and two enqueues within the same second would otherwise tie. On Linux, `st_mtime_ns` gives sub-millisecond ordering in practice; the sequence is the tie-breaker.

## The awkward edge

Dequeue does not delete the file immediately. It renames the file into `processing/` and returns it as a context manager. If the consumer block exits cleanly, the file is deleted; if it exits via an exception, the file is left in `processing/`. This is at-least-once delivery: a crashed consumer leaves work behind, and only the caller knows whether that work was finished. The library deliberately does not auto-rescue stranded items — use `requeue_processing(name)` to move an item back to `data/` when you have decided it is safe to retry. Note that `requeue_processing` re-stamps the item with a new name and timestamp, so it goes to the back of the queue, not its original position.

### Exported names

- `DirectoryQueue` — the queue class. Constructor: `DirectoryQueue(base_dir, *, clock=time.time, fsync=False)`.
- `QueueError` — raised by `dequeue()` when the queue is empty.

Methods on `DirectoryQueue`:

- `enqueue(payload: str) -> str` — append text, return the item name.
- `dequeue() -> context manager` — claim the oldest item; raises `QueueError` if empty.
- `size() -> int` — count of items waiting in `data/`.
- `processing_size() -> int` — count of items in `processing/`.
- `requeue_processing(name: str) -> None` — move an item from `processing/` to the back of `data/`.
- `sweep_tmp() -> int` — remove orphaned `.tmp` files, return the count removed.

The dequeued context manager exposes `.name`, `.path`, and `.payload`.

## Running the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```

No third-party packages are required.
