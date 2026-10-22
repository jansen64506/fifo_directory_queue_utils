# SPDX-License-Identifier: MIT
"""fifo_directory_queue_utils: a crash-safe, dependency-free FIFO work queue.

Atomic enqueue and dequeue of arbitrary text payloads into a directory, with
insertion order reconstructed from filesystem mtime and a monotonic sequence
suffix. No database, no third-party packages.
"""

from .core import DirectoryQueue, QueueError

__all__ = ["DirectoryQueue", "QueueError"]

__version__ = "0.1.0"
