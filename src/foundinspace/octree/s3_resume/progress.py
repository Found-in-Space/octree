from __future__ import annotations

import time


class Progress:
    def __init__(self, *, total_bytes: int, enabled: bool = True) -> None:
        self.total_bytes = total_bytes
        self.enabled = enabled
        self.start = time.monotonic()
        self.done = 0

    def add(self, size: int) -> None:
        self.done += size
        if not self.enabled:
            return
        elapsed = max(time.monotonic() - self.start, 0.001)
        speed = self.done / elapsed
        remaining = max(self.total_bytes - self.done, 0)
        eta = remaining / speed if speed > 0 else 0.0
        pct = (self.done / self.total_bytes) * 100.0 if self.total_bytes else 100.0
        print(
            f"\rUploaded {self.done}/{self.total_bytes} bytes "
            f"({pct:.1f}%), {speed / 1024 / 1024:.2f} MiB/s, ETA {eta:.1f}s",
            end="",
            flush=True,
        )

    def finish(self) -> None:
        if self.enabled:
            print()
