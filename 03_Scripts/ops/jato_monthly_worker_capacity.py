#!/usr/bin/env python3
"""Print a non-secret capacity report for sizing the monthly worker cgroup."""

from __future__ import annotations

import json
import os
import shutil


def main() -> None:
    page_size = os.sysconf("SC_PAGE_SIZE")
    page_count = os.sysconf("SC_PHYS_PAGES")
    memory_bytes = page_size * page_count
    disk = shutil.disk_usage("/")
    report = {
        "memoryBytes": memory_bytes,
        "diskFreeBytes": disk.free,
        "diskTotalBytes": disk.total,
        "recommendation": {
            "workerMemoryHighBytes": int(memory_bytes * 0.55),
            "workerMemoryMaxBytes": int(memory_bytes * 0.70),
            "workerCpuWeight": 20,
            "workerIoWeight": 20,
            "note": "Review these values with production workload measurements before enabling a MemoryMax drop-in.",
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
