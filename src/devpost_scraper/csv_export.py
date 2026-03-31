from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Iterable

from devpost_scraper.models import DevpostProject


def write_projects(projects: Iterable[DevpostProject], output: str | None) -> None:
    """Write projects to CSV. Prints to stdout if output is None."""
    fieldnames = DevpostProject.fieldnames()

    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = path.open("w", newline="", encoding="utf-8")
        close = True
    else:
        fh = sys.stdout
        close = False

    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for project in projects:
        writer.writerow(project.model_dump())

    if close:
        fh.close()
