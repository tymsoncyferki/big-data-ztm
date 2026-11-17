"""Stage 3 of the 15-minute pipeline: refresh the ML dataset for the most recent days.

Called from spark/batch/run_pipeline.py after cleaning and GTFS enrichment, inside the
SAME Spark application and the SAME batch lock (so it must not take the lock itself).

Cost control: only the last `ML_STAGE_DAYS` UTC days are rebuilt (older partitions are
left untouched), and not more often than every `ML_STAGE_MIN_INTERVAL_MIN` minutes.
A full or historical rebuild is done by hand: bash scripts/build-ml-dataset.sh
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from build_ml_dataset import BuildConfig, build
from ml_common import HDFS, _fs, configure_session, write_text

LOG = logging.getLogger("ztm-ml")
MARKER = "/data/enriched/_ml_reports/last_stage_run.json"


def _minutes_since_last_run(spark, base: str) -> float | None:
    _, fs, path = _fs(spark, f"{base}{MARKER}")
    if not fs.exists(path):
        return None
    return (time.time() * 1000 - fs.getFileStatus(path).getModificationTime()) / 60000.0


def run_ml_stage(spark, base: str = HDFS, today: str | None = None) -> str:
    """Returns 'built', 'skipped' or 'no-data'. Raises on genuine errors."""
    base = base.rstrip("/")
    days = max(1, int(os.getenv("ML_STAGE_DAYS", "2")))
    min_interval = float(os.getenv("ML_STAGE_MIN_INTERVAL_MIN", "60"))

    age = _minutes_since_last_run(spark, base)
    if age is not None and age < min_interval:
        LOG.info("Stage 3: ML dataset refreshed %.0f min ago (< %.0f); skipping", age, min_interval)
        return "skipped"

    end = datetime.strptime(today, "%Y-%m-%d").date() if today else datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)
    cfg = BuildConfig(base=base, start_date=start.isoformat(), end_date=end.isoformat())

    LOG.info("Stage 3: building ML dataset for %s..%s", cfg.start_date, cfg.end_date)
    # A separate session: same cluster/application, but its own SQL settings.
    ml = configure_session(spark.newSession())
    try:
        stats = build(ml, cfg)
    except SystemExit as exc:      # build() uses SystemExit for "nothing to do yet"
        LOG.warning("Stage 3: ML dataset not built: %s", exc)
        return "no-data"
    write_text(ml, f"{base}{MARKER}", json.dumps({"finished": time.time(), "dates": [cfg.start_date, cfg.end_date],
                                                  "rows": stats.get("ml_dataset", {}).get("rows")}))
    LOG.info("Stage 3: ML dataset done in %ss", stats.get("seconds"))
    return "built"
