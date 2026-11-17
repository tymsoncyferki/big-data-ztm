"""Diagnose the quality of the estimated delays (read-only).

Run after build_ml_dataset.py when the numbers look odd, e.g. when the
"no-change" baseline is worse than "predict 0":

    bash scripts/diagnose-delays.sh [--start-date ...] [--end-date ...]

It answers:
  A. How are delays / visit-to-visit changes / distance-to-stop distributed?
  B. Is the noise concentrated at the first or last stop of a trip (terminals,
     where buses wait and the 'nearest ping' is ambiguous)?
  C. Does it differ for trams vs buses?
  D. How good is "no change" vs "predict 0" depending on how old the last delay is?
"""

from __future__ import annotations

import argparse
import json
import logging

from pyspark.sql import Window, functions as F

from delay_estimation import TRIP_KEY
from ml_common import HDFS, BatchLock, build_spark, write_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("ztm-ml")

QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95, 0.99]


def quantiles(frame, column):
    row = frame.agg(F.percentile_approx(column, QUANTILES).alias("q")).first()
    return dict(zip([f"p{int(q * 100)}" for q in QUANTILES], [None if v is None else float(v) for v in row["q"]]))


def group_summary(frame, by):
    rows = (
        frame.groupBy(by)
        .agg(
            F.count("*").alias("visits"),
            F.percentile_approx("delay_s", 0.5).alias("median_delay_s"),
            F.percentile_approx(F.abs("delay_s"), 0.95).alias("p95_abs_delay_s"),
            F.percentile_approx(F.abs("delay_change_s"), 0.5).alias("median_abs_change_s"),
            F.percentile_approx(F.abs("delay_change_s"), 0.95).alias("p95_abs_change_s"),
        )
        .orderBy(by)
        .collect()
    )
    return [r.asDict() for r in rows]


def baselines(frame):
    d = frame.withColumn("err_nochange", F.abs(F.col("target_delay_s") - F.col("last_delay_s"))).withColumn(
        "err_zero", F.abs(F.col("target_delay_s"))
    )
    row = d.agg(
        F.count("*").alias("rows"),
        F.avg("err_nochange").alias("mae_no_change_s"),
        F.avg("err_zero").alias("mae_zero_s"),
        F.expr("percentile_approx(err_nochange, 0.5)").alias("median_ae_no_change_s"),
        F.expr("percentile_approx(err_zero, 0.5)").alias("median_ae_zero_s"),
        F.avg(((F.col("target_delay_s") * F.col("last_delay_s")) < 0).cast("double")).alias("share_sign_flip"),
    ).first()
    return None if row["rows"] == 0 else {k: (None if v is None else float(v)) for k, v in row.asDict().items()}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default=HDFS)
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--no-lock", action="store_true")
    a = p.parse_args()
    base = a.base.rstrip("/")

    def in_range(frame):
        if a.start_date:
            frame = frame.filter(F.col("date") >= a.start_date)
        if a.end_date:
            frame = frame.filter(F.col("date") <= a.end_date)
        return frame

    with BatchLock(enabled=not a.no_lock):
        spark = build_spark("big-data-ztm-diagnose", base)
        try:
            visits = in_range(spark.read.parquet(f"{base}/data/enriched/stop_visits"))
            ml = in_range(spark.read.parquet(f"{base}/data/enriched/ml_dataset"))
            out: dict = {}

            # ---- A. distributions ------------------------------------------------
            out["visit_count"] = visits.count()
            out["delay_s"] = quantiles(visits, "delay_s")
            out["abs_delay_change_s"] = quantiles(visits.filter("delay_change_s is not null")
                                                  .withColumn("a", F.abs("delay_change_s")), "a")
            out["distance_m"] = quantiles(visits, "distance_m")
            n = max(out["visit_count"], 1)
            out["share_visits_early_over_2min"] = visits.filter("delay_s < -120").count() / n
            out["share_visits_late_over_10min"] = visits.filter("delay_s > 600").count() / n

            # ---- B. first / middle / last stop of the trip -----------------------
            w = Window.partitionBy(*TRIP_KEY)
            pos = visits.withColumn("_min", F.min("stop_sequence").over(w)).withColumn(
                "_max", F.max("stop_sequence").over(w)
            ).withColumn(
                "stop_position",
                F.when(F.col("stop_sequence") == F.col("_min"), "1_first_visited")
                 .when(F.col("stop_sequence") == F.col("_max"), "3_last_visited")
                 .otherwise("2_middle"),
            )
            out["by_stop_position"] = group_summary(pos, "stop_position")

            # Visits whose time order disagrees with stop_sequence order (the bus "reached"
            # a later stop before an earlier one): GPS noise, trip_id switched too early,
            # or opposite-direction stops within the radius.
            by_time = Window.partitionBy(*TRIP_KEY).orderBy("visit_epoch" if "visit_epoch" in visits.columns
                                                            else "visit_time_utc")
            prev_seq = visits.withColumn("_prev", F.lag("stop_sequence").over(by_time))
            out["share_visits_out_of_order"] = (
                prev_seq.filter(F.col("_prev") > F.col("stop_sequence")).count() / n
            )

            # ---- C. vehicle type (GTFS route_type: 0 tram, 3 bus, 2 rail) ---------
            out["by_route_type"] = group_summary(visits, "route_type")

            # ---- D. baselines on the ML dataset -----------------------------------
            usable = ml.filter("has_target and last_delay_s is not null")
            out["baselines_all"] = baselines(usable)
            buckets = (
                F.when(F.col("last_delay_age_s") < 90, "a_age_<90s")
                 .when(F.col("last_delay_age_s") < 180, "b_age_90-180s")
                 .when(F.col("last_delay_age_s") < 300, "c_age_180-300s")
                 .otherwise("d_age_>=300s")
            )
            out["baselines_by_last_delay_age"] = {
                r["b"]: baselines(usable.filter(buckets == r["b"]))
                for r in usable.select(buckets.alias("b")).distinct().collect()
            }
            out["baselines_by_route_type"] = {
                str(r["route_type"]): baselines(usable.filter(F.col("route_type") == r["route_type"]))
                for r in usable.select("route_type").distinct().collect()
            }
            out["target_minus_last_delay_s"] = quantiles(
                usable.withColumn("diff", F.col("target_delay_s") - F.col("last_delay_s")), "diff"
            )
            text = json.dumps(out, indent=2, default=str)
            write_text(spark, f"{base}/data/enriched/_ml_reports/diagnose_latest.json", text)
        finally:
            spark.stop()
    print(text)


if __name__ == "__main__":
    main()
