"""Print a few rows of an enriched table in readable form (read-only).
    bash scripts/show-sample.sh ml_dataset 5
    bash scripts/show-sample.sh ml_dataset 5 --route 175
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from ml_common import HDFS, build_spark

p = argparse.ArgumentParser()
p.add_argument("table", choices=["stop_visits", "ml_dataset"])
p.add_argument("n", nargs="?", type=int, default=10)
p.add_argument("--route", help="route_short_name, e.g. 175 or 17")
p.add_argument("--base", default=HDFS)
a = p.parse_args()

spark = build_spark("show-sample", a.base)
df = spark.read.parquet(f"{a.base.rstrip('/')}/data/enriched/{a.table}")
if a.route:
    df = df.filter(F.col("route_short_name") == a.route)
print(f"\n{a.table}: {df.count()} rows, {len(df.columns)} columns\n")
df.printSchema()
order = "visit_time_utc" if a.table == "stop_visits" else "event_time_utc"
df.orderBy(F.rand(7)).limit(a.n).orderBy("trip_id", order).show(a.n, truncate=False, vertical=True)
spark.stop()
