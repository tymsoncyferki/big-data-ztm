# Future Spark Streaming — online layer

**Not implemented in this version.** The batch pipeline reads HDFS only.

A separate online application may later consume the `gps`, `weather` and
`air_quality` Kafka topics with Spark Structured Streaming, maintain its own
checkpoints, and publish low-latency metrics or predictions to an online store.
It must not write to the batch pipeline's `/data/cleaned/` folders and must not
reuse batch job state. The online sink, late-data policy and refresh latency
should be defined by the developer implementing that layer.
