# Milestone 3: Data Processing Service (Dataflow)

ENGR 5520G / SOFE4630U. Student 101006689.

Apache Beam on Google Cloud Dataflow: four worked examples (word count, word
count 2, MNIST batch, MNIST stream) plus a design job that preprocesses smart
meter readings over Pub/Sub.

## Layout

```
wordcount/   wordcount2.py (branching MapReduce); wordcount.py is copied from
             the apache-beam library at run time, so it is not committed
mnist/       mnistBQ.py (batch, BigQuery), mnistPubSub.py (stream, Pub/Sub),
             setup.py (installs TensorFlow on workers), data/, model/
design/      smartMeterDataflow.py (the design job), producer.py, consumer.py,
             Labels.csv (smart meter readings, some with missing fields)
scripts/     setup-ms3.sh (provision + run jobs), record-demo.sh (video shots)
build_report.py   renders REPORT.md to a printable report
```

## Design job

`design/smartMeterDataflow.py` is a streaming pipeline:

```
Read from PubSub -> Filter (drop records with any missing field)
  -> Convert (pressure kPa/6.895 = psi, temperature C*1.8+32 = F)
  -> Write to PubSub
```

Reads the `meterInput` topic, writes cleaned readings to `meterOutput`.
`design/producer.py` publishes `design/Labels.csv` to `meterInput`;
`design/consumer.py` prints the converted results from `meterOutput`.

Quick check of the transform logic (no GCP needed):

```
python design/smartMeterDataflow.py --selftest
```

## Running

See `scripts/setup-ms3.sh` for every step (APIs, service account, bucket,
BigQuery, Pub/Sub topics, each job, teardown). It is scoped to the `sdt-a2`
gcloud config.

## Notes

The service account key is never committed (`.gitignore`). Copy your Pub/Sub
key JSON into the folder before running the local producer and consumer.
