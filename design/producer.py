# Publishes the smart meter readings in design/Labels.csv to the input topic
# of the preprocessing Dataflow job (default meterInput). Run on your local
# machine with the Pub/Sub service account key (*.json) in the current folder.
#
#   pip install google-cloud-pubsub
#   python design/producer.py

from google.cloud import pubsub_v1
import glob
import json
import os
import csv

# Find the service account key JSON in the current directory.
# Use a service account key if one is in the folder; otherwise fall back to
# Application Default Credentials (gcloud auth application-default login).
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
else:
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

# Set these for your project.
project_id = "project-177cbd41-037f-441e-80d"
topic_name = "meterInput"          # input topic the Dataflow job reads from
csv_file = "design/Labels.csv"

# Ordering is enabled so all readings of one device arrive in publish order.
publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing the records of {csv_file} to {topic_path}.")

# Numeric columns of the CSV. A blank cell means the meter did not report that
# measurement, so it becomes None (JSON null), which the Dataflow job filters.
numeric_fields = ["time", "temperature", "humidity", "pressure"]


def to_record(row):
    record = dict(row)
    for field in numeric_fields:
        record[field] = float(record[field]) if record[field] else None
    return record


published = 0
with open(csv_file, newline="") as f:
    for row in csv.DictReader(f):
        record = to_record(row)
        record_value = json.dumps(record).encode("utf-8")
        try:
            future = publisher.publish(topic_path, record_value,
                                       ordering_key=record["profileName"])
            future.result()
            published += 1
            print("The record {} has been published successfully".format(record))
        except Exception as error:
            print("Failed to publish the record: {}".format(error))

print("{} records published to {}".format(published, topic_name))
