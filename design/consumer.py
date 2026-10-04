# Reads the cleaned and converted smart meter readings produced by the
# preprocessing Dataflow job from the output topic (default meterOutput) and
# prints them. Run on your local machine with the Pub/Sub service account key
# (*.json) in the current folder.
#
#   pip install google-cloud-pubsub
#   python design/consumer.py          # Ctrl+C to stop

from google.cloud import pubsub_v1
import glob
import json
import os
import threading

# Use a service account key if one is in the folder; otherwise fall back to
# Application Default Credentials (gcloud auth application-default login).
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
else:
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

# Set these for your project.
project_id = "project-177cbd41-037f-441e-80d"
subscription_id = "meterOutput-sub"   # subscription on the job's output topic

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
print(f"Listening for messages on {subscription_path}..\n")

count = 0
# Pub/Sub guarantees at-least-once delivery, so the same record can arrive more
# than once. message_id is stable across redeliveries, used to flag duplicates.
seen = set()
# Streaming pull runs the callback on several threads, so shared state and the
# printing are guarded by a lock to keep output lines from interleaving.
lock = threading.Lock()


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global count
    record = json.loads(message.data.decode("utf-8"))
    with lock:
        count += 1
        duplicate = message.message_id in seen
        seen.add(message.message_id)
        print("Record {}: {}{}".format(
            len(seen), record, "   [redelivery]" if duplicate else ""))
    message.ack()


with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nStopped after {} deliveries, {} unique records.".format(count, len(seen)))
