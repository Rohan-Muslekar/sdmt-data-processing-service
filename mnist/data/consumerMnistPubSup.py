import os
from google.cloud import pubsub_v1      #pip install google-cloud-pubsub
import glob

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
else:
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)  # use gcloud ADC

# TODO : fill project id 
project_id = "project-177cbd41-037f-441e-80d"
subscription_id = "mnist_predict-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

import json
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    #print(f"Received {json.loads(message)}.")
    print(f"Received {json.loads(message.data)}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    streaming_pull_future.result()