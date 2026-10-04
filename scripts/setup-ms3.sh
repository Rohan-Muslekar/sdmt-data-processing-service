#!/usr/bin/env bash
# setup-ms3.sh - provision and run the Milestone 3 Dataflow jobs.
#
# Reference runbook. Run subcommands one at a time from the GCP Cloud Shell (or
# a machine with gcloud + apache-beam[gcp]). Every gcloud call is scoped to the
# sdt-a2 config so it never touches your default account.
#
#   ./scripts/setup-ms3.sh apis         # enable the APIs
#   ./scripts/setup-ms3.sh sa           # service account + roles
#   ./scripts/setup-ms3.sh bucket       # cloud storage bucket
#   ./scripts/setup-ms3.sh bq           # BigQuery MNIST dataset + Images table
#   ./scripts/setup-ms3.sh model        # upload the ML model to the bucket
#   ./scripts/setup-ms3.sh topics       # pub/sub topics + subscriptions
#   ./scripts/setup-ms3.sh wordcount    # example 1
#   ./scripts/setup-ms3.sh wordcount2   # example 2
#   ./scripts/setup-ms3.sh mnistbq      # example 3 (batch)
#   ./scripts/setup-ms3.sh mniststream  # example 4 (stream)
#   ./scripts/setup-ms3.sh design       # design preprocessing job (stream)
#   ./scripts/setup-ms3.sh teardown     # remove topics, bucket, dataset
#
set -euo pipefail

export CLOUDSDK_ACTIVE_CONFIG_NAME="${CLOUDSDK_ACTIVE_CONFIG_NAME:-sdt-ms3}"
# A stray GOOGLE_APPLICATION_CREDENTIALS (e.g. a work SA key in your shell) would
# override Application Default Credentials and point Beam at the wrong/missing
# key. Clear it so the jobs authenticate via `gcloud auth application-default`.
unset GOOGLE_APPLICATION_CREDENTIALS
PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
PROJECT="${PROJECT:-project-177cbd41-037f-441e-80d}"
REGION="${REGION:-northamerica-northeast2}"
BUCKET="gs://${PROJECT}-bucket"
SA="${PROJECT}-DFSA"
SA_EMAIL="${SA}@${PROJECT}.iam.gserviceaccount.com"
# worker/submit service account used for the Dataflow jobs (has all needed roles)
DF_SA="${DF_SA:-ms3-dataflow-sa@${PROJECT}.iam.gserviceaccount.com}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "project=$PROJECT region=$REGION bucket=$BUCKET config=$CLOUDSDK_ACTIVE_CONFIG_NAME"

common_flags() {
  # flags shared by every Dataflow run
  echo "--region $REGION --runner DataflowRunner --project $PROJECT \
--temp_location $BUCKET/tmp/ --service_account_email $DF_SA \
--experiment use_unsupported_python_version"
}

case "${1:-}" in
apis)
  gcloud services enable dataflow.googleapis.com bigquery.googleapis.com \
    pubsub.googleapis.com storage.googleapis.com compute.googleapis.com \
    --project "$PROJECT"
  ;;
sa)
  gcloud iam service-accounts create "$SA" --project "$PROJECT" \
    --display-name "Dataflow MS3 service account" || true
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member "serviceAccount:$SA_EMAIL" --role roles/compute.serviceAgent
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member "serviceAccount:$SA_EMAIL" --role roles/pubsub.admin
  echo "service account: $SA_EMAIL"
  ;;
bucket)
  gcloud storage buckets create "$BUCKET" --project "$PROJECT" \
    --location "$REGION" --public-access-prevention
  ;;
bq)
  bq --project_id "$PROJECT" mk --dataset "${PROJECT}:MNIST" || true
  bq --project_id "$PROJECT" load --autodetect --source_format=CSV \
    "${PROJECT}:MNIST.Images" "$REPO_ROOT/mnist/data/mnist.csv"
  ;;
model)
  gcloud storage cp -r "$REPO_ROOT/mnist/model" "$BUCKET/model"
  ;;
topics)
  for t in mnist_image mnist_predict meterInput meterOutput; do
    gcloud pubsub topics create "$t" --project "$PROJECT" || true
  done
  # subscriptions the local consumers read from
  gcloud pubsub subscriptions create mnist_predict-sub \
    --topic mnist_predict --project "$PROJECT" || true
  gcloud pubsub subscriptions create meterOutput-sub \
    --topic meterOutput --project "$PROJECT" || true
  ;;
wordcount)
  # wordcount.py ships with apache-beam; copy it out of the library if missing.
  # Search the venv first (fast, no permission noise); 2>/dev/null silences the
  # "Permission denied" lines find prints while scanning unrelated home folders.
  if [ ! -f "$REPO_ROOT/wordcount/wordcount.py" ]; then
    WC="$(find "$REPO_ROOT/.venv" -name 'wordcount.py' -path '*apache_beam*' 2>/dev/null | head -1)"
    [ -z "$WC" ] && WC="$(find ~ -name 'wordcount.py' -path '*apache_beam*' 2>/dev/null | head -1)"
    echo "found $WC"
    cp "$WC" "$REPO_ROOT/wordcount/wordcount.py"
  fi
  cd "$REPO_ROOT/wordcount"
  python3 wordcount.py $(common_flags) \
    --input gs://dataflow-samples/shakespeare/winterstale.txt \
    --output "$BUCKET/result/outputs"
  ;;
wordcount2)
  cd "$REPO_ROOT/wordcount"
  python3 wordcount2.py $(common_flags) \
    --input gs://dataflow-samples/shakespeare/winterstale.txt \
    --output "$BUCKET/result/outputs" \
    --output2 "$BUCKET/result/outputs2"
  ;;
mnistbq)
  cd "$REPO_ROOT/mnist"
  python3 mnistBQ.py \
    --runner DataflowRunner --project "$PROJECT" \
    --staging_location "$BUCKET/staging" --temp_location "$BUCKET/temp" \
    --model "$BUCKET/model" --setup_file ./setup.py \
    --input "$PROJECT.MNIST.Images" --output "$PROJECT.MNIST.Predict" \
    --service_account_email "$DF_SA" \
    --region "$REGION" --experiment use_unsupported_python_version
  ;;
mniststream)
  cd "$REPO_ROOT/mnist"
  python3 mnistPubSub.py \
    --runner DataflowRunner --project "$PROJECT" \
    --staging_location "$BUCKET/staging" --temp_location "$BUCKET/temp" \
    --model "$BUCKET/model" --setup_file ./setup.py \
    --input "projects/$PROJECT/topics/mnist_image" \
    --output "projects/$PROJECT/topics/mnist_predict" \
    --service_account_email "$DF_SA" \
    --region "$REGION" --experiment use_unsupported_python_version --streaming
  ;;
design)
  cd "$REPO_ROOT"
  python3 design/smartMeterDataflow.py $(common_flags) \
    --staging_location "$BUCKET/staging" \
    --input "projects/$PROJECT/topics/meterInput" \
    --output "projects/$PROJECT/topics/meterOutput" \
    --streaming
  ;;
teardown)
  for t in mnist_image mnist_predict meterInput meterOutput; do
    gcloud pubsub topics delete "$t" --project "$PROJECT" || true
  done
  for s in mnist_predict-sub meterOutput-sub; do
    gcloud pubsub subscriptions delete "$s" --project "$PROJECT" || true
  done
  bq --project_id "$PROJECT" rm -r -f --dataset "${PROJECT}:MNIST" || true
  gcloud storage rm -r "$BUCKET" || true
  echo "Stop any running (streaming) Dataflow jobs from the Dataflow console."
  ;;
*)
  grep '^#   ' "$0" | sed 's/^#   //'
  ;;
esac
