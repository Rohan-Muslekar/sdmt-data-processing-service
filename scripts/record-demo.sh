#!/usr/bin/env bash
# record-demo.sh - shot lists for the two Milestone 3 videos. Prints the steps
# to narrate; it records nothing itself. Use your screen recorder (OBS, Loom)
# and read each beat aloud.
#
#   ./scripts/record-demo.sh examples   # video 1 (~4 min): four examples
#   ./scripts/record-demo.sh design     # video 2 (~5 min): design part

set -euo pipefail

case "${1:-}" in
examples)
  cat <<'EOF'
VIDEO 1  (~4 minutes)  Four Dataflow examples, with audio
------------------------------------------------------------
0:00  Intro. Name, student ID, "Milestone 3, Dataflow examples".
0:15  Example 1 wordcount: show wordcount.py pipeline stages in the editor,
      then the finished Dataflow job graph (Read/Split/PairWithOne/GroupAndSum)
      and JOB_STATE_DONE. Open outputs file in the bucket, show word counts.
1:15  Example 2 wordcount2: show the fork after 'words' (two branches),
      the job graph with both branches, then outputs (a-f words) and
      outputs2 (first-letter frequencies) in the bucket.
2:15  Example 3 mnistBQ (batch): show MNIST.Images table, the 3-stage job
      (ReadFromBQ/Prediction/WriteToBQ), then MNIST.Predict with ID,P0..P9.
3:00  Example 4 mnistPubSub (stream): show the 5-stage streaming job running,
      producerMnistPubSup.py publishing, consumerMnistPubSup.py printing
      predictions live. Stop the streaming job at the end.
3:50  Wrap up. Mention results match expected digits.
EOF
  ;;
design)
  cat <<'EOF'
VIDEO 2  (~5 minutes)  Design part, with audio
------------------------------------------------------------
0:00  Intro. "Milestone 3 design: smart meter preprocessing Dataflow job."
0:20  Problem: readings from Milestone 2 carry raw kPa/Celsius and some have
      missing fields. Goal: filter incomplete rows, convert units, republish.
0:50  Walk design/smartMeterDataflow.py: Read from PubSub -> Filter (drop None)
      -> Convert (pressure kPa/6.895=psi, temp C*1.8+32=F) -> Write to PubSub.
1:40  Show topics meterInput / meterOutput and the subscription meterOutput-sub.
2:00  Launch the job (scripts/setup-ms3.sh design); show the 4-stage job graph
      and that it is Running (streaming).
2:50  Run design/producer.py: publishes design/Labels.csv to meterInput.
3:20  Run design/consumer.py: show cleaned records on meterOutput, pressure in
      psi and temperature in F, and that rows with missing values never arrive.
4:20  Point out one dropped record (a blank cell in Labels.csv) to prove filter.
4:40  Stop the streaming job. Wrap up.
EOF
  ;;
*)
  echo "usage: $0 {examples|design}"
  ;;
esac
