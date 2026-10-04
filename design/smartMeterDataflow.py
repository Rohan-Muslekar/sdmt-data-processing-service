# Smart meter preprocessing Dataflow job (Milestone 3 design part).
#
# Pipeline: Pub/Sub input topic -> drop records with missing measurements ->
# convert pressure (kPa to psi) and temperature (Celsius to Fahrenheit) ->
# Pub/Sub output topic. It is a streaming job and runs until stopped manually.
#
# Record shape (from design/Labels.csv, published by design/producer.py):
#   {"time": float, "profileName": str, "temperature": float|None,
#    "humidity": float|None, "pressure": float|None}
# A blank cell in the CSV is published as JSON null, i.e. None in Python.

import argparse
import json
import logging

# pressure: 1 psi = 6.895 kPa ; temperature: F = C * 1.8 + 32
KPA_PER_PSI = 6.895


def is_complete(record):
    """Keep only readings where every field was reported (no None)."""
    return all(value is not None for value in record.values())


def convert(record):
    """Convert pressure kPa->psi and temperature Celsius->Fahrenheit.

    Returns a new dict; the input is not mutated. Other fields pass through.
    """
    out = dict(record)
    out["pressure"] = record["pressure"] / KPA_PER_PSI
    out["temperature"] = record["temperature"] * 1.8 + 32
    return out


def run(argv=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input", dest="input", required=True,
                        help="Input Pub/Sub topic (projects/<id>/topics/<name>).")
    parser.add_argument("--output", dest="output", required=True,
                        help="Output Pub/Sub topic (projects/<id>/topics/<name>).")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # import beam here so --selftest can run without apache-beam installed
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.options.pipeline_options import SetupOptions

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input)
         | "toDict" >> beam.Map(lambda b: json.loads(b.decode("utf-8")))
         | "Filter" >> beam.Filter(is_complete)
         | "Convert" >> beam.Map(convert)
         | "to byte" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
         | "Write to PubSub" >> beam.io.WriteToPubSub(topic=known_args.output))


def _selftest():
    full = {"time": 1.0, "profileName": "denver",
            "temperature": 100.0, "humidity": 40.0, "pressure": 68.95}
    missing = {"time": 2.0, "profileName": "denver",
               "temperature": None, "humidity": 40.0, "pressure": 68.95}

    assert is_complete(full)
    assert not is_complete(missing)

    out = convert(full)
    assert abs(out["pressure"] - 10.0) < 1e-9, out["pressure"]   # 68.95/6.895
    assert abs(out["temperature"] - 212.0) < 1e-9, out["temperature"]  # 100C=212F
    assert out["humidity"] == 40.0 and out["profileName"] == "denver"
    assert full["pressure"] == 68.95, "input must not be mutated"
    print("selftest ok")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        _selftest()
    else:
        logging.getLogger().setLevel(logging.INFO)
        run()
