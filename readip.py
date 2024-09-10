#!/usr/bin/env python3

import argparse

from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet
import pandas as pd
from prophet import Prophet


def main(
    project_id="project-id",
    instance_id="instance-id",
    table_id="my-table",
    family_id="my-family",
):
    dates = []
    points = []
    # Create a Cloud Bigtable client.
    client = bigtable.Client(project=project_id)

    # Connect to an existing Cloud Bigtable instance.
    instance = client.instance(instance_id)

    # Open an existing table.
    table = instance.table(table_id)

    row_set = RowSet()
    row_set.add_row_range_from_keys(
        start_key=b"10.0.0.99#0", end_key=b"10.0.0.99#99999999999999"
    )

    rows = table.read_rows(row_set=row_set)
    for row in rows:
        if row is not None:
            dates.append(row.row_key.decode("utf-8").split("#")[1])
            points.append(
                int.from_bytes(
                    row.cells[family_id]["ips".encode("utf-8")][0].value, "big"
                )
            )

    ts_data = pd.DataFrame({"date": pd.to_datetime(dates, unit="s"), "points": points})

    ts_hourly = ts_data.resample('H', on=date).sum().reset_index()
    model_hourly = Prophet()
    model_hourly.fit(ts_hourly)
    future_hourly = model_hourly.make_future_dataframe(periods=24, freq="H")
    forecast_hourly = model_hourly.predict(future_hourly)
    print(forecast_hourly)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--project_id", "-p", help="GCP project ID.", required=True)
    parser.add_argument(
        "--instance_id",
        help="Cloud Bigtable instance to connect to.",
        default="timeseries",
    )
    parser.add_argument(
        "--table", help="Cloud Bigtable table to read from.", default="metrics"
    )
    parser.add_argument(
        "--family", help="Cloud Bigtable table family to read from.", default="stats"
    )

    args = parser.parse_args()
    main(args.project_id, args.instance_id, args.table, args.family)
