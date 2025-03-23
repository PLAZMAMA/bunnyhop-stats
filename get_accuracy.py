import argparse
import glob
import os
from datetime import datetime

import polars as pl


def get_datetime(datetime_str: str) -> datetime:
    date, time = datetime_str.split(" ")
    datetime_args = tuple(
        int(datetime_num) for datetime_num in date.split("-") + time.split(":")
    )
    return datetime(*datetime_args, tzinfo=None)


DATA_DIR_PATH = "/home/plazma/.config/local/share/nvim/bunnyhop/edit_predictions"
EDIT_ENTRY_SCHEMA = {
    "seq": pl.Int32,
    "time": pl.Int32,
    "diff": pl.String,
    "file": pl.String,
    "line": pl.Int32,
    "prediction_line": pl.Int32,
    "prediction_file": pl.String,
    "model": pl.String,
}

# Reading CLI args
parser = argparse.ArgumentParser(description="Get average accuracy of the predictions")
parser.add_argument(
    "-s",
    "--start-datetime",
    type=str,
    help="Start date in UTC. Format: year-month-day hour:minute:second. Exmaple: python3 get_accuracy.py -s '2023-03-16 00:00:00'",
)
parser.add_argument(
    "-e",
    "--end-datetime",
    type=str,
    help="End date in UTC. Format: year-month-day hour:minute:second. Exmaple: python3 get_accuracy.py -e '2023-03-16 00:00:00'",
)
args = parser.parse_args()

# Getting all stored edit histories and combining into one df
dfs: list[pl.DataFrame] = []
for file_path in glob.glob(DATA_DIR_PATH + "/*"):
    if os.path.getsize(file_path) == 0:
        continue

    dfs.append(
        pl.read_ndjson(
            file_path,
            schema=EDIT_ENTRY_SCHEMA,
        )
    )
df = pl.concat(dfs).with_columns(pl.from_epoch("time"))

# Extracting start/end datetimes if given
start_datetime = df.select(pl.col("time")).min().row(0)[0]
end_datetime = df.select(pl.col("time")).max().row(0)[0]
if args.start_datetime is not None:
    start_datetime = get_datetime(args.start_datetime)
if args.end_datetime is not None:
    end_datetime = get_datetime(args.end_datetime)

# Calculating accuracy of predictions
df = (
    df.filter(pl.col("time").is_between(start_datetime, end_datetime))
    .sort("time")
    .with_columns(
        [pl.col("prediction_line").shift(1), pl.col("prediction_file").shift(1)]
    )
    .slice(1)
    .with_columns(
        [
            (pl.col("line") == pl.col("prediction_line")).alias(
                "correct_line_prediction"
            ),
            (pl.col("file") == pl.col("prediction_file")).alias(
                "correct_file_prediction"
            ),
        ]
    )
)
print(
    df.select(
        [
            (
                pl.col("correct_line_prediction").cast(pl.Int8).sum()
                / pl.col("correct_line_prediction").count() * 100
            ).alias("line_prediction_accuracy (%)"),
            (
                pl.col("correct_file_prediction").cast(pl.Int8).sum()
                / pl.col("correct_line_prediction").count() * 100
            ).alias("file_prediction_accuracy"),
        ]
    )
)
