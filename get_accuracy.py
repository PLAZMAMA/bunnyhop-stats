from datetime import datetime
import argparse
import glob
import os

import polars as pl

def get_datetime(datetime_str: str) -> datetime:
    date, time = datetime_str.split(" ")
    datetime_args = tuple(int(datetime_num) for datetime_num in date.split("-") + time.split(":"))
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

parser = argparse.ArgumentParser(description="Get average accuracy of the predictions")
parser.add_argument("-s", "--start-date", type=str, help="Start date in UTC. Format: year-month-day hour:minute:second. Exmaple: python3 get_accuracy.py -s '2023-03-16 00:00:00'")
parser.add_argument("-e", "--end-date", type=str, help="End date in UTC. Format: year-month-day hour:minute:second. Exmaple: python3 get_accuracy.py -e '2023-03-16 00:00:00'")
args = parser.parse_args()

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

df = pl.concat(dfs)
df = df.with_columns(pl.from_epoch("time"))

start_datetime = df.select(pl.col("time")).min().row(0)[0]
end_datetime = df.select(pl.col("time")).max().row(0)[0]
if args.start_date is not None:
    start_date, start_time = args.start_date.split(" ")
    datetime_args = tuple(int(datetime_num) for datetime_num in start_date.split("-") + start_time.split(":"))
    start_datetime = datetime(*datetime_args, tzinfo=None)

if args.end_date is not None:
    end_date, end_time = args.end_date.split(" ")
    datetime_args = tuple(int(datetime_num) for datetime_num in end_date.split("-") + end_time.split(":"))
    end_datetime = datetime(*datetime_args, tzinfo=None)

df = df.filter(pl.col("time").is_between(start_datetime, end_datetime)).sort("time")
print(df.select(pl.col("line", "prediction_line")))
print(df)
