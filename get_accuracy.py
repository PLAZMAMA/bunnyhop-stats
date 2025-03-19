import glob
import os

import polars as pl

DATA_DIR_PATH = "/home/plazma/.config/local/share/nvim/bunnyhop/edit_predictions"
EDIT_ENTRY_SCHEMA = pl.Schema(
    {
        "seq": pl.Int32,
        "time": pl.Int32,
        "diff": pl.String,
        "file": pl.String,
        "line": pl.Int32,
        "prediction_line": pl.Int32,
        "prediction_file": pl.String,
        "model": pl.String,
    }
)

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
df = df.sort("time")
print(df.head())
