from pathlib import Path

import pandas as pd

index_file = Path(r"D:\Desktop\rust-folder-analysis\results\index.parquet")

df = pd.read_parquet(index_file)

df = df[df["extension"] == "pt"]

df = df.sort_values(by="size", ascending=False)

df.to_csv("results/big_pt_files.csv")