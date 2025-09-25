from glob import glob
import pandas as pd

def extract(options: dict, source_name: str):
    pattern = options["path"]
    for path in glob(pattern):
        df = pd.read_csv(filepath_or_buffer=path)
        for rec in df.to_dict(orient="records"):
            yield rec
