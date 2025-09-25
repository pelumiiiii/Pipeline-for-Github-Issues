def clean_record(rec: dict) -> dict:
    # Skeptical defaults: strip strings, normalize whitespace, coerce nulls
    out = {}
    for k,v in rec.items():
        if isinstance(v, str):
            v = v.strip()
            if v == "": v = None
        out[k] = v
    return out
