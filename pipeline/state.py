import sqlite3, json, os, time
DB = os.environ.get("PIPELINE_STATE_DB", "./pipeline_state.db")

def _conn(): #create a connection to the sqlite db
    conn = sqlite3.connect(DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS state(
        source TEXT PRIMARY KEY, checkpoint TEXT, meta TEXT, updated_at INTEGER
    )""")
    return conn

def get_checkpoint(source: str): #get the checkpoint for a given source
    with _conn() as c:
        row = c.execute(
            "SELECT checkpoint FROM state WHERE source=?",
            (source,),
        ).fetchone()#fetchone returns a single row or None
        return row[0] if row else None

def set_checkpoint(source: str, checkpoint: str, meta: dict|None=None): #set the checkpoint for a given source
    with _conn() as c:
        c.execute(
            "REPLACE INTO state(source, checkpoint, meta, updated_at) VALUES(?,?,?,?)",
            (source, checkpoint, json.dumps(meta or {}), int(time.time())))
