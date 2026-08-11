"""
缇庢床鍖鸿揣鏌滆繘搴﹁拷韪?v6 - Turso 浜?SQLite + Render 閮ㄧ讲
"""

import asyncio, sqlite3, os, json, httpx
from datetime import datetime
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

TURSO_URL  = os.getenv("TURSO_URL", "")
TURSO_TOKEN = os.getenv("TURSO_TOKEN", "")
DB_PATH = Path(__file__).parent / "dashboard.db"

NODES_DEF = [
    ("甯傚満鎻愰渶", 0, 0, 1),
    ("绯荤粺涓嬪崟", 1, 0, 1),
    ("鎺掓煖纭鏁伴噺", 2, 0, 1),
    ("渚涘簲鍟嗕笅鍗?, 3, 0, 1),
    ("鍟嗘", 4, 0, 1),
    ("鍒颁粨", 5, 0, 1),
    ("璁㈣埍", 6, 0, 1),
    ("瑁呮煖", 7, 0, 1),
    ("寮€鑸?, 8, 0, 1),
    ("鍒版腐", 9, 0, 1),
    ("娓呭叧", 10, 0, 1),
    ("鍏ュ簱", 11, 0, 1),
]

NODE_NAMES = [n[0] for n in NODES_DEF]

sse_listeners: list[asyncio.Queue] = []


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class TursoConnection:
    """Turso HTTP API 灏佽锛屾彁渚?sqlite3.Row 鍏煎鎺ュ彛"""

    def __init__(self, url: str, token: str):
        # libsql://host 鈫?https://host
        self._http_url = "https://" + url.replace("libsql://", "").replace("https://", "")
        self._token = token
        self.row_factory = None

    def _pipeline(self, requests: list) -> list:
        r = httpx.post(
            f"{self._http_url}/v2/pipeline",
            headers={"Authorization": f"Bearer {self._token}"},
            json={"requests": requests + [{"type": "close"}]},
            timeout=30,
        )
        if r.status_code != 200:
            raise Exception(f"Turso error {r.status_code}: {r.text[:500]}")
        return r.json().get("results", [])

    def _make_row(self, cols, row_values):
        row = _TursoRow()
        row._keys = [c["name"] for c in cols]
        vals = []
        for v in row_values:
            if not isinstance(v, dict):
                vals.append(v)
            elif v.get("type") == "null":
                vals.append(None)
            elif v.get("type") == "integer":
                vals.append(int(v.get("value", 0)))
            elif v.get("type") == "float":
                vals.append(float(v.get("value", 0)))
            else:
                vals.append(v.get("value", ""))
        row._values = vals
        return row

    def execute(self, sql: str, params=None):
        params = params or ()
        if not isinstance(params, (list, tuple)):
            params = (params,)
        args = []
        for p in params:
            if isinstance(p, bool) or isinstance(p, int):
                args.append({"type": "integer", "value": str(p)})
            elif isinstance(p, float):
                args.append({"type": "float", "value": str(p)})
            elif p is None:
                args.append({"type": "null"})
            else:
                args.append({"type": "text", "value": str(p)})

        body = [{"type": "execute", "stmt": {"sql": sql, "args": args}}]
        results = self._pipeline(body)

        if results:
            r = results[0]
            if r.get("type") == "error":
                raise Exception(r.get("error", {}).get("message", "unknown Turso error"))
            rows = r.get("response", {}).get("result", {}).get("rows", [])
            cols = r.get("response", {}).get("result", {}).get("cols", [])
            cursor = _TursoCursor()
            cursor._results = [self._make_row(cols, row) for row in rows]
            cursor._lastrowid = int(r.get("response", {}).get("result", {}).get("last_insert_rowid", 0) or 0)
            return cursor
        return _TursoCursor()

    def commit(self):
        pass  # Turso HTTP API is auto-commit

    def close(self):
        pass


class _TursoRow:
    """妯℃嫙 sqlite3.Row"""

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._keys.index(key)]

    def keys(self):
        return self._keys


class _TursoCursor:
    """妯℃嫙 sqlite3.Cursor"""

    def __init__(self):
        self._results = []
        self._lastrowid = 0

    def fetchall(self):
        return self._results

    def fetchone(self):
        return self._results[0] if self._results else None

    @property
    def lastrowid(self):
        return self._lastrowid


async def broadcast_change():
    for q in sse_listeners:
        await q.put("changed")


def _get_turso():
    return TursoConnection(TURSO_URL, TURSO_TOKEN)


@contextmanager
def get_db():
    if TURSO_URL and TURSO_TOKEN:
        db = _get_turso()
        try:
            yield db
        finally:
            db.close()
    else:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=3000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def _mk_time():
    return now_str()


def init_db():
    with get_db() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT NOT NULL, batch_name TEXT NOT NULL,
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT '')""")
        db.execute("""CREATE TABLE IF NOT EXISTS node_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_name TEXT UNIQUE NOT NULL, sort_order INTEGER NOT NULL,
            estimated_days REAL DEFAULT 0, is_timepoint INTEGER DEFAULT 0)""")
        db.execute("""CREATE TABLE IF NOT EXISTS batch_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL, node_name TEXT NOT NULL,
            start_time TEXT DEFAULT '', end_time TEXT DEFAULT '',
            opt_date TEXT DEFAULT '', is_overdue INTEGER DEFAULT 0,
            FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
            UNIQUE(batch_id, node_name))""")
        try:
            db.execute("ALTER TABLE batch_nodes ADD COLUMN opt_date TEXT DEFAULT ''")
        except:
            pass

        for nm, so, ed, tp in NODES_DEF:
            db.execute("INSERT OR IGNORE INTO node_config (node_name, sort_order, estimated_days, is_timepoint) VALUES (?,?,?,?)", (nm, so, ed, tp))

        if db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0:
            seed = [
                ("宸磋タ","202604",""),
                ("宸磋タ","202606",""),
                ("宸磋タ","202607-1",""),
                ("宸磋タ","202609",""),
                ("缇庝笢","202609 1-2-3",""),
                ("缇庝笢","202610",""),
                ("缇庝笢","202611锛?锛?,""),
                ("缇庝笢","202612",""),
                ("缇庝笢","202613",""),
                ("澧ㄨタ鍝?,"202609",""),
                ("缇庝笢","202614",""),
                ("缇庤タ","202614",""),
            ]
            for ctry, bat, nt in seed:
                db.execute("INSERT INTO batches (country,batch_name,notes,created_at,updated_at) VALUES (?,?,?,?,?)",
                           (ctry, bat, nt, _mk_time(), _mk_time()))
                bid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                for nm in NODE_NAMES:
                    db.execute("INSERT INTO batch_nodes (batch_id,node_name) VALUES (?,?)", (bid, nm))


def recalc_batch_overdue(db, batch_id: int):
    rows = db.execute("""
        SELECT bn.id, bn.node_name, bn.start_time, nc.estimated_days, nc.sort_order
        FROM batch_nodes bn JOIN node_config nc ON bn.node_name=nc.node_name
        WHERE bn.batch_id=? ORDER BY nc.sort_order
    """, (batch_id,)).fetchall()

    for i in range(1, len(rows)):
        prev = rows[i-1]
        curr = rows[i]
        p_time = prev["start_time"]
        c_time = curr["start_time"]
        est = curr["estimated_days"]
        overdue = 0
        if p_time and c_time and est > 0:
            try:
                d = (datetime.strptime(c_time[:10], "%Y-%m-%d") - datetime.strptime(p_time[:10], "%Y-%m-%d")).days
                if d > est:
                    overdue = 1
            except:
                pass
        db.execute("UPDATE batch_nodes SET is_overdue=? WHERE id=?", (overdue, curr["id"]))
    if rows:
        db.execute("UPDATE batch_nodes SET is_overdue=0 WHERE id=?", (rows[0]["id"],))


class BatchCreate(BaseModel):
    country: str; batch_name: str; notes: str = ""

class BatchNodeUpdate(BaseModel):
    batch_id: int; node_name: str; start_time: str = ""; end_time: str = ""

class NodeConfigUpdate(BaseModel):
    node_name: str; estimated_days: float

class BatchNoteUpdate(BaseModel):
    batch_id: int; notes: str

class OptDateUpdate(BaseModel):
    batch_id: int; node_name: str; opt_date: str = ""


app = FastAPI(title="缇庢床鍖鸿揣鏌滆繘搴﹁拷韪?v6")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)


@app.get("/api/events")
async def sse_events():
    q: asyncio.Queue = asyncio.Queue()
    sse_listeners.append(q)
    try:
        while True:
            await q.get()
            yield "data: changed\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        sse_listeners.remove(q)


@app.get("/api/batches")
def list_batches():
    with get_db() as db:
        batches = db.execute("SELECT * FROM batches ORDER BY country, batch_name").fetchall()
        result = []
        for b in batches:
            nodes = db.execute("""
                SELECT bn.*, nc.estimated_days, nc.is_timepoint FROM batch_nodes bn
                JOIN node_config nc ON bn.node_name=nc.node_name
                WHERE bn.batch_id=? ORDER BY nc.sort_order
            """, (b["id"],)).fetchall()
            result.append({
                "id": b["id"], "country": b["country"], "batch_name": b["batch_name"],
                "notes": b["notes"],
                "nodes": [{"node_name": n["node_name"], "start_time": n["start_time"],
                           "end_time": n["end_time"], "is_overdue": bool(n["is_overdue"]),
                           "estimated_days": n["estimated_days"], "is_timepoint": bool(n["is_timepoint"]),
                           "opt_date": n["opt_date"] or ""} for n in nodes],
                "created_at": b["created_at"], "updated_at": b["updated_at"],
            })
        return result


@app.post("/api/batches")
async def create_batch(data: BatchCreate):
    with get_db() as db:
        t = _mk_time()
        c = db.execute("INSERT INTO batches (country,batch_name,notes,created_at,updated_at) VALUES (?,?,?,?,?)",
                       (data.country, data.batch_name, data.notes, t, t))
        bid = c.lastrowid
        for nm in NODE_NAMES:
            db.execute("INSERT INTO batch_nodes (batch_id,node_name) VALUES (?,?)", (bid, nm))
    await broadcast_change()
    return {"id": bid, "message": "鎵规宸叉坊鍔?}


@app.delete("/api/batches/{batch_id}")
async def delete_batch(batch_id: int):
    with get_db() as db:
        db.execute("DELETE FROM batch_nodes WHERE batch_id=?", (batch_id,))
        db.execute("DELETE FROM batches WHERE id=?", (batch_id,))
    await broadcast_change()
    return {"message": "鎵规宸插垹闄?}


@app.put("/api/batch-nodes")
async def update_batch_node(data: BatchNodeUpdate):
    with get_db() as db:
        st = data.start_time
        db.execute("UPDATE batch_nodes SET start_time=?, end_time=? WHERE batch_id=? AND node_name=?",
                   (st, st, data.batch_id, data.node_name))
        db.execute("UPDATE batches SET updated_at=? WHERE id=?", (_mk_time(), data.batch_id))
        recalc_batch_overdue(db, data.batch_id)
    await broadcast_change()
    return {"message": "鑺傜偣宸叉洿鏂?}


@app.put("/api/opt-date")
async def update_opt_date(data: OptDateUpdate):
    with get_db() as db:
        db.execute("UPDATE batch_nodes SET opt_date=? WHERE batch_id=? AND node_name=?",
                   (data.opt_date, data.batch_id, data.node_name))
        db.execute("UPDATE batches SET updated_at=? WHERE id=?", (_mk_time(), data.batch_id))
    await broadcast_change()
    return {"message": "鍙€夋棩鏈熷凡鏇存柊"}


@app.get("/api/node-config")
def get_node_config():
    with get_db() as db:
        rows = db.execute("SELECT * FROM node_config ORDER BY sort_order").fetchall()
        return [{"node_name": r["node_name"], "sort_order": r["sort_order"],
                 "estimated_days": r["estimated_days"], "is_timepoint": bool(r["is_timepoint"])} for r in rows]


@app.put("/api/node-config")
async def update_node_config(data: NodeConfigUpdate):
    with get_db() as db:
        db.execute("UPDATE node_config SET estimated_days=? WHERE node_name=?",
                   (data.estimated_days, data.node_name))
        batch_ids = db.execute("SELECT id FROM batches").fetchall()
        for bid in batch_ids:
            recalc_batch_overdue(db, bid["id"])
    await broadcast_change()
    return {"message": "棰勪及鐢ㄦ椂宸叉洿鏂?}


@app.put("/api/batch-notes")
async def update_batch_note(data: BatchNoteUpdate):
    with get_db() as db:
        db.execute("UPDATE batches SET notes=?, updated_at=? WHERE id=?", (data.notes, _mk_time(), data.batch_id))
    await broadcast_change()
    return {"message": "澶囨敞宸叉洿鏂?}


@app.get("/api/anomalies")
def get_anomalies():
    with get_db() as db:
        rows = db.execute("""
            SELECT bn.id, b.country, b.batch_name, bn.node_name, bn.start_time,
                   nc.estimated_days, nc.sort_order
            FROM batch_nodes bn JOIN batches b ON bn.batch_id=b.id
            JOIN node_config nc ON bn.node_name=nc.node_name
            WHERE bn.is_overdue=1 ORDER BY b.country, b.batch_name, nc.sort_order
        """).fetchall()
        result = []
        for r in rows:
            prev = db.execute("""
                SELECT bn2.start_time FROM batch_nodes bn2
                JOIN node_config nc2 ON bn2.node_name=nc2.node_name
                WHERE bn2.batch_id=(SELECT batch_id FROM batch_nodes WHERE id=?)
                AND nc2.sort_order=? ORDER BY nc2.sort_order LIMIT 1
            """, (r["id"], r["sort_order"] - 1)).fetchone()
            prev_time = prev["start_time"] if prev else ""
            result.append({
                "country": r["country"], "batch_name": r["batch_name"],
                "node_name": r["node_name"], "start_time": r["start_time"],
                "prev_time": prev_time, "estimated_days": r["estimated_days"]
            })
        return result


@app.get("/api/stats")
def get_stats(country: str = ""):
    with get_db() as db:
        country_filter = "WHERE country=?" if country else ""
        country_params = (country,) if country else ()

        country_stats = db.execute("SELECT country, COUNT(*) as cnt FROM batches GROUP BY country ORDER BY country").fetchall()
        countries = [{"country": r["country"], "count": r["cnt"]} for r in country_stats]
        total_batches = db.execute(f"SELECT COUNT(*) as cnt FROM batches {country_filter}", country_params).fetchone()["cnt"]

        batches = db.execute(f"SELECT id FROM batches {country_filter} ORDER BY id", country_params).fetchall()
        node_config = db.execute("SELECT node_name, sort_order FROM node_config ORDER BY sort_order").fetchall()
        node_names = [nc["node_name"] for nc in node_config]
        node_counts = {name: 0 for name in node_names}
        node_counts["宸插畬鎴?] = 0

        for b in batches:
            nodes = db.execute("""
                SELECT bn.node_name, bn.start_time, nc.sort_order FROM batch_nodes bn
                JOIN node_config nc ON bn.node_name=nc.node_name
                WHERE bn.batch_id=? ORDER BY nc.sort_order
            """, (b["id"],)).fetchall()
            current = "宸插畬鎴?
            for n in nodes:
                if not n["start_time"]:
                    current = n["node_name"]
                    break
            node_counts[current] = node_counts.get(current, 0) + 1

        nodes = [{"node_name": name, "count": node_counts.get(name, 0)} for name in node_names + ["宸插畬鎴?]]
        return {"total_batches": total_batches, "countries": countries, "nodes": nodes}


@app.get("/")
def index():
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

if __name__ == "__main__":
    init_db()
    if TURSO_URL:
        print("v6 Turso 妯″紡鍚姩: http://localhost:8765")
    else:
        print("v6 鏈湴妯″紡鍚姩: http://localhost:8765")
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
