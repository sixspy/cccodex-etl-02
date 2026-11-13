import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

# 管道闸口状态机：待抽取 → 已抽取待清洗 → 清洗中 → 已清洗待加载 → 已落库
# 隔离口径下脏数据进隔离筐：清洗后转 隔离待补齐，补齐再送清洗过关才到 已清洗待加载
ST_WAIT_EXTRACT = "待抽取"
ST_EXTRACT_FAIL = "抽取失败"
ST_WAIT_CLEAN = "已抽取待清洗"
ST_CLEANING = "清洗中"
ST_WAIT_FIX = "隔离待补齐"
ST_WAIT_LOAD = "已清洗待加载"
ST_LOADED = "已落库"

CLEANABLE_STATUSES = (ST_WAIT_CLEAN, ST_WAIT_FIX)

# 行级状态
ROW_WAIT = "待清洗"
ROW_DROPPED = "已丢弃"
ROW_QUARANTINE = "隔离中"
ROW_FIXED = "已补齐"
ROW_CLEANED = "已清洗"
ROW_LOADED = "已落库"

# 脏数据口径：写进说明后全天不得改弦
POLICY_NAMES = {"drop": "空值丢弃", "quarantine": "空值隔离"}


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              loaded INTEGER NOT NULL DEFAULT 0,
              batch INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS batch_rows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              batch INTEGER NOT NULL,
              row_no INTEGER NOT NULL,
              title TEXT,
              amount REAL,
              state TEXT NOT NULL DEFAULT '待清洗',
              reason TEXT,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_batch_rows_pipe
              ON batch_rows (pipeline_id, state);
            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              kind TEXT NOT NULL,
              detail TEXT,
              operator TEXT,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            """
        )
        _migrate(conn)
        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )


def _migrate(conn: sqlite3.Connection) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(pipelines)")}
    if "batch" not in cols:
        conn.execute(
            "ALTER TABLE pipelines ADD COLUMN batch INTEGER NOT NULL DEFAULT 0"
        )
    # 旧状态并入清洗闸：已抽取待加载 → 已抽取待清洗
    conn.execute(
        "UPDATE pipelines SET status = ? WHERE status = ?",
        (ST_WAIT_CLEAN, "已抽取待加载"),
    )


# ---------- 读取 ----------

def _counts(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    rows = conn.execute(
        "SELECT state, COUNT(*) AS c FROM batch_rows "
        "WHERE pipeline_id = ? GROUP BY state",
        (pipeline_id,),
    ).fetchall()
    by_state = {r["state"]: r["c"] for r in rows}
    return {
        "pending_clean": by_state.get(ROW_WAIT, 0),
        "dropped": by_state.get(ROW_DROPPED, 0),
        "quarantined": by_state.get(ROW_QUARANTINE, 0),
        "fixed": by_state.get(ROW_FIXED, 0),
        "cleaned": by_state.get(ROW_CLEANED, 0),
    }


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM pipelines ORDER BY id").fetchall()
    items = []
    for r in rows:
        item = dict(r)
        item["counts"] = _counts(conn, item["id"])
        items.append(item)
    return items


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["counts"] = _counts(conn, pipeline_id)
    return item


def pipeline_detail(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    item["quarantine"] = [
        dict(r)
        for r in conn.execute(
            "SELECT id, row_no, title, amount, state, reason FROM batch_rows "
            "WHERE pipeline_id = ? AND state IN (?, ?) ORDER BY row_no",
            (pipeline_id, ROW_QUARANTINE, ROW_FIXED),
        ).fetchall()
    ]
    item["dropped_rows"] = [
        dict(r)
        for r in conn.execute(
            "SELECT id, row_no, title, amount, state, reason FROM batch_rows "
            "WHERE pipeline_id = ? AND state = ? ORDER BY row_no",
            (pipeline_id, ROW_DROPPED),
        ).fetchall()
    ]
    item["events"] = [
        dict(r)
        for r in conn.execute(
            "SELECT id, kind, detail, operator, created_at FROM events "
            "WHERE pipeline_id = ? ORDER BY id DESC LIMIT 30",
            (pipeline_id,),
        ).fetchall()
    ]
    return item


def _log(
    conn: sqlite3.Connection,
    pipeline_id: int,
    kind: str,
    detail: str,
    operator: str = "",
) -> None:
    conn.execute(
        "INSERT INTO events (pipeline_id, kind, detail, operator) VALUES (?,?,?,?)",
        (pipeline_id, kind, detail, operator),
    )


# ---------- 脏数据口径（规则设置） ----------

def get_policy(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'clean_policy'"
    ).fetchone()
    if not row:
        return None
    try:
        doc = json.loads(row["value"])
    except (ValueError, TypeError):
        return None
    return doc if isinstance(doc, dict) else None


def policy_view(conn: sqlite3.Connection) -> dict:
    doc = get_policy(conn)
    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "policy": doc,
        "locked": bool(doc and doc.get("date") == today),
        "names": POLICY_NAMES,
        "today": today,
    }


def set_policy(
    conn: sqlite3.Connection, policy: str, note: str, operator: str = ""
) -> tuple[dict | None, tuple[int, str] | None]:
    if policy not in POLICY_NAMES:
        return None, (400, "未知清洗口径")
    today = datetime.now().strftime("%Y-%m-%d")
    existing = get_policy(conn)
    if existing and existing.get("date") == today:
        return existing, (400, "口径已写进说明，全天不得改弦")
    doc = {
        "policy": policy,
        "name": POLICY_NAMES[policy],
        "note": str(note or "").strip(),
        "date": today,
        "operator": operator,
    }
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('clean_policy', ?)",
        (json.dumps(doc, ensure_ascii=False),),
    )
    return doc, None


# ---------- 脏数据判定 ----------

def _is_dirty(title, amount) -> bool:
    # 标题字段为空算脏数据；任何空值行都算脏数据
    return title is None or not str(title).strip() or amount is None


def _row_reason(row: sqlite3.Row) -> str:
    parts = []
    if row["title"] is None or not str(row["title"]).strip():
        parts.append("标题字段为空")
    if row["amount"] is None:
        parts.append("金额字段为空")
    return "、".join(parts) or "空值行"


# ---------- 抽取 ----------

def run_extract(
    conn: sqlite3.Connection, pipeline_id: int, operator: str = ""
) -> tuple[dict | None, tuple[int, str] | None]:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None, None
    if item["status"] not in (ST_WAIT_EXTRACT, ST_EXTRACT_FAIL):
        return item, None
    batch = item["batch"] + 1
    n = 12 + (pipeline_id % 5)
    conn.execute("DELETE FROM batch_rows WHERE pipeline_id = ?", (pipeline_id,))
    rows = []
    for i in range(1, n + 1):
        title = f"{item['code']}-B{batch}-{i:03d}"
        amount = round(37 + i * 11.5, 2)
        if i % 4 == 0:
            title = ""  # 标题字段为空算脏数据
        if i % 5 == 0:
            amount = None  # 空值行
        rows.append((pipeline_id, batch, i, title, amount))
    conn.executemany(
        "INSERT INTO batch_rows (pipeline_id, batch, row_no, title, amount) "
        "VALUES (?,?,?,?,?)",
        rows,
    )
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0, batch = ? "
        "WHERE id = ?",
        (ST_WAIT_CLEAN, n, batch, pipeline_id),
    )
    dirty = sum(1 for _, _, _, t, a in rows if _is_dirty(t, a))
    _log(conn, pipeline_id, "抽取", f"第 {batch} 批抽取 {n} 行，其中脏数据 {dirty} 行", operator)
    return get_pipeline(conn, pipeline_id), None


# ---------- 清洗闸 ----------

def run_clean(
    conn: sqlite3.Connection,
    pipeline_id: int,
    req_policy: str,
    operator: str = "",
    mode: str = "clean",
) -> tuple[dict | None, tuple[int, str] | None]:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None, None
    stored = get_policy(conn)
    if not stored:
        return item, (400, "未设置清洗口径，请先在规则设置写进说明")
    if req_policy != stored["policy"]:
        _log(
            conn,
            pipeline_id,
            "清洗口径写歪了",
            f"送来口径「{POLICY_NAMES.get(req_policy, req_policy or '空')}」，"
            f"说明口径「{stored['name']}」",
            operator,
        )
        return item, (400, "清洗口径写歪了")
    # 首洗与补齐再洗是两道闸：同一批只留一笔，状态 CAS 抢不到即争用失败
    gate_status = ST_WAIT_FIX if mode == "reclean" else ST_WAIT_CLEAN
    cur = conn.execute(
        "UPDATE pipelines SET status = ? WHERE id = ? AND status = ?",
        (ST_CLEANING, pipeline_id, gate_status),
    )
    if cur.rowcount == 0:
        fresh = get_pipeline(conn, pipeline_id)
        if fresh["status"] in (ST_WAIT_EXTRACT, ST_EXTRACT_FAIL):
            return fresh, (409, "未抽取成功，不能清洗")
        _log(conn, pipeline_id, "清洗争用失败", "同一批清洗只留一笔，本次争抢未中", operator)
        return fresh, (409, "清洗争用失败")
    # 清洗中落锁单独提交，同时争抢的另一侧立刻看见闸已占
    conn.commit()
    try:
        if gate_status == ST_WAIT_FIX:
            err = _reclean_after_fix(conn, pipeline_id, operator)
            if err:
                conn.execute(
                    "UPDATE pipelines SET status = ? WHERE id = ?",
                    (ST_WAIT_FIX, pipeline_id),
                )
                return get_pipeline(conn, pipeline_id), err
        else:
            _apply_clean(conn, item, stored, operator)
    except Exception:
        conn.rollback()
        conn.execute(
            "UPDATE pipelines SET status = ? WHERE id = ?",
            (gate_status, pipeline_id),
        )
        conn.commit()
        raise
    return get_pipeline(conn, pipeline_id), None


def _apply_clean(
    conn: sqlite3.Connection, item: dict, stored: dict, operator: str
) -> None:
    pid = item["id"]
    rows = conn.execute(
        "SELECT * FROM batch_rows WHERE pipeline_id = ? AND state = ? ORDER BY row_no",
        (pid, ROW_WAIT),
    ).fetchall()
    dirty = [r for r in rows if _is_dirty(r["title"], r["amount"])]
    clean = [r for r in rows if not _is_dirty(r["title"], r["amount"])]
    for r in clean:
        conn.execute(
            "UPDATE batch_rows SET state = ? WHERE id = ?", (ROW_CLEANED, r["id"])
        )
    if stored["policy"] == "drop":
        # 空值行直接丢掉并累计丢弃数
        for r in dirty:
            conn.execute(
                "UPDATE batch_rows SET state = ?, reason = ? WHERE id = ?",
                (ROW_DROPPED, _row_reason(r), r["id"]),
            )
        new_status = ST_WAIT_LOAD
        move = f"丢弃 {len(dirty)} 行"
    else:
        # 空值行进隔离筐，补齐前禁止落库
        for r in dirty:
            conn.execute(
                "UPDATE batch_rows SET state = ?, reason = ? WHERE id = ?",
                (ROW_QUARANTINE, _row_reason(r), r["id"]),
            )
        new_status = ST_WAIT_FIX if dirty else ST_WAIT_LOAD
        move = f"隔离 {len(dirty)} 行"
    conn.execute("UPDATE pipelines SET status = ? WHERE id = ?", (new_status, pid))
    _log(
        conn,
        pid,
        "清洗通过",
        f"口径「{stored['name']}」：洗净 {len(clean)} 行，{move}",
        operator,
    )


def _reclean_after_fix(
    conn: sqlite3.Connection, pipeline_id: int, operator: str
) -> tuple[int, str] | None:
    open_q = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_rows WHERE pipeline_id = ? AND state = ?",
        (pipeline_id, ROW_QUARANTINE),
    ).fetchone()["c"]
    if open_q > 0:
        return (409, "隔离筐未补齐，不能送清洗")
    fixed = conn.execute(
        "SELECT * FROM batch_rows WHERE pipeline_id = ? AND state = ?",
        (pipeline_id, ROW_FIXED),
    ).fetchall()
    still_dirty = [r for r in fixed if _is_dirty(r["title"], r["amount"])]
    ok = [r for r in fixed if not _is_dirty(r["title"], r["amount"])]
    for r in ok:
        conn.execute(
            "UPDATE batch_rows SET state = ? WHERE id = ?", (ROW_CLEANED, r["id"])
        )
    for r in still_dirty:
        conn.execute(
            "UPDATE batch_rows SET state = ?, reason = ? WHERE id = ?",
            (ROW_QUARANTINE, _row_reason(r), r["id"]),
        )
    if still_dirty:
        conn.execute(
            "UPDATE pipelines SET status = ? WHERE id = ?", (ST_WAIT_FIX, pipeline_id)
        )
        _log(
            conn,
            pipeline_id,
            "清洗未过关",
            f"{len(still_dirty)} 行仍是脏数据，退回隔离筐",
            operator,
        )
    else:
        conn.execute(
            "UPDATE pipelines SET status = ? WHERE id = ?", (ST_WAIT_LOAD, pipeline_id)
        )
        _log(
            conn,
            pipeline_id,
            "清洗通过",
            f"隔离补齐 {len(ok)} 行再送清洗，全部洗净",
            operator,
        )
    return None


def fix_row(
    conn: sqlite3.Connection,
    pipeline_id: int,
    row_id: int,
    title: str,
    amount: float | None,
    operator: str = "",
) -> tuple[dict | None, tuple[int, str] | None]:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None, None
    row = conn.execute(
        "SELECT * FROM batch_rows WHERE id = ? AND pipeline_id = ?",
        (row_id, pipeline_id),
    ).fetchone()
    if not row:
        return item, (404, "隔离行不存在")
    if row["state"] != ROW_QUARANTINE:
        return item, (409, "该行不在隔离筐中")
    if not title or not str(title).strip():
        return item, (400, "标题字段为空算脏数据，补齐必须填标题")
    new_title = str(title).strip()
    conn.execute(
        "UPDATE batch_rows SET title = ?, amount = COALESCE(?, amount), state = ? "
        "WHERE id = ?",
        (new_title, amount, ROW_FIXED, row_id),
    )
    _log(conn, pipeline_id, "隔离补齐", f"第 {row['row_no']} 行补齐标题「{new_title}」", operator)
    return get_pipeline(conn, pipeline_id), None


# ---------- 落库门槛 ----------

def run_load(
    conn: sqlite3.Connection, pipeline_id: int, operator: str = ""
) -> tuple[dict | None, tuple[int, str] | None]:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None, None
    status = item["status"]
    if status != ST_WAIT_LOAD:
        if status in (ST_WAIT_EXTRACT, ST_EXTRACT_FAIL):
            return item, (409, "未抽取成功，不能落库")
        if status == ST_WAIT_CLEAN:
            return item, (409, "清洗未过关，禁止落库")
        if status == ST_WAIT_FIX:
            return item, (409, "隔离筐未补齐，禁止落库")
        if status == ST_CLEANING:
            return item, (409, "清洗中，稍候再落库")
        return item, (409, "当前状态不能落库")
    open_q = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_rows WHERE pipeline_id = ? AND state = ?",
        (pipeline_id, ROW_QUARANTINE),
    ).fetchone()["c"]
    if open_q > 0:
        return item, (409, "隔离筐未补齐，禁止落库")
    loaded = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_rows WHERE pipeline_id = ? AND state = ?",
        (pipeline_id, ROW_CLEANED),
    ).fetchone()["c"]
    conn.execute(
        "UPDATE batch_rows SET state = ? WHERE pipeline_id = ? AND state = ?",
        (ROW_LOADED, pipeline_id, ROW_CLEANED),
    )
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = ? WHERE id = ?",
        (ST_LOADED, loaded, pipeline_id),
    )
    _log(conn, pipeline_id, "落库", f"落库 {loaded} 行", operator)
    return get_pipeline(conn, pipeline_id), None
