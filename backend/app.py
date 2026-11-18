import hashlib, secrets, sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 同主题并发入库：SQLite BEGIN IMMEDIATE 负责排队；落库前读一次该主题写入水位，
# 排队后若水位已被同主题别人推进，即判为后到，记主题争用。
BUSY_TIMEOUT = 10.0

KIND_CROSS_REJECT = "串写拒绝"      # 主题与声明版本对不上
KIND_DEVIATION = "串写口径偏了"      # 主题不在启用主题簿内
KIND_CROSS_CONTENTION = "串写争用"   # 条件更新影响行数为零
KIND_SUBJECT_CONTENTION = "主题争用"  # 同主题并发后到


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    # isolation_level=None：由调用方显式 BEGIN / COMMIT，便于把校验与落库放进同一事务。
    conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        /* 主题串写簿：拒绝、口径偏离与争用一律留痕，成功入站不写此簿。 */
        CREATE TABLE IF NOT EXISTS subject_write_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          declared_version TEXT,
          matched_version TEXT,
          kind TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT,
          created_at TEXT NOT NULL
        );
        /* 同主题并发入库由 BEGIN IMMEDIATE 与条件写入保证顺序，无需额外占位表。 */
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3", "订单入站", "严格", "正式", "启用", "order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow", "订单入站", "兼容", "影子", "试跑", "order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2", "库存入站", "严格", "正式", "停用", "sku,qty"))


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}


class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present
    # 抓取补采可声明本批按哪个契约版本校验；留空表示按该主题的启用正式契约。
    contract_version: str | None = Field(default=None, validation_alias=AliasChoices("contract_version", "version"))
    source: str | None = None
    model_config = ConfigDict(populate_by_name=True)


def enabled_formal_contract(conn, subject: str):
    """甲路径的只读口径：按主题查启用正式契约。"""
    return conn.execute(
        "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1",
        (subject,),
    ).fetchone()


def contract_by_version(conn, version: str | None):
    if not version:
        return None
    return conn.execute("SELECT * FROM contracts WHERE version=? ORDER BY id DESC LIMIT 1", (version,)).fetchone()


def log_ledger(conn, subject, declared_version, matched_version, kind, reason, source=None):
    conn.execute(
        "INSERT INTO subject_write_ledger(subject,declared_version,matched_version,kind,reason,source,created_at) VALUES (?,?,?,?,?,?,?)",
        (subject, declared_version, matched_version, kind, reason, source, now_str()),
    )


def reject(status: int, reason: str, **extra) -> JSONResponse:
    body = {"detail": reason, "reason": reason, "reject": True}
    body.update({k: v for k, v in extra.items() if v is not None})
    return JSONResponse(status_code=status, content=body)


def insert_conditionally(conn, subject, payload_fields, version, result, detail, created):
    """乙路径：主题与版本必须一并命中启用正式契约，影响行数为零即视为争用。"""
    return conn.execute(
        """INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
           SELECT ?,?,c.version,?,?,?
           FROM contracts c
           WHERE c.subject=? AND c.version=? AND c.track='正式' AND c.status='启用'""",
        (subject, payload_fields, result, detail, created, subject, version),
    )


def subject_marker(conn, subject):
    """同主题写入水位：读取该主题已落库批次的最大批次号与行数。"""
    row = conn.execute(
        "SELECT COALESCE(MAX(id),0) m, COUNT(*) n FROM ingress_batches WHERE subject=?", (subject,)
    ).fetchone()
    return (row["m"], row["n"])


def _is_lock_error(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()


@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    subject = body.subject
    declared = (body.contract_version or "").strip() or None
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}

    # 甲路径：落库前按主题查启用正式契约，无匹配则拒绝且零写入。
    with connect() as c:
        ctr = enabled_formal_contract(c, subject)
        if not ctr:
            owner = contract_by_version(c, declared)
            if owner and owner["subject"] != subject:
                kind = KIND_CROSS_REJECT
                reason = (f"主题串写拒绝：主题「{subject}」无启用正式契约，"
                          f"声明版本「{declared}」属于主题「{owner['subject']}」"
                          f"（{owner['status']}/{owner['track']}）；已拒绝且零写入")
            else:
                kind = KIND_DEVIATION
                reason = f"串写口径偏了：主题「{subject}」无启用正式契约；已拒绝且零写入"
                if owner:
                    reason += f"（该主题契约「{declared}」为{owner['status']}/{owner['track']}）"
            log_ledger(c, subject, declared, None, kind, reason, body.source)
            c.commit()
            return reject(409 if declared else 404, reason, kind=kind,
                          subject=subject, declared_version=declared)
        if declared and declared != ctr["version"]:
            owner = contract_by_version(c, declared)
            if owner and owner["subject"] == subject:
                owned = f"该版本是同一主题的{owner['track']}契约（{owner['status']}），并非启用正式契约"
            elif owner:
                owned = f"该版本属于主题「{owner['subject']}」（{owner['status']}/{owner['track']}）"
            else:
                owned = "该版本不在版本目录中"
            reason = (f"主题串写拒绝：主题「{subject}」与声明版本「{declared}」不一致，{owned}；"
                      f"该主题启用契约为「{ctr['version']}」，已拒绝且零写入")
            log_ledger(c, subject, declared, ctr["version"], KIND_CROSS_REJECT, reason, body.source)
            c.commit()
            return reject(409, reason, kind=KIND_CROSS_REJECT, subject=subject,
                          declared_version=declared, enabled_version=ctr["version"])
        version = ctr["version"]
        snapshot = subject_marker(c, subject)

    # 同主题抢入库：BEGIN IMMEDIATE 排队；后到（水位已变）只记主题争用，不静默。
    try:
        with connect() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                overtaken = subject_marker(c, subject) != snapshot
                if overtaken:
                    log_ledger(c, subject, declared, version, KIND_SUBJECT_CONTENTION,
                               f"主题争用：主题「{subject}」同主题并发入库，后到请求排队后写入",
                               body.source)

                # 落库前复核：主题与版本必须同时仍是启用正式契约。
                ctr = enabled_formal_contract(c, subject)
                if not ctr or ctr["version"] != version:
                    reason = (f"串写争用：主题「{subject}」落库前启用契约已变化（期望「{version}」），"
                              f"已拒绝且零写入")
                    log_ledger(c, subject, declared, version, KIND_CROSS_CONTENTION, reason, body.source)
                    c.commit()
                    return reject(409, reason, kind=KIND_CROSS_CONTENTION,
                                  subject=subject, declared_version=declared, enabled_version=version)

                need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
                missing = sorted(need - fields)
                if missing and ctr["mode"] == "兼容":
                    result, detail = "缓冲", "缺字段:" + ",".join(missing)
                elif missing:
                    result, detail = "拒收", "缺字段:" + ",".join(missing)
                else:
                    result, detail = "通过", "字段齐全"

                cur = insert_conditionally(c, subject, body.payload_fields, version, result, detail, now_str())
                if cur.rowcount != 1:
                    reason = (f"串写争用：主题「{subject}」与版本「{version}」条件更新影响行数为零，"
                              f"已拒绝且零写入")
                    log_ledger(c, subject, declared, version, KIND_CROSS_CONTENTION, reason, body.source)
                    c.commit()
                    return reject(409, reason, kind=KIND_CROSS_CONTENTION,
                                  subject=subject, declared_version=declared, enabled_version=version)

                batch_id = cur.lastrowid
                if result == "缓冲":
                    c.execute("INSERT INTO buffers(batch_id,reason) VALUES(?,?)",
                              (batch_id, "缺字段进双写缓冲"))
                c.commit()
            except Exception:
                c.rollback()
                raise
    except sqlite3.OperationalError as exc:
        if not _is_lock_error(exc):
            raise
        with connect() as c:
            reason = f"主题争用：主题「{subject}」同主题并发入库排队超时，已拒绝且零写入"
            log_ledger(c, subject, declared, version, KIND_SUBJECT_CONTENTION, reason, body.source)
            c.commit()
        return reject(409, reason, kind=KIND_SUBJECT_CONTENTION,
                      subject=subject, declared_version=declared, enabled_version=version)

    resp = {"result": result, "version": version, "subject": subject,
            "contract_subject": subject, "batch_id": batch_id}
    if missing:
        resp["missing"] = missing
    return resp


@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT i.*, c.subject AS contract_subject,
                   CASE WHEN c.subject = i.subject THEN 1 ELSE 0 END AS subject_consistent
            FROM ingress_batches i
            LEFT JOIN contracts c ON c.version = i.contract_version
            ORDER BY i.id DESC
        """).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["subject_consistent"] = bool(d["subject_consistent"])
            items.append(d)
        return {"items": items}


@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT b.*, i.subject, i.payload_fields, i.contract_version,
                   c.subject AS contract_subject,
                   CASE WHEN c.subject = i.subject THEN 1 ELSE 0 END AS subject_consistent
            FROM buffers b
            JOIN ingress_batches i ON i.id = b.batch_id
            LEFT JOIN contracts c ON c.version = i.contract_version
            ORDER BY b.id DESC
        """).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["subject_consistent"] = bool(d["subject_consistent"])
            items.append(d)
        return {"items": items}


@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        mismatch = c.execute("""
            SELECT COUNT(*) c FROM ingress_batches i
            LEFT JOIN contracts c ON c.version = i.contract_version
            WHERE c.subject IS NULL OR c.subject != i.subject
        """).fetchone()["c"]
        counts = {row["kind"]: row["c"] for row in c.execute(
            "SELECT kind, COUNT(*) c FROM subject_write_ledger GROUP BY kind").fetchall()}
        ledger = [dict(r) for r in c.execute(
            "SELECT * FROM subject_write_ledger ORDER BY id DESC LIMIT 50").fetchall()]
    return {
        "ingress_total": total,
        "passed": passed,
        "buffered": buffered,
        "rejected": rejected,
        "buffer_rows": buf_rows,
        "aligned": buffered == buf_rows,
        "subject_mismatch": mismatch,
        "subject_aligned": mismatch == 0,
        "cross_write_rejections": counts.get(KIND_CROSS_REJECT, 0),
        "ledger_deviations": counts.get(KIND_DEVIATION, 0),
        "cross_write_contentions": counts.get(KIND_CROSS_CONTENTION, 0),
        "subject_contentions": counts.get(KIND_SUBJECT_CONTENTION, 0),
        "ledger": ledger,
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
