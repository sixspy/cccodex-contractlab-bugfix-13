import threading

import pytest
from fastapi.testclient import TestClient

import app as A


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    A.SESSIONS.clear()
    A.init_db()
    yield


def _client():
    c = TestClient(A.app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _batches():
    with A.connect() as c:
        return [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id").fetchall()]


def _ledger():
    with A.connect() as c:
        return [dict(r) for r in c.execute("SELECT * FROM subject_write_ledger ORDER BY id").fetchall()]


# ---------------------------------------------------------------- 1. 错填主题复现串写成功
# 修复前：IngressIn 根本不接收 contract_version，调用方按 v2（库存入站）补采却把主题写成
# 订单入站，服务端静默忽略声明版本、按订单入站 v3 判成功并落成“通过”批次，主题与版本对不上。
# 复现用例按修复前的旧契约（不声明版本）构造，必须仍然落成成功批次，用来证明历史批次是只读的。

def test_wrong_subject_crosswrite_reproduced_as_success():
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency",
                     "source": "抓取补采"})
    assert r.status_code == 200 and r.json()["result"] == "通过"
    rows = _batches()
    assert len(rows) == 1 and rows[0]["result"] == "通过"
    assert rows[0]["subject"] == "订单入站" and rows[0]["contract_version"] == "v3"


def test_crosswrite_repro_pre_fix_semantics_declared_version_ignored():
    """修复前的缺陷点：请求里的版本被静默丢弃（IngressIn 无此字段）。修复后必须接收并校验。"""
    parsed = A.IngressIn(**{"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert parsed.contract_version is None
    parsed = A.IngressIn(**{"subject": "订单入站", "payload_fields": "order_id,amount,currency",
                            "contract_version": "v2"})
    assert parsed.contract_version == "v2", "声明版本不得再被静默忽略"


# ---------------------------------------------------------------- 2. 落库前匹配失败测（甲路径）

def test_declared_version_from_other_subject_rejected_zero_write():
    c, h = _client()
    before = len(_batches())
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency",
                     "contract_version": "v2", "source": "抓取补采"})
    assert r.status_code == 409
    j = r.json()
    assert j["reject"] is True and j["kind"] == A.KIND_CROSS_REJECT
    assert j["declared_version"] == "v2" and j["enabled_version"] == "v3"
    assert "库存入站" in j["reason"]
    assert len(_batches()) == before, "拒绝必须零写入"
    led = _ledger()
    assert len(led) == 1 and led[0]["kind"] == A.KIND_CROSS_REJECT
    assert led[0]["source"] == "抓取补采" and "串写拒绝" in led[0]["reason"]


def test_enabled_formal_contract_missing_rejected_zero_write():
    c, h = _client()
    before = len(_batches())
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "库存入站", "payload_fields": "sku,qty"})
    assert r.status_code in (404, 409)
    j = r.json()
    assert j["reject"] is True and j["kind"] == A.KIND_DEVIATION
    assert "口径偏了" in j["reason"]
    assert len(_batches()) == before
    assert _ledger()[0]["kind"] == A.KIND_DEVIATION


def test_unknown_subject_rejected_with_reason():
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订货入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code in (404, 409)
    assert r.json()["reject"] is True and r.json()["reason"]
    assert _batches() == []


# ---------------------------------------------------------------- 3. 启用契约命中 / 条件更新命中

def test_enabled_contract_hit_matches_subject_and_version():
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency",
                     "contract_version": "v3"})
    assert r.status_code == 200
    j = r.json()
    assert j["result"] == "通过" and j["version"] == "v3" and j["subject"] == "订单入站"
    rows = _batches()
    assert rows[0]["subject"] == "订单入站" and rows[0]["contract_version"] == "v3"


def test_conditional_update_zero_rows_logged_as_cross_contention(monkeypatch):
    """乙路径：条件更新影响行数为零 → 记串写争用，零写入。"""
    c, h = _client()
    before = len(_batches())

    class Zero:
        rowcount = 0
        lastrowid = None

    def zero_rows(*a, **kw):
        return Zero()

    monkeypatch.setattr("app.insert_conditionally", zero_rows)
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 409
    j = r.json()
    assert j["kind"] == A.KIND_CROSS_CONTENTION and "影响行数为零" in j["reason"]
    assert len(_batches()) == before
    assert _ledger()[-1]["kind"] == A.KIND_CROSS_CONTENTION


def test_conditional_update_rechecks_enabled_contract(monkeypatch):
    """落库前复核：启用契约在排队期间变化 → 记串写争用。"""
    c, h = _client()
    real = A.enabled_formal_contract
    calls = {"n": 0}

    def flip(conn, subject):
        calls["n"] += 1
        row = real(conn, subject)
        if calls["n"] >= 2:
            return None
        return row

    monkeypatch.setattr("app.enabled_formal_contract", flip)
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 409 and r.json()["kind"] == A.KIND_CROSS_CONTENTION
    assert _batches() == []


# ---------------------------------------------------------------- 4. 同主题并发：后到记争用

def test_same_subject_concurrent_ingress_records_contention(monkeypatch):
    """同主题两名值班同时抢入库：双方读到同一水位，先到者入库，后到者排队后只记主题争用。"""
    c, h = _client()
    real = A.subject_marker
    barrier = threading.Barrier(2, timeout=10)
    lock = threading.Lock()
    pre_reads = {"n": 0}
    results = {}
    pre_snapshots = []

    def gated_marker(conn, subject):
        with lock:
            pre_reads["n"] += 1
            idx = pre_reads["n"]
        if idx <= 2:                      # 落库前的两次快照读：强制双方同时读到同一水位
            marker = real(conn, subject)
            pre_snapshots.append(marker)
            barrier.wait()
            return marker
        return real(conn, subject)

    monkeypatch.setattr("app.subject_marker", gated_marker)

    def worker(name):
        results[name] = c.post("/api/ingress/validate", headers=h,
                               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})

    ts = [threading.Thread(target=worker, args=(n,)) for n in ("a", "b")]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=15)

    assert len(pre_snapshots) == 2 and pre_snapshots[0] == pre_snapshots[1], "双方须读到同一水位"
    assert all(r.status_code == 200 for r in results.values()), {k: r.text for k, r in results.items()}
    contention = [l for l in _ledger() if l["kind"] == A.KIND_SUBJECT_CONTENTION]
    assert len(contention) == 1, "同主题并发后到必须记主题争用"
    assert len(_batches()) == 2


def test_same_subject_true_overlap_records_contention(monkeypatch):
    """真正交叠：先到请求读到水位后、写入前被同主题另一请求抢先落库，后到者记主题争用。"""
    c, h = _client()
    real = A.subject_marker
    calls = {"n": 0}

    def racing_marker(conn, subject):
        marker = real(conn, subject)
        calls["n"] += 1
        if calls["n"] == 1:            # 快照读之后，模拟另一名值班抢先落库
            conn.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
                         " VALUES ('订单入站','order_id,amount,currency','v3','通过','字段齐全','x')")
        return marker

    monkeypatch.setattr("app.subject_marker", racing_marker)
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 200, r.text
    kinds = [l["kind"] for l in _ledger()]
    assert kinds == [A.KIND_SUBJECT_CONTENTION]
    assert len(_batches()) == 2


def test_different_subjects_do_not_record_contention():
    c, h = _client()
    with A.connect() as c2:
        c2.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                   ("v8", "补采入站", "严格", "正式", "启用", "a"))
    r1 = c.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    r2 = c.post("/api/ingress/validate", headers=h, json={"subject": "补采入站", "payload_fields": "a"})
    assert r1.status_code == r2.status_code == 200
    assert not [l for l in _ledger() if l["kind"] == A.KIND_SUBJECT_CONTENTION]


# ---------------------------------------------------------------- 5. 历史正确批次只读 + 各处主题一致

def test_history_batches_not_rewritten_by_fix():
    c, h = _client()
    c.post("/api/ingress/validate", headers=h,
           json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    snapshot = _batches()
    c.post("/api/ingress/validate", headers=h,
           json={"subject": "订单入站", "payload_fields": "order_id,amount", "contract_version": "v2"})
    assert _batches() == snapshot, "串写拒绝不得改写历史正确批次"


def test_lists_detail_reconcile_share_subject():
    c, h = _client()
    c.post("/api/ingress/validate", headers=h,
           json={"subject": "订单入站", "payload_fields": "order_id,amount"})   # 严格模式拒收
    c.post("/api/ingress/validate", headers=h,
           json={"subject": "订单入站", "payload_fields": "order_id,amount", "contract_version": "v3"})
    items = c.get("/api/ingress", headers=h).json()["items"]
    assert items and all(i["subject"] == i["contract_subject"] for i in items)
    assert all(i["subject_consistent"] for i in items)
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["subject_aligned"] is True and recon["subject_mismatch"] == 0
    assert recon["subject_contentions"] == 0 and recon["cross_write_rejections"] == 0


def test_compat_mode_buffer_still_works_and_aligned():
    c, h = _client()
    with A.connect() as conn:
        conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                     ("v9", "补采入站", "兼容", "正式", "启用", "a,b"))
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "补采入站", "payload_fields": "a"})
    assert r.status_code == 200 and r.json()["result"] == "缓冲"
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["aligned"] is True and recon["buffered"] == 1 and recon["buffer_rows"] == 1
    buf = c.get("/api/buffers", headers=h).json()["items"]
    assert buf[0]["subject"] == buf[0]["contract_subject"] == "补采入站"
    assert buf[0]["subject_consistent"] is True


def test_conditional_update_sql_requires_subject_and_version():
    with A.connect() as conn:
        conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                     ("v7", "甲入站", "严格", "正式", "启用", "x"))
    with A.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        cur = A.insert_conditionally(conn, "甲入站", "x", "v7", "通过", "字段齐全", "now")
        assert cur.rowcount == 1
        cur = A.insert_conditionally(conn, "乙入站", "x", "v7", "通过", "字段齐全", "now")
        assert cur.rowcount == 0, "版本属于甲主题，乙主题不得命中"
        cur = A.insert_conditionally(conn, "甲入站", "x", "v9", "通过", "字段齐全", "now")
        assert cur.rowcount == 0, "版本对不上不得命中"
        conn.rollback()
