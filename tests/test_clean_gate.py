"""清洗闸逐条核验：口径、隔离、落库门槛、争用、空标题、页面分区。"""

import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.main import app


def _login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200


def _set_policy(authed, policy="drop", note="口径说明"):
    return authed.post("/api/policy", json={"policy": policy, "note": note})


def _first_pid(authed):
    return authed.get("/api/pipelines").json()["items"][0]["id"]


def test_policy_locked_after_written(authed):
    """口径写进说明后全天不得改弦。"""
    r1 = _set_policy(authed, "drop", "空值行直接丢掉并累计丢弃数")
    assert r1.status_code == 200
    assert r1.json()["policy"]["name"] == "空值丢弃"
    assert r1.json()["locked"] is True
    r2 = _set_policy(authed, "quarantine", "想改弦")
    assert r2.status_code == 400
    assert "全天不得改弦" in r2.json()["error"]


def test_clean_requires_policy_and_extract(authed):
    """未设口径、未抽取，都进不了清洗闸。"""
    pid = _first_pid(authed)
    r = authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "drop"})
    assert r.status_code == 400
    assert "未设置清洗口径" in r.json()["error"]
    _set_policy(authed)
    r = authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "drop"})
    assert r.status_code == 409
    assert "未抽取" in r.json()["error"]


def test_policy_mismatch_returns_waiyu(authed):
    """点清洗时口径偏了，返回清洗口径写歪了，详情里也能看到。"""
    _set_policy(authed, "drop")
    pid = _first_pid(authed)
    authed.post(f"/api/pipelines/{pid}/extract")
    r = authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "quarantine"})
    assert r.status_code == 400
    assert r.json()["error"] == "清洗口径写歪了"
    detail = authed.get(f"/api/pipelines/{pid}").json()
    assert any(e["kind"] == "清洗口径写歪了" for e in detail["events"])


def test_load_blocked_before_clean(authed):
    """清洗未过关禁止落库。"""
    pid = _first_pid(authed)
    r = authed.post(f"/api/pipelines/{pid}/load")
    assert r.status_code == 409
    authed.post(f"/api/pipelines/{pid}/extract")
    r = authed.post(f"/api/pipelines/{pid}/load")
    assert r.status_code == 409
    assert "清洗未过关" in r.json()["error"]


def test_drop_policy_discards_dirty_rows(authed):
    """空值丢弃口径：脏行直接丢掉并累计丢弃数。"""
    _set_policy(authed, "drop")
    pid = _first_pid(authed)
    ex = authed.post(f"/api/pipelines/{pid}/extract").json()
    cl = authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "drop"})
    assert cl.status_code == 200
    body = cl.json()
    assert body["status"] == "已清洗待加载"
    dropped = body["counts"]["dropped"]
    assert dropped > 0
    assert body["counts"]["quarantined"] == 0
    detail = authed.get(f"/api/pipelines/{pid}").json()
    assert len(detail["dropped_rows"]) == dropped
    assert all(r["reason"] for r in detail["dropped_rows"])
    ld = authed.post(f"/api/pipelines/{pid}/load").json()
    assert ld["status"] == "已落库"
    assert ld["loaded"] == ex["extracted"] - dropped


def test_quarantine_policy_blocks_load_until_fixed(authed):
    """空值隔离口径：进隔离筐，补齐前禁止落库，补齐后可再送清洗。"""
    _set_policy(authed, "quarantine", "空值行进隔离筐，补齐前禁止落库")
    pid = _first_pid(authed)
    authed.post(f"/api/pipelines/{pid}/extract")
    cl = authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "quarantine"})
    assert cl.status_code == 200
    body = cl.json()
    assert body["status"] == "隔离待补齐"
    assert body["counts"]["quarantined"] > 0
    # 补齐前禁止落库
    blocked = authed.post(f"/api/pipelines/{pid}/load")
    assert blocked.status_code == 409
    assert "禁止落库" in blocked.json()["error"]
    # 隔离原因可读
    detail = authed.get(f"/api/pipelines/{pid}").json()
    quarantine = detail["quarantine"]
    assert quarantine and all(r["reason"] for r in quarantine)
    # 只补一条还不能送清洗
    first = quarantine[0]
    r = authed.post(
        f"/api/pipelines/{pid}/rows/{first['id']}/fix",
        json={"title": "补齐标题", "amount": 1.5},
    )
    assert r.status_code == 200
    early = authed.post(
        f"/api/pipelines/{pid}/clean",
        json={"policy": "quarantine", "mode": "reclean"},
    )
    assert early.status_code == 409
    assert "未补齐" in early.json()["error"]
    # 全部补齐
    detail = authed.get(f"/api/pipelines/{pid}").json()
    for row in detail["quarantine"]:
        if row["state"] == "隔离中":
            r = authed.post(
                f"/api/pipelines/{pid}/rows/{row['id']}/fix",
                json={"title": f"补齐{row['row_no']}", "amount": 9.9},
            )
            assert r.status_code == 200
    # 补齐后再送清洗 → 过关 → 落库
    re = authed.post(
        f"/api/pipelines/{pid}/clean",
        json={"policy": "quarantine", "mode": "reclean"},
    )
    assert re.status_code == 200
    assert re.json()["status"] == "已清洗待加载"
    ld = authed.post(f"/api/pipelines/{pid}/load")
    assert ld.status_code == 200
    assert ld.json()["status"] == "已落库"


def test_empty_title_counts_as_dirty(authed):
    """标题字段为空算脏数据，补齐时空标题照样拦。"""
    _set_policy(authed, "quarantine")
    pid = _first_pid(authed)
    authed.post(f"/api/pipelines/{pid}/extract")
    authed.post(f"/api/pipelines/{pid}/clean", json={"policy": "quarantine"})
    detail = authed.get(f"/api/pipelines/{pid}").json()
    reasons = [r["reason"] for r in detail["quarantine"]]
    assert any("标题字段为空" in reason for reason in reasons)
    row = detail["quarantine"][0]
    r = authed.post(
        f"/api/pipelines/{pid}/rows/{row['id']}/fix",
        json={"title": "   ", "amount": 1},
    )
    assert r.status_code == 400
    assert "标题" in r.json()["error"]


def test_clean_contention_single_winner(authed, client):
    """两人同时争抢清洗同一批只留一笔，另一侧记清洗争用失败。"""
    _set_policy(authed, "drop")
    pid = _first_pid(authed)
    authed.post(f"/api/pipelines/{pid}/extract")

    barrier = threading.Barrier(2)

    def clean_with(c):
        barrier.wait(timeout=10)
        return c.post(f"/api/pipelines/{pid}/clean", json={"policy": "drop"})

    with TestClient(app) as other:
        _login(other)
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(clean_with, client)
            f2 = pool.submit(clean_with, other)
            results = [f1.result(), f2.result()]
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409]
    loser = next(r for r in results if r.status_code == 409)
    assert loser.json()["error"] == "清洗争用失败"
    # 只留一笔：清洗通过只有一条，争用失败记在详情里
    detail = client.get(f"/api/pipelines/{pid}").json()
    kinds = [e["kind"] for e in detail["events"]]
    assert kinds.count("清洗通过") == 1
    assert "清洗争用失败" in kinds
    # 再抢同一批，仍是争用失败
    again = client.post(f"/api/pipelines/{pid}/clean", json={"policy": "drop"})
    assert again.status_code == 409
    assert again.json()["error"] == "清洗争用失败"


def test_clean_contention_under_quarantine_policy(authed, client):
    """隔离口径下同批争抢同样只留一笔，输家记清洗争用失败。"""
    _set_policy(authed, "quarantine")
    pid = _first_pid(authed)
    authed.post(f"/api/pipelines/{pid}/extract")

    barrier = threading.Barrier(2)

    def clean_with(c):
        barrier.wait(timeout=10)
        return c.post(f"/api/pipelines/{pid}/clean", json={"policy": "quarantine"})

    with TestClient(app) as other:
        _login(other)
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(clean_with, client)
            f2 = pool.submit(clean_with, other)
            results = [f1.result(), f2.result()]
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409]
    loser = next(r for r in results if r.status_code == 409)
    assert loser.json()["error"] == "清洗争用失败"
    detail = client.get(f"/api/pipelines/{pid}").json()
    kinds = [e["kind"] for e in detail["events"]]
    assert kinds.count("清洗通过") == 1
    assert "清洗争用失败" in kinds


def test_page_distinguishes_quarantine_and_discard(authed):
    """页面能分清隔离与丢弃，总览带待清洗/已丢弃/隔离中/口径名称。"""
    r = authed.get("/app/")
    assert r.status_code == 200
    text = r.text
    for kw in [
        "待清洗",
        "已丢弃",
        "隔离中",
        "口径名称",
        "隔离原因",
        "清洗争用失败",
        "清洗工作台",
        "规则设置",
        "脏数据详情",
    ]:
        assert kw in text
