import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer

from app.config import Config
from app.db import (
    fix_row,
    get_db,
    init_db,
    list_pipelines,
    pipeline_detail,
    policy_view,
    run_clean,
    run_extract,
    run_load,
    set_policy,
)

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def _gate_response(item, err):
    """闸口动作统一回包：成功给管道，被拦给错误与最新状态。"""
    if item is None:
        raise HTTPException(status_code=404, detail="管道不存在")
    if err:
        code, msg = err
        return JSONResponse(
            {"ok": False, "error": msg, "item": item}, status_code=code
        )
    return item


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        return {"items": list_pipelines(conn)}


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = pipeline_detail(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.get("/api/policy")
def api_get_policy(user: str = Depends(require_user)):
    with get_db() as conn:
        return policy_view(conn)


@app.post("/api/policy")
async def api_set_policy(request: Request, user: str = Depends(require_user)):
    body = await request.json()
    policy = str(body.get("policy", "")).strip()
    note = str(body.get("note", ""))
    with get_db() as conn:
        doc, err = set_policy(conn, policy, note, user)
        view = policy_view(conn)
    if err:
        code, msg = err
        return JSONResponse({"ok": False, "error": msg, **view}, status_code=code)
    return {"ok": True, **view}


@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item, err = run_extract(conn, pipeline_id, user)
    return _gate_response(item, err)


@app.post("/api/pipelines/{pipeline_id}/clean")
async def api_clean(pipeline_id: int, request: Request, user: str = Depends(require_user)):
    body = await request.json()
    policy = str(body.get("policy", "")).strip()
    mode = "reclean" if str(body.get("mode", "")) == "reclean" else "clean"
    with get_db() as conn:
        item, err = run_clean(conn, pipeline_id, policy, user, mode)
    return _gate_response(item, err)


@app.post("/api/pipelines/{pipeline_id}/rows/{row_id}/fix")
async def api_fix_row(
    pipeline_id: int, row_id: int, request: Request, user: str = Depends(require_user)
):
    body = await request.json()
    title = str(body.get("title", ""))
    raw_amount = body.get("amount", None)
    amount = None
    if raw_amount not in (None, ""):
        try:
            amount = float(raw_amount)
        except (TypeError, ValueError):
            return JSONResponse(
                {"ok": False, "error": "金额格式不对"}, status_code=400
            )
    with get_db() as conn:
        item, err = fix_row(conn, pipeline_id, row_id, title, amount, user)
    return _gate_response(item, err)


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item, err = run_load(conn, pipeline_id, user)
    return _gate_response(item, err)


def create_app() -> FastAPI:
    return app
