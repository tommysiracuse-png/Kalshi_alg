from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Annotated, AsyncIterator, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from ui_api.config import Settings
from ui_api.store import OperationsStore, now_ms
from session_store import SessionConflictError


settings = Settings.from_environment()
store = OperationsStore(settings)
app = FastAPI(title="Kalshi Operations API", version="1.0.0", docs_url=None, redoc_url=None)


async def authorize(x_internal_token: Annotated[Optional[str], Header()] = None) -> str:
    expected = os.getenv("KALSHI_UI_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=401, detail="invalid internal token")
    return "operator"


@app.exception_handler(Exception)
async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    status = 404 if isinstance(exc, KeyError) else 409 if isinstance(exc, SessionConflictError) else 400 if isinstance(exc, ValueError) else 500
    return JSONResponse(status_code=status, content={
        "code": "not_found" if status == 404 else "conflict" if status == 409 else "invalid_request" if status == 400 else "internal_error",
        "message": str(exc), "requestId": request_id, "details": None,
    })


@app.exception_handler(HTTPException)
async def http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    return JSONResponse(status_code=exc.status_code, content={"code": "unauthorized" if exc.status_code == 401 else "request_failed", "message": str(exc.detail), "requestId": request_id, "details": None})


@app.exception_handler(RequestValidationError)
async def validation_exception(request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    return JSONResponse(status_code=422, content={"code": "validation_error", "message": "request validation failed", "requestId": request_id, "details": exc.errors()})


@app.get("/api/v1/health")
async def health(_: str = Depends(authorize)) -> dict:
    state = store.status()
    return {"generatedAt": now_ms(), "status": "degraded" if state["source"].get("stale") else "ok", "sources": {"launcher": state["source"]}}


@app.get("/api/v1/overview")
async def overview(_: str = Depends(authorize)) -> dict:
    return store.overview()


@app.get("/api/v1/markets")
async def markets(search: str = "", watchdog_mode: str = "", disabled: Optional[bool] = None, sort: str = "rank", _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "items": store.markets(search=search, watchdog_mode=watchdog_mode, disabled=disabled, sort=sort)}


@app.get("/api/v1/markets/{ticker}")
async def market(ticker: str, _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), **store.market_detail(ticker)}


@app.get("/api/v1/pnl")
async def pnl(window: Literal["1h", "24h", "7d", "all"] = "all", _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), **store.pnl(window)}


@app.get("/api/v1/monitoring")
async def monitoring(_: str = Depends(authorize)) -> dict:
    return store.monitoring()


@app.get("/api/v1/monitoring/clients/{market_id}")
async def client_monitoring(market_id: str, _: str = Depends(authorize)) -> dict:
    return store.client_monitoring(market_id)


@app.get("/api/v1/portfolio")
async def portfolio(_: str = Depends(authorize)) -> dict:
    return store.portfolio()


@app.get("/api/v1/audit")
async def audit(limit: int = Query(100, ge=1, le=500), _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "items": store.audit.list(limit)}


@app.get("/api/v1/system")
async def system(_: str = Depends(authorize)) -> dict:
    status = store.status()
    return {"generatedAt": now_ms(), "workspace": str(settings.workspace), "service": settings.service_name, "sources": {"launcher": status["source"]}}


async def _json_body(request: Request) -> dict:
    try:
        body = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail="request body must be valid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be an object")
    return body


def _audit_session(action: str, target: str, operator: str, request_id: Optional[str]) -> str:
    resolved = _request_id(request_id)
    store.record_session_audit(resolved, action, target, operator)
    return resolved


@app.get("/api/v1/sessions")
async def sessions(include_archived: bool = False, _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "items": store.sessions.list_sessions(include_archived=include_archived), "activeRun": store.sessions.active_run()}


@app.post("/api/v1/sessions", status_code=201)
async def create_session(request: Request, operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    try:
        item = store.sessions.create_session(await _json_body(request))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    request_id = _audit_session("session_create", item["id"], operator, x_request_id)
    return {"generatedAt": now_ms(), "requestId": request_id, "item": item}


@app.get("/api/v1/sessions/{session_id}")
async def session_detail(session_id: str, _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "item": store.sessions.get_session(session_id)}


@app.put("/api/v1/sessions/{session_id}")
async def update_session(session_id: str, request: Request, operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    try:
        item = store.sessions.update_session(session_id, await _json_body(request))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    request_id = _audit_session("session_update", session_id, operator, x_request_id)
    return {"generatedAt": now_ms(), "requestId": request_id, "item": item}


@app.delete("/api/v1/sessions/{session_id}")
async def archive_session(session_id: str, operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    item = store.sessions.archive_session(session_id)
    request_id = _audit_session("session_archive", session_id, operator, x_request_id)
    return {"generatedAt": now_ms(), "requestId": request_id, "item": item}


@app.post("/api/v1/sessions/{session_id}/select")
async def select_session(session_id: str, operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    item = store.sessions.select_session(session_id)
    request_id = _audit_session("session_select", session_id, operator, x_request_id)
    return {"generatedAt": now_ms(), "requestId": request_id, "item": item}


@app.post("/api/v1/sessions/{session_id}/restore")
async def restore_session(session_id: str, operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    item = store.sessions.restore_session(session_id)
    request_id = _audit_session("session_restore", session_id, operator, x_request_id)
    return {"generatedAt": now_ms(), "requestId": request_id, "item": item}


@app.get("/api/v1/runs")
async def runs(session_id: str = "", status: str = "", from_ms: Optional[int] = None, to_ms: Optional[int] = None, _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "items": store.sessions.list_runs(session_id=session_id, status=status, from_ms=from_ms, to_ms=to_ms)}


@app.get("/api/v1/runs/{run_id}")
async def run_detail(run_id: str, _: str = Depends(authorize)) -> dict:
    return {"generatedAt": now_ms(), "item": store.sessions.get_run(run_id)}


@app.get("/api/v1/metrics")
async def historical_metrics(session_id: str = "", status: str = "", from_ms: Optional[int] = None, to_ms: Optional[int] = None, _: str = Depends(authorize)) -> dict:
    return store.sessions.metrics(session_id=session_id, status=status, from_ms=from_ms, to_ms=to_ms)


async def event_stream() -> AsyncIterator[str]:
    event_id = 0
    while True:
        event_id += 1
        payload = store.overview()
        yield f"id: {event_id}\nevent: overview\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
        monitoring_payload = store.monitoring()
        yield f"id: {event_id}\nevent: monitoring\ndata: {json.dumps(monitoring_payload, separators=(',', ':'))}\n\n"
        portfolio_payload = store.portfolio()
        yield f"id: {event_id}\nevent: portfolio\ndata: {json.dumps(portfolio_payload, separators=(',', ':'))}\n\n"
        await asyncio.sleep(2)


@app.get("/api/v1/events")
async def events(_: str = Depends(authorize)) -> StreamingResponse:
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/v1/logs/{ticker}/stream")
async def logs(ticker: str, source: Literal["bot", "watchdog"] = "bot", _: str = Depends(authorize)) -> StreamingResponse:
    path = store.log_path(ticker, source)

    async def stream() -> AsyncIterator[str]:
        position = 0
        initialized = False
        while True:
            try:
                size = path.stat().st_size
                if size < position:
                    position = 0
                with path.open(encoding="utf-8", errors="replace") as handle:
                    if not initialized:
                        handle.seek(max(0, size - 65_536))
                        if handle.tell() > 0:
                            handle.readline()
                        initialized = True
                    else:
                        handle.seek(position)
                    for line in handle:
                        yield f"event: log\ndata: {json.dumps(line.rstrip())}\n\n"
                    position = handle.tell()
            except OSError:
                yield ": waiting for log file\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _request_id(value: Optional[str]) -> str:
    return value or str(uuid.uuid4())


@app.post("/api/v1/controls/fleet/{action}")
async def fleet_control(action: Literal["start", "stop", "refresh"], operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    return await asyncio.to_thread(store.control, action, ticker=None, operator=operator, request_id=_request_id(x_request_id))


@app.post("/api/v1/controls/markets/{ticker}/{action}")
async def market_control(ticker: str, action: Literal["disable", "enable"], operator: str = Depends(authorize), x_request_id: Annotated[Optional[str], Header()] = None) -> dict:
    return await asyncio.to_thread(store.control, action, ticker=ticker, operator=operator, request_id=_request_id(x_request_id))
