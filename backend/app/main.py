"""FastAPI application: HTTP API + WebSocket live channel + static frontend.

Run (container)::

    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import hmac
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .db import Database
from .definitions import DrillError, load_file
from .engine import Engine, EngineError, now_ts
from .hub import Client, Hub
from .joints import JointError, JointOrchestrator, stage_outcomes
from .records import (
    build_replay,
    host_record,
    load_events,
    personal_record,
    render_host_md,
    render_personal_md,
)
from .snapshots import build_snapshot


class TimerService:
    """One asyncio task per open stage deadline. Server clock is authoritative;
    each task captures the deadline it was armed with and hands it to
    settle('deadline', expected_deadline=...), which re-verifies the deadline
    identity inside the atomic transaction, so a stale callback from an earlier
    timed stage can never settle the stage that replaced it."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}

    def schedule(self, session_id: str, deadline: float | None) -> None:
        self.cancel(session_id)
        if deadline is None:
            return
        delay = max(0.05, deadline - time.time())
        self._tasks[session_id] = asyncio.create_task(
            self._fire(session_id, deadline, delay)
        )

    def cancel(self, session_id: str) -> None:
        t = self._tasks.pop(session_id, None)
        if t and not t.done():
            t.cancel()

    async def _fire(self, session_id: str, deadline: float, delay: float) -> None:
        await asyncio.sleep(delay)
        # Unregister only while still the active timer: an advancing stage may
        # have armed a successor while this callback was waiting on the session
        # lock / event loop. Removing the successor here would orphan it.
        if self._tasks.get(session_id) is asyncio.current_task():
            self._tasks.pop(session_id, None)
        await on_deadline(session_id, deadline)


# set during lifespan
db = Database(str(config.DB_PATH))
engine = Engine(db)
joints = JointOrchestrator(db, engine)
hub = Hub()
timers = TimerService()


def check_host(key: str | None) -> None:
    if not key or not hmac.compare_digest(key, config.HOST_KEY):
        raise HTTPException(status_code=401, detail="主持人密钥无效")


async def send_snapshot(client: Client, session_id: str, viewer: tuple[str, str] | None) -> None:
    sess, d, state = await engine.load(session_id)
    snap = build_snapshot(sess, d, state, viewer, online=hub.online_roles(session_id))
    snap["kind"] = viewer[0] if viewer else "none"
    snap["server_time"] = now_ts()
    hub.push_to(client, {"type": "snapshot", "snapshot": snap})


async def broadcast_snapshot(session_id: str) -> None:
    sess, d, state = await engine.load(session_id)
    online = hub.online_roles(session_id)
    ts = now_ts()
    for c in list(hub._sessions.get(session_id, ())):  # type: ignore[attr-defined]
        viewer = ("host", None) if c.role_id is None else ("role", c.role_id)
        snap = build_snapshot(sess, d, state, viewer, online=online)
        snap["server_time"] = ts
        hub.push_to(c, {"type": "snapshot", "snapshot": snap})


async def reschedule(session_id: str) -> None:
    try:
        _, _, state = await engine.load(session_id)
    except EngineError:
        timers.cancel(session_id)
        return
    if state.status == "running" and state.current is not None:
        timers.schedule(session_id, state.deadline)
    else:
        timers.cancel(session_id)


async def broadcast_joint(joint_id: str) -> None:
    """Push the live branch-wait snapshot to every monitor of this joint."""
    try:
        status = await joints.joint_status(joint_id)
    except JointError:
        return
    hub.push(f"joint:{joint_id}", {"type": "joint_snapshot", "joint": status})


async def sync_session_joints(session_id: str) -> None:
    """After a session commit that could have sealed a stage, reconcile every
    joint it belongs to, then broadcast:
      * the monitor snapshot of changed joints (branch waits / block reasons);
      * a fresh session snapshot to each member whose own state just changed
        (a fired injection lands in the TARGET session's log)."""
    affected = await joints.on_session_progress(session_id)
    touched_sessions: set[str] = set()
    for jid in affected:
        await broadcast_joint(jid)
        try:
            doc = await joints.get_joint(jid)
        except JointError:
            continue
        touched_sessions.update(doc["session_ids"])
    # The source session's own snapshot is unaffected by injections, but the
    # target session (possibly this one) must refresh; cheap to re-send.
    for sid in touched_sessions:
        await broadcast_snapshot(sid)


async def on_deadline(session_id: str, expected_deadline: float) -> None:
    try:
        info = await engine.settle(
            session_id, "deadline", expected_deadline=expected_deadline
        )
    except EngineError:
        return
    if not info.get("noop"):
        await broadcast_snapshot(session_id)
        await reschedule(session_id)
        await sync_session_joints(session_id)


def parse_yaml_body(yaml_text: str):
    """Parse + validate uploaded YAML. Raises DrillError on any problem."""
    import yaml as pyyaml

    from .definitions import parse_definition

    raw = pyyaml.safe_load(yaml_text)
    return parse_definition(raw or {})


def persist_yaml_file(drill_id: str, yaml_text: str) -> bool:
    """Best-effort mirror of uploaded YAML into DRILLS_DIR. The database is
    the source of truth for versions; in Docker the drills directory is a
    read-only bind mount, so a write failure here must never break the API."""
    try:
        path = config.DRILLS_DIR / f"{drill_id}.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# lifespan: open DB, seed drills from YAML, re-arm timers after a restart
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.DRILLS_DIR.mkdir(parents=True, exist_ok=True)
    await db.connect()

    # Reconcile shipped YAML files with the immutable version store:
    # new drill -> released v1; changed file -> a new draft for the host to
    # review; unchanged file -> nothing.
    if config.DRILLS_DIR.exists():
        for path in sorted(config.DRILLS_DIR.glob("*.y*ml")):
            try:
                d = load_file(path)
                await engine.seed_from_file(d, path.read_text(encoding="utf-8"))
            except DrillError as exc:
                # A broken seed file must not prevent the server from starting.
                print(f"[seed] skip {path.name}: {exc}")

    for s in await engine.list_sessions():
        if s["status"] == "running":
            try:
                _, _, state = await engine.load(s["id"])
            except Exception:
                continue
            if state.current is not None:
                timers.schedule(s["id"], state.deadline)

    # Re-converge joint barriers after a restart: branches that sealed while
    # we were down get their idempotent hits, and fully-satisfied but
    # undelivered rules fire their one-shot injections now.
    for j in await joints.list_joints():
        try:
            await joints.reconcile_joint(j["id"])
        except Exception as exc:  # a broken joint must not block startup
            print(f"[joint] reconcile {j['id']} skipped: {exc}")
    try:
        yield
    finally:
        for sid in list(timers._tasks):
            timers.cancel(sid)
        await db.close()


app = FastAPI(title="多人应急演练", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# HTTP API
# --------------------------------------------------------------------------- #
class JoinBody(BaseModel):
    invite: str
    name: str | None = None


class DrillBody(BaseModel):
    yaml: str
    base_version: int | None = None


class PublishBody(BaseModel):
    base_version: int | None = None


class CopyBody(BaseModel):
    source_version: int
    base_version: int | None = None


class JointBranchBody(BaseModel):
    source_session_id: str
    source_stage_id: str
    outcome: str  # stage id, or "@end" (sealing the stage ends its drill)


class JointRuleBody(BaseModel):
    id: str | None = None
    target_session_id: str
    target_stage_id: str
    title: str
    public: str = ""
    views: dict[str, str] = {}
    branches: list[JointBranchBody]


class JointBody(BaseModel):
    name: str
    session_ids: list[str]
    rules: list[JointRuleBody]


@app.get("/api/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/api/drills")
async def api_drills(x_host_key: str | None = Header(default=None)) -> list[dict[str, Any]]:
    check_host(x_host_key)
    return await engine.list_drills()


@app.post("/api/drills")
async def api_create_drill(
    body: DrillBody, x_host_key: str | None = Header(default=None)
) -> dict[str, Any]:
    """Upload or edit YAML. Always creates (or replaces) an immutable *draft*
    version; drafts cannot create sessions until published. Re-saving the same
    content is idempotent and reuses the existing version/digest."""
    check_host(x_host_key)
    try:
        d = parse_yaml_body(body.yaml)
    except DrillError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        result = await engine.save_draft(d, body.yaml, body.base_version)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    # Best-effort YAML mirror (read-only mount in Docker is fine).
    result["yaml_written"] = persist_yaml_file(d.id, body.yaml)
    return result


@app.get("/api/drills/{drill_id}/versions")
async def api_list_versions(
    drill_id: str, x_host_key: str | None = Header(default=None)
) -> list[dict[str, Any]]:
    check_host(x_host_key)
    try:
        return await engine.list_versions(drill_id)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.get("/api/drills/{drill_id}/versions/{version}")
async def api_get_version(
    drill_id: str,
    version: int,
    x_host_key: str | None = Header(default=None),
) -> dict[str, Any]:
    check_host(x_host_key)
    try:
        return await engine.get_version(drill_id, version)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.post("/api/drills/{drill_id}/versions/{version}/publish")
async def api_publish(
    drill_id: str,
    version: int,
    body: PublishBody,
    x_host_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Release a draft. On a base-version conflict the transaction rolls back:
    the current release is unchanged and a 409 tells the host to re-diff."""
    check_host(x_host_key)
    try:
        meta = await engine.publish_drill(drill_id, version, body.base_version)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    try:
        row = await engine.get_version(drill_id, version)
        persist_yaml_file(drill_id, row["yaml"])
    except EngineError:
        pass
    return meta


@app.post("/api/drills/{drill_id}/copy-draft")
async def api_copy_draft(
    drill_id: str,
    body: CopyBody,
    x_host_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Copy any historical version's content into a fresh editable draft."""
    check_host(x_host_key)
    try:
        return await engine.copy_as_draft(
            drill_id, body.source_version, body.base_version
        )
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.get("/api/drills/{drill_id}/diff")
async def api_diff(
    drill_id: str,
    from_version: int = Query(..., alias="from"),
    to_version: int = Query(..., alias="to"),
    x_host_key: str | None = Header(default=None),
) -> dict[str, Any]:
    check_host(x_host_key)
    try:
        return await engine.diff_versions(drill_id, from_version, to_version)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.post("/api/sessions")
async def api_create_session(
    drill_id: str = Query(...), x_host_key: str | None = Header(default=None)
) -> JSONResponse:
    check_host(x_host_key)
    try:
        sid = await engine.create_session(drill_id)
        sess = await engine.get_session(sid)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    return JSONResponse(
        {
            "session_id": sid,
            "drill_version": sess["drill_version"],
            "drill_sha256": sess["drill_sha256"],
            "digest_short": sess["drill_sha256"][:12],
        },
        status_code=201,
    )


@app.get("/api/sessions")
async def api_sessions(x_host_key: str | None = Header(default=None)) -> list[dict[str, Any]]:
    check_host(x_host_key)
    out = []
    for s in await engine.list_sessions():
        try:
            d = await engine.get_version_def(s["drill_id"], s["drill_version"])
            s["drill_name"] = d.name
        except EngineError:
            s["drill_name"] = s["drill_id"]
        s["digest_short"] = (s.get("drill_sha256") or "")[:12]
        out.append(s)
    return out


@app.post("/api/sessions/{session_id}/join")
async def api_join(session_id: str, body: JoinBody) -> dict[str, Any]:
    try:
        await engine.get_session(session_id)
        return await engine.join(session_id, body.invite, body.name or "")
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


async def _session_defs(session_ids: list[str]) -> tuple[dict[str, Any], dict[str, int]]:
    """Load each enrolled session's PINNED immutable script definition.

    Sessions in a joint keep independent script versions on purpose, so
    validation is done per session against its own pinned version — never
    against a "latest release".
    """
    defs: dict[str, Any] = {}
    versions: dict[str, int] = {}
    for sid in dict.fromkeys(session_ids):
        sess = await engine.get_session(sid)  # raises 404 when missing
        d = await engine.get_version_def(sess["drill_id"], sess["drill_version"])
        defs[sid] = d
        versions[sid] = sess["drill_version"]
    return defs, versions


@app.get("/api/joints")
async def api_joints(x_host_key: str | None = Header(default=None)) -> list[dict[str, Any]]:
    check_host(x_host_key)
    return await joints.list_joints()


@app.post("/api/joints", status_code=201)
async def api_create_joint(
    body: JointBody, x_host_key: str | None = Header(default=None)
) -> dict[str, Any]:
    check_host(x_host_key)
    try:
        defs, versions = await _session_defs(body.session_ids)
        raw_rules = [r.model_dump() for r in body.rules]
        doc = await joints.create_joint(
            body.name, body.session_ids, raw_rules, defs, versions
        )
    except JointError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    return await joints.joint_status(doc["id"])


@app.get("/api/joints/{joint_id}")
async def api_get_joint(
    joint_id: str, x_host_key: str | None = Header(default=None)
) -> dict[str, Any]:
    check_host(x_host_key)
    try:
        return await joints.joint_status(joint_id)
    except JointError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.put("/api/joints/{joint_id}")
async def api_update_joint(
    joint_id: str,
    body: JointBody,
    x_host_key: str | None = Header(default=None),
) -> dict[str, Any]:
    check_host(x_host_key)
    try:
        defs, versions = await _session_defs(body.session_ids)
        raw_rules = [r.model_dump() for r in body.rules]
        await joints.update_joint(
            joint_id, body.name, body.session_ids, raw_rules, defs, versions
        )
        status = await joints.joint_status(joint_id)
    except JointError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    await broadcast_joint(joint_id)
    return status


@app.delete("/api/joints/{joint_id}")
async def api_delete_joint(
    joint_id: str, x_host_key: str | None = Header(default=None)
) -> dict[str, bool]:
    check_host(x_host_key)
    try:
        await joints.delete_joint(joint_id)
    except JointError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    return {"ok": True}


@app.get("/api/sessions/{session_id}/outline")
async def api_session_outline(
    session_id: str, x_host_key: str | None = Header(default=None)
) -> dict[str, Any]:
    """Stage/role/outline of a session's PINNED script version, used by the
    joint editor to pick stages and validate outcomes."""
    check_host(x_host_key)
    try:
        sess, d, _ = await engine.load(session_id)
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    return {
        "session_id": session_id,
        "status": sess["status"],
        "drill": {
            "id": d.id,
            "name": d.name,
            "version": sess["drill_version"],
            "sha256": sess["drill_sha256"],
        },
        "roles": [{"id": r.id, "name": r.name} for r in d.roles.values()],
        "stages": [
            {
                "id": stg.id,
                "title": stg.title,
                "outcomes": sorted(stage_outcomes(stg)),
            }
            for stg in d.stages.values()
        ],
    }


async def _viewer_for(
    session_id: str, host_key: str | None, token: str | None
) -> tuple[str, str]:
    """Returns ("host", "") or ("role", role_id)."""
    if host_key:
        if hmac.compare_digest(host_key, config.HOST_KEY):
            return ("host", "")
        raise HTTPException(status_code=401, detail="主持人密钥无效")
    if token:
        p = await engine.get_participant(session_id, token)
        if p is None:
            raise HTTPException(status_code=401, detail="访问令牌无效")
        return ("role", p["role_id"])
    raise HTTPException(status_code=401, detail="需要主持人密钥或参与者令牌")


@app.get("/api/sessions/{session_id}/replay")
async def api_replay(
    session_id: str,
    x_host_key: str | None = Header(default=None),
    token: str | None = Query(default=None),
    version: int | None = Query(default=None),
) -> dict[str, Any]:
    viewer = await _viewer_for(session_id, x_host_key, token)
    try:
        sess, d, state = await engine.load(session_id)
        rows = await db.all(
            "SELECT version, seq, type, actor, at, payload FROM events "
            "WHERE session_id=? ORDER BY version, seq",
            (session_id,),
        )
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    events = load_events(rows)
    if version is not None:
        events = [e for e in events if e["version"] <= version]
    return build_replay(sess, d, events, viewer if viewer[0] == "host" else viewer)


@app.get("/api/sessions/{session_id}/record")
async def api_record(
    session_id: str,
    fmt: str = Query("md"),
    x_host_key: str | None = Header(default=None),
    token: str | None = Query(default=None),
) -> Response:
    viewer = await _viewer_for(session_id, x_host_key, token)
    try:
        sess, d, _ = await engine.load(session_id)
        rows = await db.all(
            "SELECT version, seq, type, actor, at, payload FROM events "
            "WHERE session_id=? ORDER BY version, seq",
            (session_id,),
        )
    except EngineError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc
    events = load_events(rows)

    if viewer[0] == "host":
        rec = host_record(sess, d, events)
        if fmt == "json":
            return JSONResponse(rec)
        return Response(
            render_host_md(rec),
            media_type="text/markdown; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="host-{session_id}.md"'},
        )

    rec = personal_record(sess, d, events, viewer[1])
    if fmt == "json":
        return JSONResponse(rec)
    return Response(
        render_personal_md(rec),
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="record-{session_id}-{viewer[1]}.md"'
        },
    )


# --------------------------------------------------------------------------- #
# WebSocket: /ws/{session_id}?token=...&host=...
# --------------------------------------------------------------------------- #
@app.websocket("/ws/{session_id}")
async def ws_endpoint(ws: WebSocket, session_id: str) -> None:
    token = ws.query_params.get("token")
    host_key = ws.query_params.get("host")

    # authenticate before accept is not possible with query params in all
    # clients, so accept then close with an error message if invalid.
    await ws.accept()
    try:
        viewer = await _viewer_for(session_id, host_key, token)
    except HTTPException as exc:
        await ws.send_json({"type": "error", "code": "auth", "detail": exc.detail})
        await ws.close(code=4401)
        return

    try:
        await engine.get_session(session_id)
    except EngineError:
        await ws.send_json({"type": "error", "code": "no_session", "detail": "会话不存在"})
        await ws.close(code=4404)
        return

    client = Client(ws=ws, role_id=None if viewer[0] == "host" else viewer[1])
    hub.add(session_id, client)

    sender = asyncio.create_task(_ws_sender(session_id, client, viewer))
    await send_snapshot(client, session_id, viewer)
    await broadcast_snapshot(session_id)  # refresh online flags for everyone

    try:
        while True:
            raw = await ws.receive_json()
            await _handle_ws_message(session_id, client, viewer, raw)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        sender.cancel()
        hub.remove(session_id, client)
        await broadcast_snapshot(session_id)


async def _ws_sender(session_id: str, client: Client, viewer: tuple[str, str]) -> None:
    while True:
        msg = await client.queue.get()
        if msg is None:
            return
        if msg.get("type") == "resync":
            await send_snapshot(client, session_id, viewer)
            continue
        try:
            await client.ws.send_json(msg)
        except Exception:
            return


async def _handle_ws_message(
    session_id: str, client: Client, viewer: tuple[str, str], raw: dict[str, Any]
) -> None:
    mtype = raw.get("type")
    try:
        if mtype == "ping":
            hub.push_to(client, {"type": "pong", "t": raw.get("t"), "server_time": now_ts()})
            return

        if viewer[0] == "host":
            await _handle_host_message(session_id, raw)
            return

        if mtype == "submit":
            cid = str(raw.get("choice_id") or "")
            stage_id = raw.get("stage_id")
            client_stage = raw.get("client_stage")
            try:
                result = await engine.submit(
                    session_id, viewer[1], stage_id, cid, client_stage=client_stage
                )
            except EngineError as exc:
                hub.push_to(
                    client,
                    {"type": "error", "code": "submit_rejected", "detail": str(exc),
                     "status": exc.status},
                )
                await send_snapshot(client, session_id, viewer)
                return
            hub.push_to(client, {"type": "submit_result", **result})
            await broadcast_snapshot(session_id)
            await reschedule(session_id)
            if result.get("accepted") or result.get("settled"):
                await sync_session_joints(session_id)
            return

        hub.push_to(client, {"type": "error", "code": "unknown_type", "detail": mtype})
    except EngineError as exc:
        hub.push_to(client, {"type": "error", "code": "bad_action", "detail": str(exc)})


async def _handle_host_message(session_id: str, raw: dict[str, Any]) -> None:
    mtype = raw.get("type")
    changed = False
    if mtype == "start":
        await engine.start(session_id)
        changed = True
    elif mtype == "settle":
        info = await engine.settle(session_id, "host")
        changed = not info.get("noop")
    elif mtype == "end":
        await engine.end_session(session_id)
        changed = True
    else:
        return
    await broadcast_snapshot(session_id)
    await reschedule(session_id)
    if changed:
        await sync_session_joints(session_id)


# --------------------------------------------------------------------------- #
# WebSocket: /ws/joint/{joint_id}?host=... — host-only joint monitor
# --------------------------------------------------------------------------- #
@app.websocket("/ws/joint/{joint_id}")
async def ws_joint(ws: WebSocket, joint_id: str) -> None:
    host_key = ws.query_params.get("host")
    await ws.accept()
    if not host_key or not hmac.compare_digest(host_key, config.HOST_KEY):
        await ws.send_json({"type": "error", "code": "auth", "detail": "主持人密钥无效"})
        await ws.close(code=4401)
        return
    try:
        await joints.get_joint(joint_id)
    except JointError:
        await ws.send_json({"type": "error", "code": "no_joint", "detail": "联演不存在"})
        await ws.close(code=4404)
        return

    channel = f"joint:{joint_id}"
    client = Client(ws=ws, role_id=None)
    hub.add(channel, client)
    sender = asyncio.create_task(_joint_ws_sender(joint_id, client))
    try:
        status = await joints.joint_status(joint_id)
        hub.push_to(client, {"type": "joint_snapshot", "joint": status})
        while True:
            raw = await ws.receive_json()
            # Monitors are read-only; ping keeps the clock/connection fresh.
            if raw.get("type") == "ping":
                hub.push_to(client, {"type": "pong", "t": raw.get("t"),
                                     "server_time": now_ts()})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        sender.cancel()
        hub.remove(channel, client)


async def _joint_ws_sender(joint_id: str, client: Client) -> None:
    while True:
        msg = await client.queue.get()
        if msg is None:
            return
        if msg.get("type") == "resync":
            try:
                status = await joints.joint_status(joint_id)
                msg = {"type": "joint_snapshot", "joint": status}
            except JointError:
                return
        try:
            await client.ws.send_json(msg)
        except Exception:
            return


# --------------------------------------------------------------------------- #
# static frontend (mounted last so /api and /ws take precedence)
# --------------------------------------------------------------------------- #
if config.STATIC_DIR.exists():
    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(config.STATIC_DIR / "index.html")

    app.mount("/", StaticFiles(directory=str(config.STATIC_DIR), html=True), name="static")
