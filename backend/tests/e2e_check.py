"""Regression tests for the drill platform.

Covers the unified event-log allocation rules (join / start / submit /
deadline & manual settle / end): one atomic commit == one new version ==
last_version + 1, and events inside a commit are numbered seq 0..k in a
fixed causal order, so (session_id, version, seq) never collides. Also
covers deadline races, the stale-timer identity check across two
consecutive timed stages, concurrent idempotency, deterministic replay of
every version prefix, and an end-to-end HTTP + WebSocket pass through the
real FastAPI app.

Run from the repo root:   python backend/tests/e2e_check.py
Run from backend/:        python tests/e2e_check.py
Exit code is non-zero when any check fails.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

_TMP = tempfile.mkdtemp(prefix="drill-test-")
os.environ["DATA_DIR"] = _TMP
os.environ["DRILLS_DIR"] = str(Path(_TMP) / "drills")
os.environ["STATIC_DIR"] = str(Path(__file__).resolve().parents[2] / "frontend")
os.environ["HOST_KEY"] = "test-key"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from app.db import Database  # noqa: E402
from app.definitions import DrillError, load_file, parse_definition  # noqa: E402
from app.engine import Engine, EngineError, now_ts, reduce_events  # noqa: E402
from app.joints import JointError, stage_outcomes  # noqa: E402
from app.main import app  # noqa: E402
from app.records import build_replay, load_events  # noqa: E402
from app.versions import content_digest  # noqa: E402

HOST = {"X-Host-Key": "test-key"}
PASS, FAIL = 0, 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {extra}")


# --------------------------------------------------------------------------- #
# drill definitions used by the tests
# --------------------------------------------------------------------------- #
DUEL = {
    "id": "duel",
    "name": "双角色演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
        {"id": "c", "name": "丙", "invite": "CCC"},
    ],
    "stages": [
        {
            "id": "s1", "title": "阶段一", "public": "公开一", "duration": 30,
            "default_next": "s2",
            "views": {"a": "甲的隐秘s1", "b": "乙的隐秘s1"},
            "options": {
                "a": [{"id": "x", "text": "甲选X"}, {"id": "y", "text": "甲选Y"}],
                "b": [{"id": "p", "text": "乙选P"}, {"id": "q", "text": "乙选Q"}],
            },
            "transitions": [{"when": {"a": "x", "b": "p"}, "next": "s2"}],
        },
        {
            "id": "s2", "title": "阶段二", "public": "公开二", "duration": None,
            "default_next": None,
            "views": {"a": "甲的隐秘s2"},
            "options": {"a": [{"id": "k", "text": "甲终选"}]},
        },
    ],
}

FAST = {
    "id": "fast",
    "name": "限时演练",
    "roles": [{"id": "a", "name": "甲", "invite": "AAA"}],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "p1", "duration": 1,
            "default_next": "s2",
            "options": {"a": [{"id": "x", "text": "X"}]},
        },
        {
            "id": "s2", "title": "二", "public": "p2", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "K"}]},
        },
    ],
}

# Two consecutive timed stages: the stale-timer regression needs both s1 and
# s2 to carry a duration, so a delayed s1 callback can mistake itself for s2's
# timeout. A clock mock drives the engine deterministically (no real sleeps).
TIMED2 = {
    "id": "timed2",
    "name": "连续限时演练",
    "roles": [{"id": "a", "name": "甲", "invite": "AAA"}],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "p1", "duration": 10,
            "default_next": "s2",
            "options": {"a": [{"id": "x", "text": "X"}]},
        },
        {
            "id": "s2", "title": "二", "public": "p2", "duration": 20,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "K"}]},
        },
    ],
}

WS_DRILL = {
    "id": "wsdrill",
    "name": "WS演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
    ],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "公开一", "duration": 60,
            "default_next": "s2",
            "views": {"a": "甲私有一", "b": "乙私有一"},
            "options": {
                "a": [{"id": "x", "text": "X"}],
                "b": [{"id": "p", "text": "P"}],
            },
        },
        {
            "id": "s2", "title": "二", "public": "公开二", "duration": None,
            "default_next": None,
            "views": {"b": "乙私有二"},
            "options": {"b": [{"id": "k", "text": "K"}]},
        },
    ],
}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
async def fetch_events(db, sid):
    rows = await db.all(
        "SELECT version, seq, type, actor, at, payload FROM events "
        "WHERE session_id=? ORDER BY version, seq",
        (sid,),
    )
    return load_events(rows)


def log_error(events):
    """Validate the unified allocation invariants of an event log."""
    versions = sorted({e["version"] for e in events})
    if versions != list(range(1, len(versions) + 1)):
        return f"版本号不是从 1 连续递增: {versions}"
    keys = [(e["version"], e["seq"]) for e in events]
    if len(set(keys)) != len(keys):
        return "存在重复 (version, seq)"
    for v in versions:
        seqs = sorted(e["seq"] for e in events if e["version"] == v)
        if seqs != list(range(len(seqs))):
            return f"v{v} 内 seq 不是从 0 连续分配: {seqs}"
    return None


def check_log(name, events):
    err = log_error(events)
    check(name, err is None, err or "")


def ev_at(events, version, seq):
    for e in events:
        if e["version"] == version and e["seq"] == seq:
            return e
    return None


# --------------------------------------------------------------------------- #
# [1] YAML validation
# --------------------------------------------------------------------------- #
def test_definitions():
    print("[1] YAML 校验")
    d = load_file(Path(__file__).resolve().parents[2] / "drills" / "chem-plant.yaml")
    check("示例剧本可解析", d.id == "chem-plant")
    check("3 个角色", len(d.roles) == 3)
    check("8 个阶段", len(d.stages) == 8)

    base = yaml.safe_load(yaml.safe_dump(DUEL))
    parse_definition(base)  # valid baseline must not raise
    bad_invite = {**base, "roles": base["roles"] + [{"id": "z", "name": "丙", "invite": "AAA"}]}
    try:
        parse_definition(bad_invite)
        check("重复邀请码被拒", False)
    except DrillError:
        check("重复邀请码被拒", True)
    bad_next = yaml.safe_load(yaml.safe_dump(DUEL))
    bad_next["stages"][0]["default_next"] = "nope"
    try:
        parse_definition(bad_next)
        check("悬空 next 被拒", False)
    except DrillError:
        check("悬空 next 被拒", True)


# --------------------------------------------------------------------------- #
# [2] join -> start allocation (regression: start used to hardcode version=1)
# --------------------------------------------------------------------------- #
async def test_join_then_start(eng):
    print("[2] 先加入再启动的版本分配")
    sid = await eng.create_session("duel")
    ja = await eng.join(sid, "AAA", "张三")
    jb = await eng.join(sid, "BBB", "李四")
    check("两次加入各占一个版本", (ja["version"], jb["version"]) == (1, 2))

    r = await eng.start(sid)
    check("启动占用下一版本 v3", r["version"] == 3)
    ev = await fetch_events(eng.db, sid)
    check_log("事件日志分配规则一致", ev)
    check("v3.0 为 session_started", (ev_at(ev, 3, 0) or {}).get("type") == "session_started")
    check("v3.1 为 stage_opened", (ev_at(ev, 3, 1) or {}).get("type") == "stage_opened")

    _, _, st = await eng.load(sid)
    check("状态 running 且进入首阶段", st.status == "running" and st.current == "s1")
    check("名册含两名参与者", sorted(st.roster) == ["a", "b"])

    # start may still run with zero joins and must then be v1
    sid2 = await eng.create_session("duel")
    r2 = await eng.start(sid2)
    check("无人加入时启动为 v1", r2["version"] == 1)
    check_log("无人加入日志一致", await fetch_events(eng.db, sid2))

    # idempotency / rejection paths must not add events
    n0 = len(await fetch_events(eng.db, sid))
    try:
        await eng.start(sid)
        check("重复 start 被拒", False)
    except EngineError as exc:
        check("重复 start 被拒", exc.status == 409)
    try:
        await eng.join(sid, "AAA", "王五")
        check("重复邀请码加入被拒", False)
    except EngineError as exc:
        check("重复邀请码加入被拒", exc.status == 409)
    try:
        await eng.join(sid, "NOPE", "王五")
        check("无效邀请码被拒", False)
    except EngineError as exc:
        check("无效邀请码被拒", exc.status == 403)
    check("被拒请求不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    jc = await eng.join(sid, "CCC", "王五")  # late join while running
    check("进行中加入占下一版本", jc["version"] == 4)
    check_log("加入后日志仍一致", await fetch_events(eng.db, sid))


# --------------------------------------------------------------------------- #
# [3] auto-settle on last submission (regression: choice and seal shared
#     the same (version, seq=0))
# --------------------------------------------------------------------------- #
async def test_auto_settle(eng):
    print("[3] 最后一名决策者提交触发自动结算")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.start(sid)  # v3

    r1 = await eng.submit(sid, "a", "s1", "x")
    check("首名提交被接受且未结算", r1["accepted"] and not r1["settled"])
    r2 = await eng.submit(sid, "b", "s1", "p")
    check("末名提交被接受并结算", r2["accepted"] and r2["settled"])
    check("选择与结算同属一个版本", r1["version"] + 1 == r2["version"] == r2["seal_version"])
    check("结算原因为 complete", r2["seal_reason"] == "complete")
    check("自动进入下一阶段 s2", r2["next_stage"] == "s2" and not r2["ended"])

    ev = await fetch_events(eng.db, sid)
    v = r2["version"]
    check("vN.0 为 stage_choice", (ev_at(ev, v, 0) or {}).get("type") == "stage_choice")
    check("vN.1 为 stage_sealed", (ev_at(ev, v, 1) or {}).get("type") == "stage_sealed")
    check("vN.2 为 stage_opened", (ev_at(ev, v, 2) or {}).get("type") == "stage_opened")
    check_log("日志分配规则一致", ev)

    subs = await eng.db.all(
        "SELECT role_id, choice_id FROM submissions WHERE session_id=? ORDER BY role_id",
        (sid,),
    )
    check("两名决策者的提交均已落库", [dict(s)["role_id"] for s in subs] == ["a", "b"])

    # final stage: single decider submits -> seal + session end in one version
    r3 = await eng.submit(sid, "a", "s2", "k")
    check("末阶段提交后演练结束", r3["settled"] and r3["ended"])
    ev = await fetch_events(eng.db, sid)
    v3 = r3["version"]
    check("终版 v.0 为 stage_choice", (ev_at(ev, v3, 0) or {}).get("type") == "stage_choice")
    check("终版 v.1 为 stage_sealed", (ev_at(ev, v3, 1) or {}).get("type") == "stage_sealed")
    check("终版 v.2 为 session_ended", (ev_at(ev, v3, 2) or {}).get("type") == "session_ended")
    check_log("终版日志一致", ev)

    # idempotent replays after settlement: no new events, original outcome
    n0 = len(ev)
    d1 = await eng.submit(sid, "a", "s1", "x")
    check("阶段结算后重发幂等", d1["duplicate"] and d1["same"] and d1["settled"])
    d2 = await eng.submit(sid, "b", "s1", "q")  # different choice, same slot
    check("重发不同选项返回首次结果", d2["duplicate"] and not d2["same"] and d2["choice_id"] == "p")
    check("幂等重放不产生事件", len(await fetch_events(eng.db, sid)) == n0)


# --------------------------------------------------------------------------- #
# [4] deadline races
# --------------------------------------------------------------------------- #
async def test_deadline_race(eng):
    print("[4] 截止竞争")
    sid = await eng.create_session("fast")
    start1 = await eng.start(sid)
    await asyncio.sleep(1.2)  # let the 1s deadline pass

    r = await eng.submit(sid, "a", "s1", "x")
    check("截止后提交被拒", not r["accepted"] and not r["duplicate"])
    check("拒绝原因 deadline", r.get("reason") == "deadline")
    check("拒绝与结算同版本完成", r["settled"] and r["next_stage"] == "s2")
    ev = await fetch_events(eng.db, sid)
    check("拒绝路径不含 choice 事件", all(e["type"] != "stage_choice" for e in ev))
    check("v2.0 为 stage_sealed", (ev_at(ev, 2, 0) or {}).get("type") == "stage_sealed")
    check("v2.1 为 stage_opened", (ev_at(ev, 2, 1) or {}).get("type") == "stage_opened")
    check_log("拒绝路径日志一致", ev)

    n0 = len(ev)
    # s2 has duration: null; the captured s1 deadline is stale here as well.
    noop = await eng.settle(sid, "deadline", expected_deadline=start1["deadline"])
    check("无时限阶段的 deadline 结算为空转", noop.get("noop") is True)
    try:
        await eng.submit(sid, "a", "s1", "x")
        check("已结算阶段的迟到提交被拒", False)
    except EngineError as exc:
        check("已结算阶段的迟到提交被拒", exc.status == 409)
    check("空转与迟到提交不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    # concurrent storm right after the deadline: exactly one seal may survive
    sid2 = await eng.create_session("fast")
    start2 = await eng.start(sid2)
    await asyncio.sleep(1.2)
    results = await asyncio.gather(
        *[eng.submit(sid2, "a", "s1", "x") for _ in range(5)],
        *[eng.settle(sid2, "deadline", expected_deadline=start2["deadline"])
          for _ in range(3)],
        return_exceptions=True,
    )
    ev2 = await fetch_events(eng.db, sid2)
    seals = [e for e in ev2 if e["type"] == "stage_sealed"]
    check("并发下只有一次结算事件", len(seals) == 1)
    check("结算原因为 deadline", seals[0]["payload"]["reason"] == "deadline")
    submits_ok = all(
        isinstance(r, Exception)
        or (not r.get("accepted") and r.get("reason") == "deadline")
        for r in results[:5]
    )
    check("并发提交全部被拒/报错", submits_ok)
    settles = results[5:]
    check("并发结算至多一个生效", sum(1 for r in settles if not r.get("noop")) <= 1)
    _, _, st2 = await eng.load(sid2)
    check("竞争后进入 s2 且未结束", st2.current == "s2" and st2.status == "running")
    check_log("竞争后日志一致", ev2)


# --------------------------------------------------------------------------- #
# [5] stale timer across two consecutive timed stages (regression: a delayed
#     stage-1 deadline callback used to seal the already-open timed stage 2)
# --------------------------------------------------------------------------- #
async def test_stale_deadline_timer(eng):
    print("[5] 连续限时阶段的旧定时器身份核对")
    import app.engine as emod

    real_now = emod.now_ts

    async def open_timed2():
        """Start a timed2 session; returns (sid, s1_deadline) at now=t0."""
        sid = await eng.create_session("timed2")
        r = await eng.start(sid)
        _, _, st = await eng.load(sid)
        check("进入限时阶段 s1", st.current == "s1" and st.deadline is not None)
        return sid, r["deadline"]

    def sealed_stages(sid, events):
        return [e["payload"]["stage_id"] for e in events if e["type"] == "stage_sealed"]

    try:
        # --- A: the last participant's submission advances s1 -> timed s2 ---
        clock = {"t": real_now()}
        emod.now_ts = lambda: clock["t"]
        sid, d1 = await open_timed2()

        # A timer still armed for s1 that wakes up *early* must never settle.
        clock["t"] = d1 - 1
        early = await eng.settle(sid, "deadline", expected_deadline=d1)
        check("旧定时器提前唤醒为空转", early.get("noop") and early.get("early"))
        check("提前唤醒不产生事件", sealed_stages(sid, await fetch_events(eng.db, sid)) == [])

        # Last submission lands while s1 is still open: complete -> s2, armed.
        clock["t"] = d1 - 0.5
        sub = await eng.submit(sid, "a", "s1", "x")
        d2 = sub["next_deadline"]
        check("末名提交推进到同样限时的 s2",
              sub["accepted"] and sub["sealed_stage"] == "s1"
              and sub["next_stage"] == "s2" and d2 is not None and d2 != d1)
        _, _, st = await eng.load(sid)
        check("当前截止已换成 s2 的 d2", st.current == "s2" and st.deadline == d2)
        n_ev = len(await fetch_events(eng.db, sid))

        # The stale s1 callback is delayed by the session lock / event loop
        # until stage 2's deadline has also passed. Old code mistook this for
        # s2's timeout and wrote stage_sealed(s2, deadline).
        clock["t"] = d2 + 5
        stale = await eng.settle(sid, "deadline", expected_deadline=d1)
        check("身份不符的旧回调空转", stale.get("noop") is True and stale.get("stale") is True)
        ev = await fetch_events(eng.db, sid)
        check("旧回调不新增任何事件", len(ev) == n_ev)
        check("s2 未被旧回调误结算", sealed_stages(sid, ev) == ["s1"])
        _, _, st = await eng.load(sid)
        check("s2 仍然开放且未结束", st.current == "s2" and st.status == "running")
        check_log("旧回调后日志仍一致", ev)

        # A callback that lost its deadline (e.g. duration:null stage) is also
        # stale rather than a seal.
        lost = await eng.settle(sid, "deadline", expected_deadline=None)
        check("携带空截止的回调按身份不符空转", lost.get("noop") and lost.get("stale"))
        check("空截止回调不新增事件", len(await fetch_events(eng.db, sid)) == n_ev)

        # The genuine s2 timer settles s2 exactly once.
        legit = await eng.settle(sid, "deadline", expected_deadline=d2)
        check("正常回调结算 s2", legit.get("sealed_stage") == "s2"
              and legit.get("seal_reason") == "deadline" and legit.get("ended"))
        again = await eng.settle(sid, "deadline", expected_deadline=d2)
        check("正常回调只能结算一次（重复者空转）", again.get("noop") is True)
        ev = await fetch_events(eng.db, sid)
        check("最终每个阶段仅一条 seal", sealed_stages(sid, ev) == ["s1", "s2"])
        check("s2 seal 原因为 deadline",
              next(e for e in ev if e["type"] == "stage_sealed"
                   and e["payload"]["stage_id"] == "s2")["payload"]["reason"] == "deadline")
        check_log("正常回调后日志一致", ev)

        # --- B: a host settle advances s1 -> timed s2 before the timer runs --
        clock["t"] = real_now()
        sid2, d1b = await open_timed2()
        clock["t"] = d1b + 1  # host manually settles once s1 is overdue
        host = await eng.settle(sid2, "host")
        d2b = host["next_deadline"]
        check("主持人手动结算推进到限时 s2",
              host.get("sealed_stage") == "s1" and host.get("next_stage") == "s2"
              and d2b is not None and d2b != d1b)
        n_ev2 = len(await fetch_events(eng.db, sid2))

        clock["t"] = d2b + 5
        stale2 = await eng.settle(sid2, "deadline", expected_deadline=d1b)
        check("手动结算后的旧 s1 回调空转",
              stale2.get("noop") is True and stale2.get("stale") is True)
        check("手动竞争旧回调不新增事件",
              len(await fetch_events(eng.db, sid2)) == n_ev2)
        check("s2 未被旧回调误结算",
              sealed_stages(sid2, await fetch_events(eng.db, sid2)) == ["s1"])

        legit2 = await eng.settle(sid2, "deadline", expected_deadline=d2b)
        check("s2 的正常回调仍可结算",
              legit2.get("sealed_stage") == "s2" and legit2.get("seal_reason") == "deadline")
        ev2 = await fetch_events(eng.db, sid2)
        check("手动竞争下每阶段仅一条 seal", sealed_stages(sid2, ev2) == ["s1", "s2"])
        check_log("手动竞争后日志一致", ev2)

        # --- C: several identical s1 callbacks firing concurrently ----------
        # (event-loop wakes the timer more than once): lock serializes them,
        # exact-deadline identity guarantees at most one seal.
        clock["t"] = real_now()
        sid3, d1c = await open_timed2()
        clock["t"] = d1c + 1
        results = await asyncio.gather(*[
            eng.settle(sid3, "deadline", expected_deadline=d1c) for _ in range(4)
        ])
        effective = [r for r in results if not r.get("noop")]
        ev3 = await fetch_events(eng.db, sid3)
        check("重复正常回调仅一次结算", len(effective) == 1)
        check("重复回调只 seal s1 一次", sealed_stages(sid3, ev3) == ["s1"])
        _, _, st3 = await eng.load(sid3)
        check("竞争后停留在 s2", st3.current == "s2" and st3.status == "running")
        check_log("重复回调并发后日志一致", ev3)
    finally:
        emod.now_ts = real_now


# --------------------------------------------------------------------------- #
# [6] TimerService bookkeeping: a firing stale task must not unregister the
#     successor timer (defence in depth; settle's identity check is authoritative)
# --------------------------------------------------------------------------- #
async def test_timer_bookkeeping():
    print("[6] 定时器登记薄不被旧任务覆盖")
    from app.main import TimerService

    timers = TimerService()
    fired = []
    import app.main as mainmod
    orig = mainmod.on_deadline

    async def record(session_id, expected_deadline):
        fired.append((session_id, expected_deadline))

    mainmod.on_deadline = record
    try:
        d1 = time.time() + 0.2
        timers.schedule("s", d1)
        await asyncio.sleep(0.25)  # s1 timer fires and unregisters itself
        check("触发后任务自行注销", "s" not in timers._tasks)
        check("s1 回调携带捕获的截止值", fired == [("s", d1)])

        # Simulate the regression window directly: a stage-2 successor has
        # already been armed when the stale stage-1 task finally reaches the
        # unregister step of _fire. The stale task must not pop the successor.
        async def stale_fire(session_id, deadline):
            # same unregister-then-callback sequence as TimerService._fire
            if timers._tasks.get(session_id) is asyncio.current_task():
                timers._tasks.pop(session_id, None)
            await mainmod.on_deadline(session_id, deadline)

        d2 = time.time() + 1
        timers.schedule("s", d2)
        successor = timers._tasks["s"]
        stale_task = asyncio.create_task(stale_fire("s", d1))
        await asyncio.sleep(0)  # let the stale task run to completion
        check("旧任务的注销步骤不会移除后继定时器",
              timers._tasks.get("s") is successor and not stale_task.cancelled())
        check("旧任务仍以旧截止值触发（由事务身份核对空转）", fired[-1] == ("s", d1))
        timers.cancel("s")
    finally:
        mainmod.on_deadline = orig
        timers.cancel("s")


# --------------------------------------------------------------------------- #
# [7] concurrent idempotency
# --------------------------------------------------------------------------- #
async def test_concurrent_idempotency(eng):
    print("[7] 并发幂等")
    sid = await eng.create_session("duel")
    await eng.start(sid)

    res = await asyncio.gather(*[eng.submit(sid, "a", "s1", "x") for _ in range(5)])
    accepted = [r for r in res if r["accepted"]]
    dups = [r for r in res if r.get("duplicate")]
    check("5 并发同提交仅 1 个被接受", len(accepted) == 1 and len(dups) == 4)
    ev = await fetch_events(eng.db, sid)
    check("仅产生一条 choice 事件", sum(1 for e in ev if e["type"] == "stage_choice") == 1)
    rows = await eng.db.all("SELECT * FROM submissions WHERE session_id=?", (sid,))
    check("submissions 仅一行", len(rows) == 1)

    res2 = await asyncio.gather(*[eng.submit(sid, "b", "s1", "p") for _ in range(3)])
    ok2 = [r for r in res2 if r["accepted"]]
    check("末名并发提交仅 1 个生效", len(ok2) == 1)
    check("生效者完成自动结算", ok2[0]["settled"] is True)
    ev = await fetch_events(eng.db, sid)
    check("仅产生一条 seal 事件", sum(1 for e in ev if e["type"] == "stage_sealed") == 1)
    check_log("并发提交后日志一致", ev)

    # concurrent manual settles: serialized by the session lock, each
    # effective settle seals exactly one stage, no stage is sealed twice
    sid2 = await eng.create_session("duel")
    await eng.start(sid2)
    settles = await asyncio.gather(*[eng.settle(sid2, "host") for _ in range(3)])
    effective = [r for r in settles if not r.get("noop")]
    ev2 = await fetch_events(eng.db, sid2)
    seals2 = [e for e in ev2 if e["type"] == "stage_sealed"]
    check("并发手动结算逐阶段推进", len(effective) == 2 and len(seals2) == 2)
    check("同一阶段不会被结算两次", sorted(e["payload"]["stage_id"] for e in seals2) == ["s1", "s2"])
    _, _, st2 = await eng.load(sid2)
    check("两阶段后演练结束", st2.status == "ended")
    check_log("并发结算后日志一致", ev2)

    # deadline timer vs host settle racing the same open stage: exactly one
    # of them seals s1; the loser either noops or settles the next stage
    sid3 = await eng.create_session("fast")
    start3 = await eng.start(sid3)
    await asyncio.sleep(1.2)
    await asyncio.gather(
        eng.settle(sid3, "deadline", expected_deadline=start3["deadline"]),
        eng.settle(sid3, "host"),
    )
    ev3 = await fetch_events(eng.db, sid3)
    seals3 = [e for e in ev3 if e["type"] == "stage_sealed"]
    s1_seals = [e for e in seals3 if e["payload"]["stage_id"] == "s1"]
    check("截止/手动竞争下 s1 只结算一次", len(s1_seals) == 1)
    check("s1 结算原因合法", s1_seals[0]["payload"]["reason"] in ("deadline", "host"))
    check("总结算数不超过阶段数", len(seals3) <= 2)
    check_log("截止/手动竞争日志一致", ev3)


# --------------------------------------------------------------------------- #
# [8] deterministic replay of every version prefix
# --------------------------------------------------------------------------- #
async def test_replay(eng):
    print("[8] 任意版本前缀的确定性回放")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.start(sid)
    await eng.submit(sid, "a", "s1", "x")
    await eng.submit(sid, "b", "s1", "p")   # auto-settle -> s2
    await eng.submit(sid, "a", "s2", "k")   # auto-settle -> end

    sess = await eng.get_session(sid)
    d = await eng.get_definition("duel")
    events = await fetch_events(eng.db, sid)
    final = max(e["version"] for e in events)
    check_log("完整日志一致", events)

    ok = True
    for v in range(0, final + 1):
        prefix = [e for e in events if e["version"] <= v]
        s1, s2 = reduce_events(prefix), reduce_events(prefix)
        if s1 != s2 or s1.last_version != v:
            ok = False
    check("每个前缀折叠确定且版本单调", ok)

    _, _, st_load = await eng.load(sid)
    check("load 与 reduce_events 结果一致", st_load == reduce_events(events))

    full = build_replay(sess, d, events, ("host", ""))
    frames = full["frames"]
    check("帧数 == 最终版本+1", len(frames) == final + 1)
    check("帧版本连续", [f["version"] for f in frames] == list(range(final + 1)))
    check("帧内快照版本单调", all(
        f["snapshot"]["session"]["version"] == f["version"] for f in frames
    ))

    ok = True
    for k in sorted({0, 1, final // 2, final - 1, final}):
        pref = build_replay(sess, d, [e for e in events if e["version"] <= k], ("host", ""))
        if pref["frames"] != frames[: k + 1] or pref["final_version"] != k:
            ok = False
    check("任意前缀回放与全量回放逐帧一致", ok)

    pa = build_replay(sess, d, events, ("role", "a"))
    dump_a = json.dumps(pa, ensure_ascii=False)
    check("参与者回放不含他人私有信息", "乙的隐秘s1" not in dump_a)
    check("参与者回放含本人私有信息", "甲的隐秘s1" in dump_a)
    check("参与者回放不暴露原始事件", pa["events"] is None)


# --------------------------------------------------------------------------- #
# [9] end_session allocation (regression: created-branch hardcoded version=1)
# --------------------------------------------------------------------------- #
async def test_end_session(eng):
    print("[9] 手动终止的版本分配")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.end_session(sid)
    ev = await fetch_events(eng.db, sid)
    check("未开始即终止：ended 落在 v3.0", (ev_at(ev, 3, 0) or {}).get("type") == "session_ended")
    check_log("未开始终止日志一致", ev)
    _, _, st = await eng.load(sid)
    check("状态已结束", st.status == "ended")

    n0 = len(ev)
    try:
        await eng.join(sid, "CCC", "王五")
        check("结束后加入被拒", False)
    except EngineError as exc:
        check("结束后加入被拒", exc.status == 409)
    await eng.end_session(sid)  # idempotent
    check("重复终止不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    sid2 = await eng.create_session("duel")
    await eng.start(sid2)
    await eng.end_session(sid2)
    ev2 = await fetch_events(eng.db, sid2)
    check("进行中终止：v2.0 为 stage_sealed", (ev_at(ev2, 2, 0) or {}).get("type") == "stage_sealed")
    check("进行中终止：v2.1 为 stage_opened", (ev_at(ev2, 2, 1) or {}).get("type") == "stage_opened")
    check("进行中终止：v3.0 为 session_ended", (ev_at(ev2, 3, 0) or {}).get("type") == "session_ended")
    check("终止原因为 host", ev_at(ev2, 2, 0)["payload"]["reason"] == "host")
    check_log("进行中终止日志一致", ev2)


# --------------------------------------------------------------------------- #
# [10] end-to-end over HTTP + WebSocket through the real app
# --------------------------------------------------------------------------- #
def recv_until(ws, pred, what, limit=50):
    for _ in range(limit):
        msg = ws.receive_json()
        if pred(msg):
            return msg
    raise RuntimeError(f"未收到预期消息: {what}")


def is_snapshot(msg, **session):
    return msg.get("type") == "snapshot" and all(
        msg["snapshot"]["session"].get(k) == v for k, v in session.items()
    )


def test_http_ws_e2e():
    print("[10] HTTP + WebSocket 端到端")
    with TestClient(app) as client:
        check("健康检查", client.get("/api/health").json() == {"ok": True})
        check("首页可访问", client.get("/").status_code == 200)
        check("无密钥访问被拒", client.get("/api/drills").status_code == 401)

        r = client.post("/api/drills", json={
            "yaml": yaml.safe_dump(WS_DRILL, allow_unicode=True, sort_keys=False),
        }, headers=HOST)
        check("上传剧本生成草稿 v1", r.status_code == 200
              and r.json()["id"] == "wsdrill" and r.json()["version"] == 1
              and r.json()["status"] == "draft" and len(r.json()["sha256"]) == 64)

        # Drafts cannot back sessions until published.
        blocked = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST)
        check("草稿不能创建会话", blocked.status_code == 409)

        r = client.post("/api/drills/wsdrill/versions/1/publish",
                        json={"base_version": None}, headers=HOST)
        check("发布草稿 v1", r.status_code == 200 and r.json()["status"] == "published")

        # Version list + diff endpoints
        vl = client.get("/api/drills/wsdrill/versions", headers=HOST).json()
        check("版本列表含 v1 已发布", len(vl) == 1 and vl[0]["version"] == 1
              and vl[0]["status"] == "published" and vl[0]["yaml"])

        r = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST)
        check("创建会话", r.status_code == 201)
        sid = r.json()["session_id"]
        check("会话响应带剧本版本与摘要", r.json()["drill_version"] == 1
              and len(r.json()["drill_sha256"]) == 64
              and r.json()["digest_short"] == r.json()["drill_sha256"][:12])

        ja = client.post(f"/api/sessions/{sid}/join", json={"invite": "AAA", "name": "张三"})
        jb = client.post(f"/api/sessions/{sid}/join", json={"invite": "BBB", "name": "李四"})
        check("两名参与者加入", ja.status_code == 200 and jb.status_code == 200)
        ta, tb = ja.json()["token"], jb.json()["token"]
        check("加入版本递增", (ja.json()["version"], jb.json()["version"]) == (1, 2))
        check("重复加入 409", client.post(
            f"/api/sessions/{sid}/join", json={"invite": "AAA", "name": "x"}).status_code == 409)

        rp = client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()
        check("启动前回放 final_version=2", rp["final_version"] == 2 and len(rp["frames"]) == 3)

        with ExitStack() as stack:
            bad = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token=nope"))
            check("WS 认证失败", bad.receive_json()["type"] == "error")

            host_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?host=test-key"))
            a_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token={ta}"))
            b_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token={tb}"))
            recv_until(host_ws, lambda m: m.get("type") == "snapshot", "host snapshot")
            snap_a = recv_until(a_ws, lambda m: m.get("type") == "snapshot", "a snapshot")
            recv_until(b_ws, lambda m: m.get("type") == "snapshot", "b snapshot")
            check("参与者快照按角色裁剪", snap_a["snapshot"]["me"]["role_id"] == "a")
            check("WS 快照带绑定版本与摘要",
                  snap_a["snapshot"]["drill"]["version"] == 1
                  and len(snap_a["snapshot"]["drill"]["sha256"]) == 64)

            host_ws.send_json({"type": "start"})
            recv_until(host_ws, lambda m: is_snapshot(m, status="running"), "host running")
            run_a = recv_until(a_ws, lambda m: is_snapshot(m, status="running"), "a running")
            recv_until(b_ws, lambda m: is_snapshot(m, status="running"), "b running")
            dump_a = json.dumps(run_a, ensure_ascii=False)
            check("运行快照不含他人私有视图", "乙私有一" not in dump_a)
            check("运行快照含本人私有视图", "甲私有一" in dump_a)

            a_ws.send_json({"type": "submit", "choice_id": "x", "client_stage": "s1"})
            ra = recv_until(a_ws, lambda m: m.get("type") == "submit_result", "a result")
            check("甲提交被接受", ra["accepted"] and not ra["settled"])

            b_ws.send_json({"type": "submit", "choice_id": "p", "client_stage": "s1"})
            rb = recv_until(b_ws, lambda m: m.get("type") == "submit_result", "b result")
            check("乙提交触发自动结算", rb["accepted"] and rb["settled"] and rb["next_stage"] == "s2")

            n_before = len(client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()["events"])
            a_ws.send_json({"type": "submit", "choice_id": "x", "client_stage": "s1"})
            rd = recv_until(a_ws, lambda m: m.get("type") == "submit_result", "dup result")
            check("WS 重复提交幂等", rd["duplicate"] and rd["same"])
            n_after = len(client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()["events"])
            check("重复提交不新增事件", n_before == n_after)

            host_ws.send_json({"type": "settle"})
            end_snap = recv_until(host_ws, lambda m: is_snapshot(m, status="ended"), "ended")
            check("主持人手动结算后结束", end_snap["snapshot"]["session"]["status"] == "ended")
            recv_until(a_ws, lambda m: is_snapshot(m, status="ended"), "a ended")

        rp = client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()
        check("回放带剧本版本标识", rp["drill_version"] == 1 and len(rp["drill_sha256"]) == 64)
        check("端到端日志分配规则一致", log_error(rp["events"]) is None,
              log_error(rp["events"]) or "")
        check("端到端最终版本 v6", rp["final_version"] == 6)
        rp2 = client.get(f"/api/sessions/{sid}/replay", params={"version": 2}, headers=HOST).json()
        check("版本前缀回放 final_version=2", rp2["final_version"] == 2 and len(rp2["frames"]) == 3)
        check("前缀帧与全量一致", rp2["frames"] == rp["frames"][:3])

        rec_h = client.get(f"/api/sessions/{sid}/record", params={"fmt": "json"}, headers=HOST).json()
        check("主持人全量记录", rec_h["kind"] == "host" and len(rec_h["participants"]) == 2)
        check("主持人记录带剧本版本", rec_h["drill_version"] == 1
              and rec_h["drill"]["version"] == 1 and rec_h["drill_sha256"] == rp["drill_sha256"])
        rec_a = client.get(f"/api/sessions/{sid}/record", params={"fmt": "json", "token": ta}).json()
        check("个人记录带剧本版本", rec_a["drill_version"] == 1
              and rec_a["drill"]["digest_short"] == rp["drill_sha256"][:12])
        dump_rec_a = json.dumps(rec_a, ensure_ascii=False)
        check("个人记录不含他人私有信息", "乙私有一" not in dump_rec_a and "乙私有二" not in dump_rec_a)
        check("个人记录含本人选择", rec_a["stages"][0]["my_choice"] == "x")
        md = client.get(f"/api/sessions/{sid}/record", params={"fmt": "md", "token": tb})
        check("个人 Markdown 记录可导出", md.status_code == 200 and "个人记录" in md.text)
        check("Markdown 含剧本版本行", "剧本版本：v1" in md.text)
        check("无令牌导出被拒", client.get(f"/api/sessions/{sid}/record").status_code == 401)


def test_http_version_api():
    print("[11b] HTTP 版本管理 API")
    http_drill = yaml.safe_load(yaml.safe_dump(WS_DRILL))
    http_drill["id"] = "httpdrill"
    with TestClient(app) as client:
        # unauthenticated
        check("版本接口需主持人密钥",
              client.get("/api/drills/httpdrill/versions").status_code == 401)

        # v1 draft
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": _dump(http_drill), "base_version": None})
        check("HTTP 保存 v1 草稿", r.status_code == 200 and r.json()["version"] == 1)
        check("未发布剧本不出现在可创建会话列表",
              any(d["id"] == "httpdrill" and d["published_version"] is None
                  for d in client.get("/api/drills", headers=HOST).json()))

        # publish
        r = client.post("/api/drills/httpdrill/versions/1/publish",
                        headers=HOST, json={"base_version": None})
        check("HTTP 发布 v1", r.status_code == 200 and r.json()["status"] == "published")

        # invalid YAML -> 400
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": "id: x\nstages: not-a-list"})
        check("非法 YAML 被拒 400", r.status_code == 400)

        # v2 draft based on v1
        v2 = yaml.safe_load(yaml.safe_dump(http_drill))
        v2["stages"][0]["title"] = "一（改）"
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": _dump(v2), "base_version": 1})
        check("HTTP 保存 v2 草稿", r.json()["version"] == 2 and r.json()["status"] == "draft")

        # diff endpoint
        d = client.get("/api/drills/httpdrill/diff?from=1&to=2", headers=HOST)
        check("HTTP 差异 200 且含阶段改动",
              d.status_code == 200 and d.json()["stages"]["changed"]
              and d.json()["stages"]["changed"][0]["id"] == "s1")
        check("HTTP 差异 404", client.get(
            "/api/drills/httpdrill/diff?from=1&to=99", headers=HOST).status_code == 404)

        # stale base -> 409, current release stays v1
        r = client.post("/api/drills/httpdrill/versions/2/publish",
                        headers=HOST, json={"base_version": 99})
        check("HTTP 基准冲突发布 409", r.status_code == 409)
        current = next(x for x in client.get("/api/drills", headers=HOST).json()
                       if x["id"] == "httpdrill")
        check("冲突后当前发布版不变", current["published_version"] == 1
              and current["draft_version"] == 2)

        # copy v1 as draft while v2 draft exists -> v3 identical to v1
        r = client.post("/api/drills/httpdrill/copy-draft", headers=HOST,
                        json={"source_version": 1, "base_version": 1})
        check("HTTP 复制历史版本为草稿", r.status_code == 200 and r.json()["version"] == 3
              and r.json()["sha256"] == client.get(
                  "/api/drills/httpdrill/versions/1", headers=HOST).json()["sha256"])
        # v2 got superseded
        vlist = {v["version"]: v["status"]
                 for v in client.get("/api/drills/httpdrill/versions", headers=HOST).json()}
        check("复制后 v2 草稿被顶为 superseded", vlist.get(2) == "superseded"
              and vlist.get(3) == "draft")

        # publish the copied draft (base v1) -> v3 released
        r = client.post("/api/drills/httpdrill/versions/3/publish",
                        headers=HOST, json={"base_version": 1})
        check("HTTP 发布复制草稿 v3", r.status_code == 200)
        vlist = {v["version"]: v["status"]
                 for v in client.get("/api/drills/httpdrill/versions", headers=HOST).json()}
        check("v3 发布、v1 历史", vlist.get(3) == "published" and vlist.get(1) == "history")

        # session pins v3; session list shows the pin
        sid = client.post("/api/sessions", params={"drill_id": "httpdrill"},
                          headers=HOST).json()["session_id"]
        srow = next(s for s in client.get("/api/sessions", headers=HOST).json()
                    if s["id"] == sid)
        check("会话列表标注绑定版本",
              srow["drill_version"] == 3 and len(srow["drill_sha256"]) == 64)


# --------------------------------------------------------------------------- #
# [11] script versions: immutable versions, digests, draft gating, session
#      pinning across later releases, structured diffs, copy-from-history and
#      optimistic publish concurrency
# --------------------------------------------------------------------------- #
VDRILL = {
    "id": "vdrill",
    "name": "版本演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
    ],
    "stages": [
        {
            "id": "s1", "title": "阶段一", "public": "公开一", "duration": 60,
            "default_next": "s2",
            "views": {"a": "甲的隐秘"},
            "options": {
                "a": [{"id": "x", "text": "甲选X"}, {"id": "y", "text": "甲选Y"}],
                "b": [{"id": "p", "text": "乙选P"}],
            },
            "transitions": [{"when": {"a": "y"}, "next": "s3"}],
        },
        {
            "id": "s2", "title": "阶段二", "public": "公开二", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "甲终选"}]},
        },
        {
            "id": "s3", "title": "阶段三", "public": "公开三", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "z", "text": "Z"}]},
        },
    ],
}


def _dump(dct):
    return yaml.safe_dump(dct, allow_unicode=True, sort_keys=False)


async def test_script_versions(eng):
    print("[11] 剧本版本与发布管理")

    d1 = parse_definition(yaml.safe_load(yaml.safe_dump(VDRILL)))
    digest1 = content_digest(d1)

    # --- brand-new upload: v1 is an immutable draft, not a release ---------
    r = await eng.save_draft(d1, _dump(VDRILL), None)
    check("首次上传生成 v1 草稿", r["version"] == 1 and r["status"] == "draft")
    check("返回不可变内容摘要（sha256）", r["sha256"] == digest1 and len(r["sha256"]) == 64)
    try:
        await eng.create_session("vdrill")
        check("草稿不能创建会话", False)
    except EngineError as exc:
        check("草稿不能创建会话", exc.status == 409)

    # Re-saving identical content is idempotent (same digest -> reused).
    r_same = await eng.save_draft(d1, _dump(VDRILL), None)
    check("相同内容重复保存复用版本", r_same["version"] == 1 and r_same.get("reused") is True)
    versions = await eng.list_versions("vdrill")
    check("版本列表仍只有一条", len(versions) == 1)

    # --- publishing makes it usable ----------------------------------------
    pub = await eng.publish_drill("vdrill", 1, None)
    check("发布 v1", pub["status"] == "published" and pub["published_at"] is not None)
    sid_old = await eng.create_session("vdrill")
    sess_old = await eng.get_session(sid_old)
    check("新会话绑定 v1 与摘要", sess_old["drill_version"] == 1
          and sess_old["drill_sha256"] == digest1)

    # Editing again must not be based on a stale (None) release; this mirrors
    # the "someone else released meanwhile" guard on save as well.
    d2_dict = yaml.safe_load(yaml.safe_dump(VDRILL))
    d2_dict["stages"][0]["title"] = "阶段一（修订）"
    d2_dict["stages"][0]["views"]["a"] = "甲的隐秘-改"
    d2_dict["stages"][0]["options"]["a"][0]["text"] = "甲选X-改"
    d2_dict["stages"][0]["transitions"][0]["next"] = "s2"
    d2_dict["stages"][1]["title"] = "阶段二（修订）"
    d2 = parse_definition(d2_dict)
    digest2 = content_digest(d2)
    check("修订内容摘要不同", digest2 != digest1)

    r2 = await eng.save_draft(d2, _dump(d2_dict), 1)
    check("编辑生成不可变 v2 草稿", r2["version"] == 2 and r2["status"] == "draft"
          and r2["base_version"] == 1 and r2["sha256"] == digest2)
    versions = await eng.list_versions("vdrill")
    check("版本列表两条（v2 草稿在前）", [v["version"] for v in versions] == [2, 1])
    check("v1 仍为已发布（草稿不影响发布版）",
          next(v for v in versions if v["version"] == 1)["status"] == "published")

    # Current release definition is still v1 while v2 is a draft.
    cur = await eng.get_definition("vdrill")
    check("发布前 get_definition 仍是 v1 内容", cur.stages["s1"].title == "阶段一")
    # New sessions while a draft is open still bind the released v1.
    sid_new = await eng.create_session("vdrill")
    check("草稿期间新会话仍绑定 v1",
          (await eng.get_session(sid_new))["drill_version"] == 1)

    # A third save supersedes the open v2 draft (row kept, marked superseded).
    d3_dict = yaml.safe_load(yaml.safe_dump(d2_dict))
    d3_dict["stages"][1]["public"] = "公开二-再改"
    d3 = parse_definition(d3_dict)
    r3 = await eng.save_draft(d3, _dump(d3_dict), 1)
    check("再次编辑生成 v3 并顶掉 v2 草稿", r3["version"] == 3)
    versions = await eng.list_versions("vdrill")
    st2 = next(v for v in versions if v["version"] == 2)
    check("旧草稿 v2 标记为 superseded 且保留", st2["status"] == "superseded")

    # --- structured diff v1 -> v3 ------------------------------------------
    diff = await eng.diff_versions("vdrill", 1, 3)
    check("差异存在", diff["has_changes"] is True)
    s1_change = next((c for c in diff["stages"]["changed"] if c["id"] == "s1"), None)
    check("差异含 s1", s1_change is not None)
    check("s1 标题字段差异",
          any(f["field"] == "title" and f["old"] == "阶段一" for f in s1_change["fields"]))
    check("s1 角色视图差异",
          any(v["role_id"] == "a" and v["old"] == "甲的隐秘"
              for v in s1_change["views"]["changed"]))
    check("s1 选项文本差异",
          any(c["role_id"] == "a"
              and any(oc["id"] == "x" and any(f["field"] == "text" for f in oc["fields"])
                      for oc in c["changed"])
              for c in s1_change["options"]["changed"]))
    check("s1 转移规则差异",
          len(s1_change["transitions"]["changed"]) == 1
          and s1_change["transitions"]["changed"][0]["new"]["next"] == "s2")
    s2_change = next((c for c in diff["stages"]["changed"] if c["id"] == "s2"), None)
    check("差异含 s2（公开信息/标题）", s2_change is not None)

    # Diff against a nonexistent version is a 404.
    try:
        await eng.diff_versions("vdrill", 1, 99)
        check("不存在版本的差异报 404", False)
    except EngineError as exc:
        check("不存在版本的差异报 404", exc.status == 404)

    # --- optimistic concurrency: publishing v3 based on a stale release ----
    # v3 is an open draft based on v1. Another host (a separate process /
    # transaction is a legal writer with BEGIN IMMEDIATE) releases v2 first.
    # Emulate that external commit directly, then publishing v3 with its stale
    # base must fail and leave the current release untouched.
    await eng.db.db.execute(
        "UPDATE drill_versions SET status='history' "
        "WHERE drill_id='vdrill' AND version=1 AND status='published'"
    )
    await eng.db.db.execute(
        "UPDATE drill_versions SET status='published', published_at=? "
        "WHERE drill_id=? AND version=2",
        (str(now_ts()), "vdrill"),
    )
    await eng.db.db.execute(
        "UPDATE drills SET published_version=2, draft_version=3 WHERE id='vdrill'"
    )
    await eng.db.commit()
    drill = await eng._drill_row("vdrill")
    check("他人已先发布 v2", drill["published_version"] == 2)
    try:
        await eng.publish_drill("vdrill", 3, 1)
        check("基准版本被更新后发布被拒", False)
    except EngineError as exc:
        check("基准版本被更新后发布被拒（409）", exc.status == 409)
    drill = await eng._drill_row("vdrill")
    check("发布失败不改变当前发布版（仍 v2）", drill["published_version"] == 2)
    v3row = await eng.get_version("vdrill", 3)
    check("失败后 v3 仍为草稿", v3row["status"] == "draft")
    # A save carrying the stale base is rejected as well.
    try:
        await eng.save_draft(d3, _dump(d3_dict), 1)
        check("基于旧基准保存被拒", False)
    except EngineError as exc:
        check("基于旧基准保存被拒（409）", exc.status == 409)

    # Recover from the current release: copy v2 into a fresh draft, edit it,
    # then publish against the current base v2.
    cp = await eng.copy_as_draft("vdrill", 2, 2)
    check("从当前发布版复制为新草稿", cp["status"] == "draft" and cp["version"] >= 4)
    d4_dict = yaml.safe_load(yaml.safe_dump(d2_dict))
    d4_dict["stages"][0]["public"] = "公开一-新编辑"
    d4 = parse_definition(d4_dict)
    r4 = await eng.save_draft(d4, _dump(d4_dict), 2)
    check("刷新后基于 v2 编辑生成新草稿", r4["base_version"] == 2)
    pub4 = await eng.publish_drill("vdrill", r4["version"], 2)
    check("基于最新版编辑后发布成功", pub4["status"] == "published")
    versions = await eng.list_versions("vdrill")
    check("旧发布版 v2 变 history",
          next(v for v in versions if v["version"] == 2)["status"] == "history")
    check("只有一个 published",
          [v["version"] for v in versions if v["status"] == "published"] == [r4["version"]])

    # Copy from an arbitrary *historical* version (v1) into a new draft.
    cp1 = await eng.copy_as_draft("vdrill", 1, r4["version"])
    check("从历史 v1 复制为新草稿", cp1["status"] == "draft" and cp1["sha256"] == digest1)
    v1copy = await eng.get_version_def("vdrill", cp1["version"])
    check("复制草稿内容与 v1 完全一致", content_digest(v1copy) == digest1)

    # --- the decisive property: the session created on v1 stays on v1 ------
    # Drive the old session through start + a branch, then prove its snapshot,
    # branching and replay still use v1 even though the release has moved on.
    await eng.join(sid_old, "AAA", "张三")
    await eng.start(sid_old)
    # In v1, a=y branches to s3. Submit a=x, b=p -> transition {a:y} fails,
    # a=x has no option next -> default_next s2. (Same shape in later content,
    # but the stage titles/views the snapshot renders must remain v1's.)
    await eng.submit(sid_old, "a", "s1", "x")
    res = await eng.submit(sid_old, "b", "s1", "p")
    check("v1 会话按 v1 规则分支到 s2（默认分支）", res["next_stage"] == "s2")
    sess_old2, d_old, state_old = await eng.load(sid_old)
    check("已建会话加载的定义仍是 v1",
          sess_old2["drill_version"] == 1 and d_old.stages["s1"].title == "阶段一"
          and d_old.stages["s1"].views["a"] == "甲的隐秘"
          and content_digest(d_old) == digest1)

    # The v1 transition {a:y}->s3 still exists for this session: sid_old
    # itself is the proof that a created/started session keeps v1 forever.
    # Confirm sid_new (created mid-draft on released v1) also stays v1 after
    # the later releases.
    sess_new2, d_new, _ = await eng.load(sid_new)
    check("进行前创建的会话同样锁定 v1",
          sess_new2["drill_version"] == 1 and content_digest(d_new) == digest1)

    # Replay of the old session contains v1 content only.
    rows = await eng.db.all(
        "SELECT version, seq, type, actor, at, payload FROM events "
        "WHERE session_id=? ORDER BY version, seq", (sid_old,))
    replay = build_replay(sess_old2, d_old, load_events(rows), ("host", ""))
    dump_replay = json.dumps(replay, ensure_ascii=False)
    check("回放不含后续版本文本", "阶段一（修订）" not in dump_replay
          and "公开一-新编辑" not in dump_replay)
    check("回放保留 v1 文本", "阶段一" in dump_replay and "甲的隐秘" in dump_replay)
    check("回放响应带 v1 版本标识", replay["drill_version"] == 1
          and replay["drill_sha256"] == digest1)

    # A brand-new session after release uses the newest release, not v1.
    sid_latest = await eng.create_session("vdrill")
    sess_latest = await eng.get_session(sid_latest)
    check("新会话使用当前发布版", sess_latest["drill_version"] == r4["version"])
    _, d_latest, _ = await eng.load(sid_latest)
    check("新会话内容为最新发布版", d_latest.stages["s1"].public == "公开一-新编辑")

    # Only a released version can be published again; history/draft rules.
    try:
        await eng.publish_drill("vdrill", 2, r4["version"])
        check("历史版本不能重复发布", False)
    except EngineError as exc:
        check("非草稿版本不能发布", exc.status == 409)


# --------------------------------------------------------------------------- #
# [12] migration of a pre-versioning database
# --------------------------------------------------------------------------- #
async def test_legacy_migration():
    print("[12] 旧版数据库迁移")
    import aiosqlite
    import dataclasses

    legacy_path = str(Path(_TMP) / "legacy.db")
    d = parse_definition(yaml.safe_load(yaml.safe_dump(DUEL)))
    conn = await aiosqlite.connect(legacy_path)
    await conn.executescript("""
    CREATE TABLE drills (
        id TEXT PRIMARY KEY, name TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '', definition TEXT NOT NULL);
    CREATE TABLE sessions (
        id TEXT PRIMARY KEY, drill_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'created',
        reason TEXT, created_at TEXT NOT NULL, started_at TEXT, ended_at TEXT);
    CREATE TABLE participants (
        session_id TEXT NOT NULL, role_id TEXT NOT NULL, display_name TEXT NOT NULL,
        token TEXT NOT NULL, joined_at TEXT NOT NULL,
        PRIMARY KEY (session_id, role_id), UNIQUE (session_id, token));
    CREATE TABLE events (
        session_id TEXT NOT NULL, version INTEGER NOT NULL, seq INTEGER NOT NULL,
        type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT '', at TEXT NOT NULL,
        payload TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY (session_id, version, seq));
    CREATE TABLE submissions (
        session_id TEXT NOT NULL, version INTEGER NOT NULL, role_id TEXT NOT NULL,
        stage_id TEXT NOT NULL, choice_id TEXT NOT NULL, at TEXT NOT NULL,
        PRIMARY KEY (session_id, stage_id, role_id));
    """)
    await conn.execute(
        "INSERT INTO drills(id,name,description,definition) VALUES(?,?,?,?)",
        (d.id, d.name, d.description,
         json.dumps(dataclasses.asdict(d), ensure_ascii=False)),
    )
    await conn.execute(
        "INSERT INTO sessions(id,drill_id,status,created_at) VALUES(?,?, 'created', ?)",
        ("legacys1", d.id, "1.0"),
    )
    await conn.commit()
    await conn.close()

    mdb = Database(legacy_path)
    await mdb.connect()
    try:
        meng = Engine(mdb)
        drill = await meng._drill_row("duel")
        check("迁移后 drill 有发布版本", drill and drill["published_version"] == 1)
        versions = await meng.list_versions("duel")
        check("迁移生成不可变 v1 已发布",
              len(versions) == 1 and versions[0]["version"] == 1
              and versions[0]["status"] == "published"
              and versions[0]["sha256"] == content_digest(d))
        sess = await meng.get_session("legacys1")
        check("旧会话绑定 v1 与摘要",
              sess["drill_version"] == 1 and sess["drill_sha256"] == content_digest(d))
        _, d_loaded, st = await meng.load("legacys1")
        check("旧会话可按绑定版本重建快照",
              d_loaded.stages["s1"].title == "阶段一" and st.status == "created")
        # Idempotent: reconnecting must not create a v2.
        await mdb.close()
        mdb2 = Database(legacy_path)
        await mdb2.connect()
        meng2 = Engine(mdb2)
        versions2 = await meng2.list_versions("duel")
        check("迁移幂等：不重复生成版本", len(versions2) == 1)
        await mdb2.close()
    finally:
        if mdb.db is not None:
            await mdb.close()


# --------------------------------------------------------------------------- #
# [13] joint exercise orchestration: directed dependencies, AND barrier,
#      exactly-once injection, no early unlock, out-of-order/duplicate safety
# --------------------------------------------------------------------------- #
async def test_joint_orchestration(eng):
    print("[13] 联合演练编排")
    from app.joints import JointOrchestrator, validate_plan

    jo = JointOrchestrator(eng.db, eng)
    duel_def = await eng.get_version_def("duel", 1)
    # s1 possible outcomes: option next (s2), @end (option y), transitions,
    # default s2 — a superset including @end.
    outs = stage_outcomes(duel_def.stages["s1"])
    check("s1 可能结局集合含 s2 与 @end", "s2" in outs and "@end" in outs, str(outs))

    sA = await eng.create_session("duel")
    sB = await eng.create_session("duel")
    sC = await eng.create_session("duel")
    defs = {s: duel_def for s in (sA, sB, sC)}
    vers = {s: 1 for s in (sA, sB, sC)}

    # validation matrix
    def vplan(sids, rules):
        validate_plan("联演", sids, rules, defs, vers)

    def expect_joint_error(name, sids, rules):
        try:
            vplan(sids, rules)
            check(name, False)
        except JointError:
            check(name, True)

    expect_joint_error("少于两个会话被拒", [sA], [])
    expect_joint_error("无规则被拒", [sA, sB], [])
    expect_joint_error("同会话依赖被拒", [sA, sB], [{
        "target_session_id": sA, "target_stage_id": "s2", "title": "t", "public": "p",
        "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]}])
    expect_joint_error("无效结局被拒", [sA, sC], [{
        "target_session_id": sC, "target_stage_id": "s2", "title": "t", "public": "p",
        "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "zzz"}]}])
    expect_joint_error("无效目标阶段被拒", [sA, sC], [{
        "target_session_id": sC, "target_stage_id": "zzz", "title": "t", "public": "p",
        "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]}])
    expect_joint_error("无效推送角色被拒", [sA, sC], [{
        "target_session_id": sC, "target_stage_id": "s2", "title": "t", "public": "",
        "views": {"zz": "x"},
        "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]}])
    expect_joint_error("空注入内容被拒", [sA, sC], [{
        "target_session_id": sC, "target_stage_id": "s2", "title": "t", "public": "",
        "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]}])
    expect_joint_error("重复分支被拒", [sA, sC], [{
        "target_session_id": sC, "target_stage_id": "s2", "title": "t", "public": "p",
        "branches": [
            {"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"},
            {"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]}])
    expect_joint_error("有环依赖被拒", [sA, sB], [
        {"target_session_id": sB, "target_stage_id": "s1", "title": "t1", "public": "p",
         "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]},
        {"target_session_id": sA, "target_stage_id": "s1", "title": "t2", "public": "p",
         "branches": [{"source_session_id": sB, "source_stage_id": "s1", "outcome": "s2"}]}])
    # acyclic cross shape must pass
    validate_plan("联演", [sA, sB], [
        {"target_session_id": sB, "target_stage_id": "s2", "title": "t1", "public": "p",
         "branches": [{"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"}]},
        {"target_session_id": sA, "target_stage_id": "s2", "title": "t2", "public": "p",
         "branches": [{"source_session_id": sB, "source_stage_id": "s1", "outcome": "s2"}]}],
        defs, vers)
    check("无环跨会话依赖通过校验", True)

    # real plan: (A/s1 -> s2) AND (B/s2 -> @end) => inject C/s2
    plan = [{
        "target_session_id": sC, "target_stage_id": "s2", "title": "联演通报",
        "public": "公开注入", "views": {"a": "甲的联演密报"},
        "branches": [
            {"source_session_id": sA, "source_stage_id": "s1", "outcome": "s2"},
            {"source_session_id": sB, "source_stage_id": "s2", "outcome": "@end"},
        ],
    }]
    jdoc = await jo.create_joint("首场联演", [sA, sB, sC], plan, defs, vers)
    jid = jdoc["id"]
    rid = jdoc["rules"][0]["id"]
    check("联演创建", jid.startswith("j") and rid.startswith("jr_"))

    for s in (sA, sB, sC):
        await eng.start(s)

    # A meets its branch only -> barrier stays closed (no early unlock).
    # DUEL s1 is decided by both a and b; submit both to seal -> default s2.
    await eng.submit(sA, "a", "s1", "x")
    await eng.submit(sA, "b", "s1", "p")
    await jo.on_session_progress(sA)
    _, _, stC = await eng.load(sC)
    check("单个分支满足不提前解锁", not stC.injections.get("s2"))
    status = await jo.joint_status(jid)
    r0 = next(r for r in status["rules"] if r["id"] == rid)
    check("监控：1/2 分支等待且给出阻塞原因",
          r0["state"] == "waiting"
          and sum(1 for b in r0["branches"] if b["state"] == "matched") == 1
          and any(b["block_reason"] for b in r0["branches"] if b["state"] == "waiting"))

    # B runs to true @end (s1 -> s2 -> submit k -> end)
    await eng.settle(sB, "host")
    await jo.on_session_progress(sB)
    await eng.submit(sB, "a", "s2", "k")
    await jo.on_session_progress(sB)
    _, _, stB = await eng.load(sB)
    check("B 到达 @end", stB.status == "ended")
    _, _, stC = await eng.load(sC)
    check("屏障全满足但目标阶段未到，仍不投递", not stC.injections.get("s2"))

    # C advances into s2 -> convergence pass delivers exactly once
    await eng.settle(sC, "host")
    await jo.on_session_progress(sC)
    _, _, stC = await eng.load(sC)
    injs = stC.injections.get("s2", [])
    check("目标阶段到达后投递一次", len(injs) == 1, str(len(injs)))
    if injs:
        inj = injs[0]
        check("注入内容正确", inj.title == "联演通报" and inj.public == "公开注入")
        check("注入携带来源分支", len(inj.sources) == 2)
        check("注入非迟到", inj.late is False)

    # snapshot permission trim: role a sees its own block, role b sees none
    _, dC, _ = await eng.load(sC)
    snap_a = None
    # participant a exists only if joined; emulate via snapshots builder
    from app.snapshots import build_snapshot
    sessC, _, stC2 = await eng.load(sC)
    block = next(b for b in build_snapshot(sessC, dC, stC2, ("role", "a"))["history"]
                 if b["stage_id"] == "s2")
    check("角色 a 快照含本人密报",
          block["injections"][0]["views"].get("a") == "甲的联演密报")
    block_b = next(b for b in build_snapshot(sessC, dC, stC2, ("role", "b"))["history"]
                   if b["stage_id"] == "s2")
    check("角色 b 快照不含他人密报", not block_b["injections"][0]["views"])
    snap_host = build_snapshot(sessC, dC, stC2, ("host", None))
    hblock = next(b for b in snap_host["history"] if b["stage_id"] == "s2")
    check("主持人快照含密报", hblock["injections"][0]["views"].get("a") == "甲的联演密报")

    # duplicate / out-of-order safety
    await jo.reconcile_joint(jid)
    await jo.reconcile_joint(jid)
    _, _, stC = await eng.load(sC)
    check("重复协调绝不重复投递", len(stC.injections.get("s2", [])) == 1)
    drows = await eng.db.all(
        "SELECT version, late FROM joint_deliveries WHERE rule_id=?", (rid,))
    check("投递台账恰一行", len(drows) == 1 and drows[0]["late"] == 0)
    # a re-seal signal (replayed progress) doesn't add a second hit per branch
    await jo.on_session_progress(sA)
    hits = await eng.db.all(
        "SELECT COUNT(*) AS c FROM joint_hits WHERE rule_id=?", (rid,))
    check("重复分支信号不重复记账", hits[0]["c"] == 2)

    # the injection is a normal event version in C only; A/B logs untouched
    evA = [r["type"] for r in await eng.db.all(
        "SELECT type FROM events WHERE session_id=?", (sA,))]
    check("上游 A 事件记录不含注入", "joint_injection" not in evA)
    evC = await fetch_events(eng.db, sC)
    check("目标 C 事件记录含注入", any(e["type"] == "joint_injection" for e in evC))
    check_log("注入后日志版本分配仍一致", evC)
    inj_ev = next(e for e in evC if e["type"] == "joint_injection")
    check("注入独占一个新版本", all(
        e["version"] != inj_ev["version"] or e["seq"] == inj_ev["seq"] for e in evC))

    # delivered rule cannot be edited or deleted via the plan API
    try:
        await jo.delete_joint(jid)
        check("已投递联演不能删除", False)
    except JointError as exc:
        check("已投递联演不能删除（409）", exc.status == 409)
    try:
        await jo.update_joint(
            jid, "改名", [sA, sB, sC],
            [{**plan[0], "id": rid, "title": "改过的标题"}], defs, vers)
        check("已投递规则不能修改", False)
    except JointError as exc:
        check("已投递规则不能修改（409）", exc.status == 409)

    # late delivery: target stage already sealed when barrier releases
    sD = await eng.create_session("duel")
    sE = await eng.create_session("duel")
    plan_late = [{
        "target_session_id": sE, "target_stage_id": "s1", "title": "迟到注入",
        "public": "晚到",
        "branches": [{"source_session_id": sD, "source_stage_id": "s1", "outcome": "s2"}],
    }]
    j2 = await jo.create_joint("迟到联演", [sD, sE], plan_late,
                               {sD: duel_def, sE: duel_def}, {sD: 1, sE: 1})
    await eng.start(sD)
    await eng.start(sE)
    await eng.settle(sE, "host")   # E's s1 sealed before D satisfies
    await jo.on_session_progress(sE)
    await eng.settle(sD, "host")   # D s1 -> s2 (default); barrier releases
    changed = await jo.on_session_progress(sD)
    check("迟到注入触发联演变更", j2["id"] in changed)
    _, _, stE = await eng.load(sE)
    late_injs = stE.injections.get("s1", [])
    check("迟到场景一次性补投", len(late_injs) == 1 and late_injs[0].late is True,
          str([(i.title, i.late) for i in late_injs]))
    await jo.reconcile_joint(j2["id"])
    _, _, stE = await eng.load(sE)
    check("迟到补投不重复", len(stE.injections.get("s1", [])) == 1)

    # editing an UNDELIVERED rule keeps a stable id when body unchanged;
    # adding a new member session is allowed and does not touch deliveries
    sF = await eng.create_session("duel")
    upd = await jo.update_joint(
        j2["id"], "迟到联演（改）", [sD, sE, sF],
        [{**plan_late[0], "id": j2["rules"][0]["id"]}],
        {sD: duel_def, sE: duel_def, sF: duel_def},
        {sD: 1, sE: 1, sF: 1})
    check("规则保持稳定 id", upd["rules"][0]["id"] == j2["rules"][0]["id"])
    check("已投递记录在改名后仍存在",
          (await eng.db.one("SELECT 1 FROM joint_deliveries WHERE joint_id=?",
                            (j2["id"],))) is not None)


# --------------------------------------------------------------------------- #
async def engine_tests():
    db = Database(str(Path(_TMP) / "t.db"))
    await db.connect()
    eng = Engine(db)
    for dct in (DUEL, FAST, TIMED2):
        d = parse_definition(yaml.safe_load(yaml.safe_dump(dct)))
        # Unknown drills seed as an immediately released v1.
        await eng.seed_from_file(d, yaml.safe_dump(dct, allow_unicode=True, sort_keys=False))
    try:
        await test_join_then_start(eng)
        await test_auto_settle(eng)
        await test_deadline_race(eng)
        await test_stale_deadline_timer(eng)
        await test_timer_bookkeeping()
        await test_concurrent_idempotency(eng)
        await test_replay(eng)
        await test_end_session(eng)
        await test_script_versions(eng)
        await test_joint_orchestration(eng)
    finally:
        await db.close()


def test_http_joint_api():
    print("[13b] HTTP 联合演练 API")
    with TestClient(app) as client:
        # two fresh sessions from the released wsdrill (2 stages, roles a/b)
        sids = []
        for _ in range(3):
            r = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST)
            check("创建会话", r.status_code == 201)
            sids.append(r.json()["session_id"])
        s1, s2, s3 = sids

        # outline endpoint (host only)
        check("outline 需要密钥", client.get(f"/api/sessions/{s1}/outline").status_code == 401)
        ol = client.get(f"/api/sessions/{s1}/outline", headers=HOST).json()
        check("outline 按绑定版本给出阶段/结局",
              ol["drill"]["version"] == 1
              and {s["id"] for s in ol["stages"]} == {"s1", "s2"}
              and "s2" in ol["stages"][0]["outcomes"])

        # auth on joint endpoints
        check("联演列表需密钥", client.get("/api/joints").status_code == 401)

        # invalid plan: cycle is rejected with 400
        cyc = {"name": "环", "session_ids": [s1, s2], "rules": [
            {"target_session_id": s2, "target_stage_id": "s1", "title": "r1", "public": "p",
             "branches": [{"source_session_id": s1, "source_stage_id": "s1", "outcome": "s2"}]},
            {"target_session_id": s1, "target_stage_id": "s1", "title": "r2", "public": "p",
             "branches": [{"source_session_id": s2, "source_stage_id": "s1", "outcome": "s2"}]},
        ]}
        r = client.post("/api/joints", json=cyc, headers=HOST)
        check("有环编排保存 400", r.status_code == 400 and "环" in r.json()["detail"])

        # invalid outcome
        bad = {"name": "bad", "session_ids": [s1, s3], "rules": [
            {"target_session_id": s3, "target_stage_id": "s2", "title": "r", "public": "p",
             "branches": [{"source_session_id": s1, "source_stage_id": "s1", "outcome": "nope"}]}]}
        check("无效结局保存 400", client.post("/api/joints", json=bad, headers=HOST).status_code == 400)

        # valid plan: s1/s1->s2 AND s2/s1->s2  => inject s3/s2
        plan = {"name": "HTTP 联演", "session_ids": [s1, s2, s3], "rules": [
            {"target_session_id": s3, "target_stage_id": "s2", "title": "联动通报",
             "public": "请协同处置", "views": {"a": "甲专属联动信息"},
             "branches": [
                 {"source_session_id": s1, "source_stage_id": "s1", "outcome": "s2"},
                 {"source_session_id": s2, "source_stage_id": "s1", "outcome": "s2"}]}]}
        r = client.post("/api/joints", json=plan, headers=HOST)
        check("保存联演 201", r.status_code == 201, r.text)
        j = r.json()
        jid = j["id"]
        check("响应即监控状态", j["summary"]["rules"] == 1 and j["summary"]["waiting"] == 1)
        rid = j["rules"][0]["id"]

        # list + get
        check("联演列表含新联演", any(x["id"] == jid for x in client.get("/api/joints", headers=HOST).json()))
        check("取不存在联演 404", client.get("/api/joints/jnope", headers=HOST).status_code == 404)

        # drive via fresh sessions below; just verify the plan is listed.
        check("联演列表含新联演", any(x["id"] == jid for x in client.get("/api/joints", headers=HOST).json()))
        check("取不存在联演 404", client.get("/api/joints/jnope", headers=HOST).status_code == 404)

        # drive a second joint through HTTP + WebSocket: s4 & s5 satisfy
        # branches, s6 receives the injection once it reaches stage s2.
        s4 = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST).json()["session_id"]
        s5 = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST).json()["session_id"]
        s6 = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST).json()["session_id"]
        plan2 = {"name": "WS 联演", "session_ids": [s4, s5, s6], "rules": [
            {"target_session_id": s6, "target_stage_id": "s2", "title": "联动",
             "public": "公开联动", "views": {"a": "甲联动密报"},
             "branches": [
                 {"source_session_id": s4, "source_stage_id": "s1", "outcome": "s2"},
                 {"source_session_id": s5, "source_stage_id": "s1", "outcome": "s2"}]}]}
        r = client.post("/api/joints", json=plan2, headers=HOST)
        check("第二联演保存 201", r.status_code == 201)
        jid2 = r.json()["id"]
        rid2 = r.json()["rules"][0]["id"]

        tokens = {}
        for s in (s4, s5, s6):
            tokens[s] = {
                "a": client.post(f"/api/sessions/{s}/join",
                                 json={"invite": "AAA", "name": "甲"}).json()["token"],
                "b": client.post(f"/api/sessions/{s}/join",
                                 json={"invite": "BBB", "name": "乙"}).json()["token"],
            }

        with ExitStack() as stack:
            hosts = {s: stack.enter_context(client.websocket_connect(f"/ws/{s}?host=test-key"))
                     for s in (s4, s5, s6)}
            jws = stack.enter_context(client.websocket_connect(f"/ws/joint/{jid2}?host=test-key"))
            recv_until(jws, lambda m: m.get("type") == "joint_snapshot", "joint init")
            bad_auth = stack.enter_context(client.websocket_connect(f"/ws/joint/{jid2}"))
            check("联演 WS 无密钥被拒", bad_auth.receive_json()["type"] == "error")

            for s in (s4, s5, s6):
                hosts[s].send_json({"type": "start"})
                recv_until(hosts[s], lambda m: is_snapshot(m, status="running"), f"{s} running")

            def submit(s, role, cid):
                ws = stack.enter_context(
                    client.websocket_connect(f"/ws/{s}?token={tokens[s][role]}"))
                recv_until(ws, lambda m: m.get("type") == "snapshot", "snap")
                ws.send_json({"type": "submit", "choice_id": cid, "client_stage": "s1"})
                return recv_until(ws, lambda m: m.get("type") == "submit_result", "result")

            # s4 branch first: barrier still open
            r4a = submit(s4, "a", "x"); check("s4 甲提交", r4a["accepted"])
            r4b = submit(s4, "b", "p")
            check("s4 结算到 s2", r4b["settled"] and r4b["next_stage"] == "s2")
            jmsg = recv_until(jws, lambda m: m.get("type") == "joint_snapshot"
                              and sum(1 for b in m["joint"]["rules"][0]["branches"]
                                      if b["state"] == "matched") == 1, "1 branch matched")
            check("联演 WS 推送：仍等待", jmsg["joint"]["rules"][0]["state"] == "waiting")

            # s5 satisfies the second branch; target s6 still on s1 -> waiting
            submit(s5, "a", "x")
            r5b = submit(s5, "b", "p")
            check("s5 结算到 s2", r5b["next_stage"] == "s2")
            jmsg = recv_until(jws, lambda m: m.get("type") == "joint_snapshot"
                              and all(b["state"] == "matched"
                                      for b in m["joint"]["rules"][0]["branches"]),
                              "both matched")
            check("全分支满足但目标未到，状态非已投递",
                  jmsg["joint"]["rules"][0]["state"] in ("ready", "waiting"))

            # host advances s6 s1 -> s2; monitor shows delivered and role
            # 'a' of s6 receives the private injection in its snapshot.
            hosts[s6].send_json({"type": "settle"})
            jmsg = recv_until(jws, lambda m: m.get("type") == "joint_snapshot"
                              and m["joint"]["rules"][0]["state"] == "delivered",
                              "delivered")
            check("联演 WS 推送：已投递",
                  jmsg["joint"]["rules"][0]["delivery"]["version"] >= 1
                  and jmsg["joint"]["rules"][0]["delivery"]["late"] is False)

            a6 = stack.enter_context(
                client.websocket_connect(f"/ws/{s6}?token={tokens[s6]['a']}"))
            snap = recv_until(a6, lambda m: m.get("type") == "snapshot", "a6 snap")["snapshot"]
            s2block = next(b for b in snap["history"] if b["stage_id"] == "s2")
            check("目标角色 a 收到专属密报",
                  s2block["injections"][0]["views"].get("a") == "甲联动密报"
                  and s2block["injections"][0]["public"] == "公开联动")
            b6 = stack.enter_context(
                client.websocket_connect(f"/ws/{s6}?token={tokens[s6]['b']}"))
            snapb = recv_until(b6, lambda m: m.get("type") == "snapshot", "b6 snap")["snapshot"]
            b2 = next(b for b in snapb["history"] if b["stage_id"] == "s2")
            check("目标角色 b 只见公开注入",
                  b2["injections"][0]["public"] == "公开联动" and not b2["injections"][0]["views"])

        # replay of s6 contains the injection event (host) and is trimmed for b
        rp = client.get(f"/api/sessions/{s6}/replay", headers=HOST).json()
        check("回放事件日志含联演注入",
              any(c["type"] == "joint_injection" for f in rp["frames"] for c in f["changes"]))
        rpb = client.get(f"/api/sessions/{s6}/replay",
                         params={"token": tokens[s6]["b"]}).json()
        dumpb = json.dumps(rpb, ensure_ascii=False)
        check("参与者回放不含他人密报", "甲联动密报" not in dumpb)
        rpa = client.get(f"/api/sessions/{s6}/replay",
                         params={"token": tokens[s6]["a"]}).json()
        check("参与者回放含本人密报", "甲联动密报" in json.dumps(rpa, ensure_ascii=False))

        # records carry injections and permission trim
        rec_h = client.get(f"/api/sessions/{s6}/record", params={"fmt": "json"},
                           headers=HOST).json()
        s2rec = next(s for s in rec_h["stages"] if s["stage_id"] == "s2")
        check("主持人记录含联演注入（全角色视图）",
              s2rec["injections"] and s2rec["injections"][0]["views"].get("a") == "甲联动密报")
        rec_b = client.get(
            f"/api/sessions/{s6}/record?fmt=json&token={tokens[s6]['b']}").json()
        s2b = next(s for s in rec_b["stages"] if s["stage_id"] == "s2")
        check("个人记录注入仅公开文本", s2b["injections"][0]["public"] == "公开联动"
              and not s2b["injections"][0]["private"])
        md = client.get(f"/api/sessions/{s6}/record?fmt=md&token={tokens[s6]['a']}").text
        check("个人 Markdown 含联演注入段落", "联演注入" in md and "甲联动密报" in md)

        # cannot delete a joint that delivered
        check("删除已投递联演 409",
              client.delete(f"/api/joints/{jid2}", headers=HOST).status_code == 409)


def main():
    signal.alarm(240)  # watchdog: a hung websocket read must not block CI
    test_definitions()
    asyncio.run(engine_tests())
    asyncio.run(_migration_test())
    test_http_ws_e2e()
    test_http_version_api()
    test_http_joint_api()
    print(f"\n{'全部通过' if FAIL == 0 else '存在失败'}: {PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


async def _migration_test():
    await test_legacy_migration()


if __name__ == "__main__":
    sys.exit(main())
