"""Joint exercise orchestration ("联合演练编排").

A *joint* enrolls several otherwise independent drill sessions into one
exercise. Each joint rule describes a directed dependency:

    [ (source session A, stage s, outcome) AND (session B, stage t, outcome) … ]
        ──all satisfied──▶ append a ONE-SHOT joint injection to target
                           session X's stage Y, pushed per that session's
                           role permissions.

Validation (before save)
------------------------
* every enrolled session exists;
* every referenced stage exists in the session's *pinned* script version
  (each session keeps its own independent version, validated separately);
* every declared outcome is one the source stage can actually produce
  (its option ``next`` / transition ``next`` / default_next targets, or the
  special ``@end`` when sealing the stage can end the drill);
* within one rule branches are distinct and never contradictory (the same
  (session, stage) cannot demand two different outcomes — the AND barrier
  could never release);
* dependencies only run BETWEEN distinct sessions (in-session flow already
  has options/transitions; intra-session links are rejected);
* the (session, stage) dependency graph is acyclic (DFS cycle detection).

Runtime guarantees
------------------
* joint_hits (rule_id, idx) is the satisfaction ledger: a repeated or
  out-of-order seal signal for the same branch never double-counts and a
  barrier never releases early — a rule fires only when EVERY branch has a
  hit;
* joint_deliveries rule_id primary key is the exactly-once guard, written
  inside the target session's atomic event commit (see
  Engine.deliver_joint_injection), so duplicate/unordered triggers can never
  inject twice;
* injections land in the target session's own event log as a new monotonic
  version, so independent script versions and per-session event records are
  preserved untouched;
* a target stage already sealed still receives the injection once, flagged
  ``late``; a stage never reached blocks delivery and the monitor reports
  why.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from .db import Database, dumps
from .definitions import DrillDef, Stage
from .engine import Engine, EngineError, new_id, now_ts

END_OUTCOME = "@end"


class JointError(EngineError):
    """Validation / state error on a joint plan."""


@dataclass(frozen=True)
class Branch:
    source_session_id: str
    source_stage_id: str
    outcome: str  # a stage id, or '@end'

    def key(self) -> tuple[str, str]:
        return (self.source_session_id, self.source_stage_id)


@dataclass(frozen=True)
class JointRule:
    id: str
    target_session_id: str
    target_stage_id: str
    title: str
    public: str
    views: dict[str, str]
    branches: tuple[Branch, ...]


# --------------------------------------------------------------------------- #
# pure validation
# --------------------------------------------------------------------------- #
def stage_outcomes(stage: Stage) -> set[str]:
    """Every outcome sealing this stage can produce.

    A union of all reachable ``next`` targets (option next, conditional
    transition next and default_next) plus ``@end`` whenever any path may
    end the drill (missing/None target). The set is deliberately a
    superset: a declared outcome is accepted when it is among the stage's
    possible sealing results, never when it cannot happen.
    """
    out: set[str] = set()
    for olist in stage.options.values():
        for o in olist:
            out.add(o.next if o.next is not None else END_OUTCOME)
    for t in stage.transitions:
        out.add(t.next if t.next is not None else END_OUTCOME)
    out.add(stage.default_next if stage.default_next is not None else END_OUTCOME)
    return out


def _parse_branches(raw: list[dict[str, Any]], ctx: str) -> tuple[Branch, ...]:
    if not isinstance(raw, list) or not raw:
        raise JointError(f"{ctx}：至少需要一个前置分支（多个分支须全部满足才释放）")
    branches: list[Branch] = []
    seen: set[tuple[str, str]] = set()
    for i, b in enumerate(raw):
        bctx = f"{ctx} 前置分支 {i + 1}"
        if not isinstance(b, dict):
            raise JointError(f"{bctx}：格式错误")
        sid = str(b.get("source_session_id") or "").strip()
        stage = str(b.get("source_stage_id") or "").strip()
        outcome = str(b.get("outcome") or "").strip()
        if not sid:
            raise JointError(f"{bctx}：缺少上游会话")
        if not stage:
            raise JointError(f"{bctx}：缺少上游阶段")
        if not outcome:
            raise JointError(f"{bctx}：缺少指定结局")
        br = Branch(sid, stage, outcome)
        if br.key() in seen:
            raise JointError(f"{bctx}：与另一个分支指向同一会话阶段（重复分支）")
        seen.add(br.key())
        branches.append(br)
    # Contradictory duplicates (same key demanding a different outcome) are
    # already caught by the `seen` check above; outcomes themselves are
    # validated against the pinned definitions in validate_plan.
    return tuple(branches)


def _parse_rule(raw: dict[str, Any], ctx: str) -> tuple[str, JointRule]:
    if not isinstance(raw, dict):
        raise JointError(f"{ctx}：规则格式错误")
    rid = str(raw.get("id") or "").strip()  # assigned by the server on create
    target_session = str(raw.get("target_session_id") or "").strip()
    target_stage = str(raw.get("target_stage_id") or "").strip()
    title = str(raw.get("title") or "").strip()
    public = str(raw.get("public") or "").strip()
    views_raw = raw.get("views") or {}
    if not isinstance(views_raw, dict):
        raise JointError(f"{ctx}：按角色推送内容必须是角色→文本的映射")
    views = {str(k): str(v) for k, v in views_raw.items() if str(v).strip()}
    if not target_session:
        raise JointError(f"{ctx}：缺少目标会话")
    if not target_stage:
        raise JointError(f"{ctx}：缺少目标阶段")
    if not title:
        raise JointError(f"{ctx}：缺少注入标题")
    if not public and not views:
        raise JointError(f"{ctx}：联演注入至少要有公开文本或某角色的专属文本")
    branches = _parse_branches(raw.get("branches") or [], ctx)
    return rid, JointRule(
        id=rid,
        target_session_id=target_session,
        target_stage_id=target_stage,
        title=title,
        public=public,
        views=views,
        branches=branches,
    )


def validate_plan(
    name: str,
    session_ids: list[str],
    raw_rules: list[dict[str, Any]],
    defs: dict[str, DrillDef],
    versions: dict[str, int] | None = None,
) -> tuple[list[JointRule], list[str]]:
    """Validate a joint create/update payload against pinned definitions.

    ``defs`` maps every enrolled session id to the exact immutable script
    version that session is pinned to; ``versions`` carries the version
    numbers for readable error messages. Returns the parsed rules and the
    de-duplicated, order-preserving session membership. Raises JointError on
    the first problem with a Chinese, human-readable reason.
    """
    versions = versions or {}
    if not name.strip():
        raise JointError("联演名称不能为空")
    members: list[str] = []
    for sid in session_ids:
        sid = str(sid or "").strip()
        if not sid:
            continue
        if sid not in defs:
            raise JointError(f"会话 {sid} 不存在，无法编入联演")
        if sid not in members:
            members.append(sid)
    if len(members) < 2:
        raise JointError("联合演练至少需要编入两个独立会话")

    if not isinstance(raw_rules, list) or not raw_rules:
        raise JointError("至少需要配置一条联演注入规则")

    def vlabel(sid: str) -> str:
        return f"v{versions[sid]}" if sid in versions else "?"

    rules: list[JointRule] = []
    for i, raw in enumerate(raw_rules):
        _, rule = _parse_rule(raw, f"规则 {i + 1}")
        rctx = f"规则「{rule.title}」"

        # target session/stage valid in THAT session's pinned version
        if rule.target_session_id not in members:
            raise JointError(f"{rctx}：目标会话 {rule.target_session_id} 不在联演成员中")
        td = defs[rule.target_session_id]
        if rule.target_stage_id not in td.stages:
            raise JointError(
                f"{rctx}：目标会话的阶段「{rule.target_stage_id}」不存在"
                f"（该会话绑定剧本 {vlabel(rule.target_session_id)}）"
            )
        for rid_role, text in rule.views.items():
            if rid_role not in td.roles:
                raise JointError(
                    f"{rctx}：推送角色「{rid_role}」在目标会话的剧本中不存在"
                )
            if not text.strip():
                raise JointError(f"{rctx}：角色「{rid_role}」的推送文本为空")

        # branches: cross-session only; stages/outcomes valid upstream
        for b in rule.branches:
            if b.source_session_id not in members:
                raise JointError(f"{rctx}：前置会话 {b.source_session_id} 不在联演成员中")
            if b.source_session_id == rule.target_session_id:
                raise JointError(
                    f"{rctx}：依赖只能编排不同会话之间的阶段；"
                    "同一会话内的流转请用剧本选项/条件分支表达"
                )
            sd = defs[b.source_session_id]
            if b.source_stage_id not in sd.stages:
                raise JointError(
                    f"{rctx}：上游会话的阶段「{b.source_stage_id}」不存在"
                    f"（该会话绑定剧本 {vlabel(b.source_session_id)}）"
                )
            possible = stage_outcomes(sd.stages[b.source_stage_id])
            if b.outcome not in possible:
                raise JointError(
                    f"{rctx}：上游阶段「{b.source_stage_id}」不可能产生结局"
                    f"「{b.outcome}」（可选结局：{', '.join(sorted(possible))}）"
                )
        rules.append(rule)

    _assert_dag(rules)
    return rules, members


def _assert_dag(rules: list[JointRule]) -> None:
    """Directed graph over (session, stage) nodes: every branch node points
    at the rule's target node. Edges MUST run between distinct sessions and
    the whole graph MUST be acyclic. Iterative DFS with white/gray/black
    coloring; the first back edge is reported as a readable cycle.
    """
    nodes: set[tuple[str, str]] = set()
    adj: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for r in rules:
        target = (r.target_session_id, r.target_stage_id)
        nodes.add(target)
        for b in r.branches:
            src = (b.source_session_id, b.source_stage_id)
            nodes.add(src)
            adj.setdefault(src, set()).add(target)

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[tuple[str, str], int] = {n: WHITE for n in nodes}

    def dfs(start: tuple[str, str]) -> None:
        # explicit stack: (node, iterator over successors)
        work: list[tuple[tuple[str, str], Any]] = [(start, iter(sorted(adj.get(start, ()))))]
        color[start] = GRAY
        trace = [start]
        while work:
            node, it = work[-1]
            nxt = next(it, None)
            if nxt is None:
                color[node] = BLACK
                work.pop()
                trace.pop()
                continue
            if color[nxt] == GRAY:
                cyc = trace[trace.index(nxt):] + [nxt]
                pretty = " → ".join(f"{sid}/{stg}" for sid, stg in cyc)
                raise JointError(f"依赖存在环，保存被拒绝（环：{pretty}）")
            if color[nxt] == WHITE:
                color[nxt] = GRAY
                trace.append(nxt)
                work.append((nxt, iter(sorted(adj.get(nxt, ())))))

    for n in sorted(nodes):
        if color[n] == WHITE:
            dfs(n)


# --------------------------------------------------------------------------- #
# orchestrator
# --------------------------------------------------------------------------- #
class JointOrchestrator:
    def __init__(self, db: Database, engine: Engine):
        self.db = db
        self.engine = engine
        self._locks: dict[str, asyncio.Lock] = {}

    def lock_for(self, joint_id: str) -> asyncio.Lock:
        lk = self._locks.get(joint_id)
        if lk is None:
            lk = asyncio.Lock()
            self._locks[joint_id] = lk
        return lk

    # ------------------------------------------------------------------ #
    # storage helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _rule_json(r: JointRule) -> dict[str, Any]:
        return {
            "id": r.id,
            "target_session_id": r.target_session_id,
            "target_stage_id": r.target_stage_id,
            "title": r.title,
            "public": r.public,
            "views": r.views,
            "branches": [
                {
                    "source_session_id": b.source_session_id,
                    "source_stage_id": b.source_stage_id,
                    "outcome": b.outcome,
                }
                for b in r.branches
            ],
        }

    async def _load_rules(self, joint_id: str) -> list[JointRule]:
        rows = await self.db.all(
            "SELECT id, target_session_id, target_stage_id, inject_json "
            "FROM joint_rules WHERE joint_id=? ORDER BY row_seq",
            (joint_id,),
        )
        out: list[JointRule] = []
        for r in rows:
            payload = json.loads(r["inject_json"])
            out.append(JointRule(
                id=r["id"],
                target_session_id=r["target_session_id"],
                target_stage_id=r["target_stage_id"],
                title=payload["title"],
                public=payload.get("public", ""),
                views=payload.get("views", {}),
                branches=tuple(
                    Branch(b["source_session_id"], b["source_stage_id"], b["outcome"])
                    for b in payload["branches"]
                ),
            ))
        return out

    async def list_joints(self) -> list[dict[str, Any]]:
        rows = await self.db.all(
            "SELECT id, name, created_at FROM joints ORDER BY rowid DESC"
        )
        return [dict(r) for r in rows]

    async def get_joint_row(self, joint_id: str) -> dict[str, Any] | None:
        row = await self.db.one(
            "SELECT id, name, created_at FROM joints WHERE id=?", (joint_id,)
        )
        return dict(row) if row else None

    async def get_joint(self, joint_id: str) -> dict[str, Any]:
        row = await self.get_joint_row(joint_id)
        if row is None:
            raise JointError("联演不存在", 404)
        sessions = [
            r["session_id"]
            for r in await self.db.all(
                "SELECT session_id FROM joint_sessions WHERE joint_id=? ORDER BY rowid",
                (joint_id,),
            )
        ]
        return {"id": row["id"], "name": row["name"], "created_at": row["created_at"],
                "session_ids": sessions, "rules": [self._rule_json(r)
                                                   for r in await self._load_rules(joint_id)]}

    async def _replace_plan(
        self,
        joint_id: str,
        name: str,
        members: list[str],
        rules: list[JointRule],
        *,
        existing: dict[str, JointRule] | None = None,
        now: float,
    ) -> None:
        """Wipe and rewrite a plan inside an open transaction.

        Stable rule ids: rules that come in with a server-issued id AND are
        unchanged (same parsed payload as a stored rule) keep their id, and
        therefore keep their hits/delivery ledger. Changed rules get fresh
        ids, so a brand-new barrier must be re-satisfied and an already
        delivered old rule can never silently mutate. Deleted rules drop
        their ledger rows only while undelivered (checked before calling).
        """
        existing = existing or {}
        used_old: set[str] = set()
        seq = 0
        kept: set[str] = set()
        for rule in rules:
            if rule.id and rule.id in existing and rule.id not in used_old:
                old = existing[rule.id]
                if _same_rule_body(old, rule):
                    used_old.add(rule.id)
                    kept.add(rule.id)
            seq += 1

        # assign fresh ids to non-matching rules
        final_rules: list[JointRule] = []
        for rule in rules:
            if rule.id in kept:
                final_rules.append(rule)
            else:
                final_rules.append(JointRule(
                    id=f"jr_{new_id(5)}", target_session_id=rule.target_session_id,
                    target_stage_id=rule.target_stage_id, title=rule.title,
                    public=rule.public, views=rule.views, branches=rule.branches,
                ))

        await self.db.db.execute(
            "UPDATE joints SET name=? WHERE id=?", (name, joint_id)
        )
        await self.db.db.execute(
            "DELETE FROM joint_sessions WHERE joint_id=?", (joint_id,)
        )
        for sid in members:
            await self.db.db.execute(
                "INSERT INTO joint_sessions(joint_id, session_id, added_at) VALUES(?,?,?)",
                (joint_id, sid, now),
            )

        old_ids = {r.id for r in (await self._load_rules(joint_id))}
        drop = old_ids - {r.id for r in final_rules}
        for oid in drop:
            await self.db.db.execute(
                "DELETE FROM joint_hits WHERE rule_id=?", (oid,)
            )
            await self.db.db.execute(
                "DELETE FROM joint_branches WHERE rule_id=?", (oid,)
            )
            await self.db.db.execute(
                "DELETE FROM joint_rules WHERE id=?", (oid,)
            )
        for seq, rule in enumerate(final_rules):
            payload = self._rule_json(rule)
            if rule.id in old_ids:
                await self.db.db.execute(
                    "UPDATE joint_rules SET target_session_id=?, target_stage_id=?, "
                    "inject_json=?, row_seq=? WHERE id=?",
                    (rule.target_session_id, rule.target_stage_id, dumps(payload), seq, rule.id),
                )
                await self.db.db.execute(
                    "DELETE FROM joint_branches WHERE rule_id=?", (rule.id,)
                )
            else:
                await self.db.db.execute(
                    "INSERT INTO joint_rules(id, joint_id, target_session_id, "
                    "target_stage_id, inject_json, row_seq) VALUES(?,?,?,?,?,?)",
                    (rule.id, joint_id, rule.target_session_id, rule.target_stage_id,
                     dumps(payload), seq),
                )
            for i, b in enumerate(rule.branches):
                await self.db.db.execute(
                    "INSERT INTO joint_branches(rule_id, idx, source_session_id, "
                    "source_stage_id, outcome) VALUES(?,?,?,?,?)",
                    (rule.id, i, b.source_session_id, b.source_stage_id, b.outcome),
                )

    async def create_joint(
        self, name: str, session_ids: list[str], raw_rules: list[dict[str, Any]],
        defs: dict[str, DrillDef], versions: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        rules, members = validate_plan(name, session_ids, raw_rules, defs, versions)
        joint_id = f"j{new_id(4)}"
        now = now_ts()
        await self.db.begin()
        try:
            await self.db.db.execute(
                "INSERT INTO joints(id, name, created_at) VALUES(?,?,?)",
                (joint_id, name.strip(), now),
            )
            # New rules carry empty ids; _replace_plan assigns jr_* ids.
            await self._replace_plan(
                joint_id, name.strip(), members, rules, existing={}, now=now
            )
            await self.db.commit()
        except Exception:
            await self.db.rollback()
            raise
        # Backfill satisfaction for branches that already sealed, and fire
        # anything whose whole barrier was met before the joint existed.
        await self.reconcile_joint(joint_id)
        return await self.get_joint(joint_id)

    async def update_joint(
        self, joint_id: str, name: str, session_ids: list[str],
        raw_rules: list[dict[str, Any]], defs: dict[str, DrillDef],
        versions: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        async with self.lock_for(joint_id):
            current = await self.get_joint(joint_id)
            old_rules = {r.id: r for r in await self._load_rules(joint_id)}
            delivered = {
                r["rule_id"]
                for r in await self.db.all(
                    "SELECT rule_id FROM joint_deliveries WHERE joint_id=?", (joint_id,)
                )
            }
            if delivered:
                incoming_ids = {
                    str((r or {}).get("id") or "").strip() for r in raw_rules
                }
                missing = delivered - incoming_ids
                if missing:
                    raise JointError(
                        f"已投递的规则（{len(missing)} 条）不能删除；"
                        "其一次性注入已进入目标会话事件记录", 409
                    )

            rules, members = validate_plan(name, session_ids, raw_rules, defs, versions)
            # A delivered rule must remain byte-for-byte the same plan.
            for r in rules:
                if r.id in delivered and r.id in old_rules and not _same_rule_body(old_rules[r.id], r):
                    raise JointError(
                        f"规则「{r.title}」的注入已经投递，不能再修改其依赖或内容", 409
                    )
                if r.id in delivered and r.id not in old_rules:
                    raise JointError("规则标识与已投递记录不一致", 409)
            now = now_ts()
            await self.db.begin()
            try:
                await self._replace_plan(
                    joint_id, name.strip(), members, rules, existing=old_rules, now=now
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
        await self.reconcile_joint(joint_id)
        return await self.get_joint(joint_id)

    async def delete_joint(self, joint_id: str) -> None:
        async with self.lock_for(joint_id):
            if await self.get_joint_row(joint_id) is None:
                raise JointError("联演不存在", 404)
            fired = await self.db.one(
                "SELECT 1 FROM joint_deliveries WHERE joint_id=? LIMIT 1", (joint_id,)
            )
            if fired is not None:
                raise JointError(
                    "该联演已有注入投递到会话事件记录，不能删除；可改为清空未投递规则",
                    409,
                )
            await self.db.begin()
            try:
                rule_rows = await self.db.all(
                    "SELECT id FROM joint_rules WHERE joint_id=?", (joint_id,)
                )
                for r in rule_rows:
                    await self.db.db.execute(
                        "DELETE FROM joint_hits WHERE rule_id=?", (r["id"],)
                    )
                    await self.db.db.execute(
                        "DELETE FROM joint_branches WHERE rule_id=?", (r["id"],)
                    )
                await self.db.db.execute(
                    "DELETE FROM joint_rules WHERE joint_id=?", (joint_id,)
                )
                await self.db.db.execute(
                    "DELETE FROM joint_sessions WHERE joint_id=?", (joint_id,)
                )
                await self.db.db.execute("DELETE FROM joints WHERE id=?", (joint_id,))
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

    # ------------------------------------------------------------------ #
    # runtime: branch satisfaction + barrier release
    # ------------------------------------------------------------------ #
    async def _joints_of_session(self, session_id: str) -> list[str]:
        rows = await self.db.all(
            "SELECT joint_id FROM joint_sessions WHERE session_id=? ORDER BY rowid",
            (session_id,),
        )
        return [r["joint_id"] for r in rows]

    async def on_session_progress(self, session_id: str) -> set[str]:
        """Called after any commit that may have sealed a stage in
        *session_id* (submit/settle/end). Reconciles every joint this
        session belongs to. Returns the set of joint ids whose state
        (branch waits or deliveries) changed, so the caller can broadcast."""
        changed: set[str] = set()
        for jid in await self._joints_of_session(session_id):
            if await self.reconcile_joint(jid):
                changed.add(jid)
        return changed

    async def reconcile_joint(self, joint_id: str) -> bool:
        """One convergence pass:

        1. record hits for branches whose (session, stage) has sealed with
           the declared outcome — idempotent via the hits primary key, so
           repeated or out-of-order signals never double-count;
        2. for each undelivered rule whose branches ALL have hits, deliver
           the one-shot injection into the target session (exactly once via
           the delivery ledger committed inside its event transaction);
        3. target stages already open are picked up here; stages that sealed
           before release receive the injection flagged ``late``; stages
           never reached keep the rule waiting with an explicit reason.

        Returns True when any hit or delivery changed.
        """
        async with self.lock_for(joint_id):
            if await self.get_joint_row(joint_id) is None:
                return False
            rules = await self._load_rules(joint_id)
            if not rules:
                return False

            # Cache folded state + pinned def per involved session.
            cache: dict[str, tuple[Any, DrillDef, Any] | None] = {}

            async def state_of(sid: str):
                if sid not in cache:
                    try:
                        cache[sid] = await self.engine.load(sid)
                    except EngineError:
                        cache[sid] = None
                return cache[sid]

            changed = False

            # ---- phase 1: satisfaction ledger --------------------------------
            # Each hit is its own BEGIN IMMEDIATE transaction: the primary key
            # (rule_id, idx) makes a repeated/out-of-order seal signal an
            # idempotent no-op, and we never hold a write transaction across
            # another session's engine call.
            for rule in rules:
                delivered = await self.db.one(
                    "SELECT 1 FROM joint_deliveries WHERE rule_id=?", (rule.id,)
                )
                if delivered is not None:
                    continue
                for idx, b in enumerate(rule.branches):
                    loaded = await state_of(b.source_session_id)
                    if loaded is None:
                        continue
                    _, _, st = loaded
                    sealed = st.sealed.get(b.source_stage_id)
                    if sealed is None:
                        continue
                    if not _outcome_matches(sealed.next, b.outcome):
                        continue
                    await self.db.begin()
                    try:
                        await self.db.db.execute(
                            "INSERT INTO joint_hits(rule_id, idx, source_session_id, "
                            "source_stage_id, matched_version, at) VALUES(?,?,?,?,?,?)",
                            (rule.id, idx, b.source_session_id, b.source_stage_id,
                             sealed.version, sealed.at),
                        )
                        await self.db.commit()
                        changed = True
                    except Exception:
                        # Primary-key collision: branch already recorded.
                        await self.db.rollback()

            # ---- phase 2: barrier release ------------------------------------
            jrow = await self.get_joint_row(joint_id)
            for rule in rules:
                delivered = await self.db.one(
                    "SELECT 1 FROM joint_deliveries WHERE rule_id=?", (rule.id,)
                )
                if delivered is not None:
                    continue
                hits = await self.db.all(
                    "SELECT idx, source_session_id, source_stage_id, matched_version, at "
                    "FROM joint_hits WHERE rule_id=? ORDER BY idx",
                    (rule.id,),
                )
                if len(hits) < len(rule.branches):
                    continue  # barrier still open — never unlock early
                # Every prerequisite satisfied: fire exactly once.
                target_loaded = await state_of(rule.target_session_id)
                if target_loaded is None:
                    continue
                _, td, _ = target_loaded
                sources = [
                    {
                        "session_id": h["source_session_id"],
                        "stage_id": h["source_stage_id"],
                        "version": h["matched_version"],
                        "at": float(h["at"]),
                    }
                    for h in hits
                ]
                views = {
                    rid: text for rid, text in rule.views.items()
                    if rid in td.roles and text.strip()
                }

                async def mark(version: int, late: int, at: float, _rid=rule.id) -> None:
                    # Runs INSIDE the target session's engine transaction, so
                    # the delivery ledger row and the injection event commit
                    # atomically: the exactly-once guarantee.
                    await self.db.db.execute(
                        "INSERT INTO joint_deliveries(rule_id, joint_id, "
                        "target_session_id, target_stage_id, version, late, at) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (_rid, joint_id, rule.target_session_id,
                         rule.target_stage_id, version, late, at),
                    )

                info = await self.engine.deliver_joint_injection(
                    rule.target_session_id,
                    rule_id=rule.id, joint_id=joint_id, joint_name=jrow["name"],
                    stage_id=rule.target_stage_id, title=rule.title,
                    public=rule.public, views=views, sources=sources,
                    mark_delivered=mark,
                )
                if info.get("delivered"):
                    changed = True
                # stage_unreached: rule keeps waiting with a block reason;
                # duplicate: someone else delivered it; both are fine.
            return changed

    # ------------------------------------------------------------------ #
    # monitor snapshot for the host console
    # ------------------------------------------------------------------ #
    async def joint_status(self, joint_id: str) -> dict[str, Any]:
        """Live branch-wait view for the host monitor: per member session
        state, per rule branch state (waiting / matched) with the Chinese
        block reason, and delivery results."""
        doc = await self.get_joint(joint_id)
        sessions_out: dict[str, dict[str, Any]] = {}
        for sid in doc["session_ids"]:
            try:
                sess, d, st = await self.engine.load(sid)
                sessions_out[sid] = {
                    "id": sid,
                    "drill_id": d.id,
                    "drill_name": d.name,
                    "drill_version": sess["drill_version"],
                    "drill_sha256": sess["drill_sha256"],
                    "status": st.status,
                    "current_stage": st.current,
                    "stages": {
                        sid2: {
                            "title": stg.title,
                            "sealed": sid2 in st.sealed,
                            "current": st.current == sid2,
                            "reached": sid2 in st.order,
                            "outcome": st.sealed[sid2].next if sid2 in st.sealed else None,
                            "sealed_at": st.sealed[sid2].at if sid2 in st.sealed else None,
                        }
                        for sid2, stg in d.stages.items()
                    },
                }
            except EngineError:
                sessions_out[sid] = {"id": sid, "status": "missing"}

        rules_out: list[dict[str, Any]] = []
        for rdoc in doc["rules"]:
            rid = rdoc["id"]
            delivery_row = await self.db.one(
                "SELECT version, late, at FROM joint_deliveries WHERE rule_id=?", (rid,)
            )
            hits = {
                h["idx"]: dict(h)
                for h in await self.db.all(
                    "SELECT idx, matched_version, at FROM joint_hits WHERE rule_id=? ORDER BY idx",
                    (rid,),
                )
            }
            target = sessions_out.get(rdoc["target_session_id"], {})
            target_stages = target.get("stages", {})
            tstage = target_stages.get(rdoc["target_stage_id"])

            branches_out = []
            waiting = 0
            for idx, b in enumerate(rdoc["branches"]):
                hit = hits.get(idx)
                src = sessions_out.get(b["source_session_id"], {})
                sstage = (src.get("stages") or {}).get(b["source_stage_id"])
                if hit is not None:
                    state = "matched"
                    reason = ""
                elif not sstage:
                    state = "waiting"
                    reason = "上游阶段不存在"
                elif not sstage["reached"]:
                    state = "waiting"
                    waiting += 1
                    reason = "等待上游会话到达该阶段"
                elif sstage["sealed"]:
                    state = "waiting"
                    waiting += 1
                    got = sstage["outcome"]
                    got_label = "演练结束" if got is None else got
                    want_label = "演练结束" if b["outcome"] == END_OUTCOME else b["outcome"]
                    reason = f"阶段已结算但结局为「{got_label}」，未命中要求结局「{want_label}」"
                else:
                    state = "waiting"
                    waiting += 1
                    reason = "等待上游阶段结算到指定结局"
                branches_out.append({
                    "idx": idx,
                    "source_session_id": b["source_session_id"],
                    "source_stage_id": b["source_stage_id"],
                    "source_stage_title": (sstage or {}).get("title", b["source_stage_id"]),
                    "outcome": b["outcome"],
                    "outcome_label": "演练结束" if b["outcome"] == END_OUTCOME else b["outcome"],
                    "state": state,
                    "matched_version": hit["matched_version"] if hit else None,
                    "matched_at": float(hit["at"]) if hit else None,
                    "block_reason": reason,
                })

            if delivery_row is not None:
                rstate = "delivered"
                rreason = "已投递（迟到补投）" if delivery_row["late"] else "已投递"
            elif waiting or not hits:
                rstate = "waiting"
                if tstage is not None and not tstage["reached"]:
                    rreason = (
                        f"尚有 {len(branches_out) - len(hits)} 个分支未满足；"
                        f"且目标会话尚未进入阶段「{tstage['title']}」"
                    )
                else:
                    rreason = f"尚有 {len(branches_out) - len(hits)} 个前置分支未满足"
            else:
                rstate = "ready"
                rreason = "全部前置已满足，待目标阶段到达后投递"
                if tstage is not None and tstage["reached"] and not tstage["sealed"]:
                    rreason = "全部前置已满足，目标阶段已开放，即将投递"
                elif tstage is not None and tstage["sealed"]:
                    rstate = "delivering"
                    rreason = "目标阶段已结算，注入将作为迟到补投一次性追加"

            rules_out.append({
                **rdoc,
                "state": rstate,
                "state_reason": rreason,
                "target_stage_title": (tstage or {}).get("title", rdoc["target_stage_id"]),
                "target_reached": bool(tstage and tstage["reached"]),
                "target_sealed": bool(tstage and tstage["sealed"]),
                "branches": branches_out,
                "delivery": None if delivery_row is None else {
                    "version": delivery_row["version"],
                    "late": bool(delivery_row["late"]),
                    "at": float(delivery_row["at"]),
                },
            })

        delivered = sum(1 for r in rules_out if r["state"] == "delivered")
        waiting = sum(1 for r in rules_out if r["state"] == "waiting")
        return {
            "id": doc["id"],
            "name": doc["name"],
            "created_at": float(doc["created_at"]) if doc["created_at"] else None,
            "sessions": [sessions_out[s] for s in doc["session_ids"]],
            "rules": rules_out,
            "summary": {
                "rules": len(rules_out),
                "delivered": delivered,
                "waiting": waiting,
                "members": len(doc["session_ids"]),
            },
        }


def _outcome_matches(sealed_next: str | None, wanted: str) -> bool:
    """A sealed stage's outcome (``next`` target; None == drill ended)
    matches a declared branch outcome."""
    if wanted == END_OUTCOME:
        return sealed_next is None
    return sealed_next == wanted


def _same_rule_body(a: JointRule, b: JointRule) -> bool:
    return (
        a.target_session_id == b.target_session_id
        and a.target_stage_id == b.target_stage_id
        and a.title == b.title
        and a.public == b.public
        and a.views == b.views
        and a.branches == b.branches
    )
