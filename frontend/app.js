"use strict";
/* 多人应急演练平台 — 原生 JS 前端（无构建步骤）
 *
 * hash 路由：
 *   #/                         首页：主持人 / 参与者入口
 *   #/host                     主持人控制台（剧本、会话、联合演练）
 *   #/host/joints              联合演练编排：联演列表
 *   #/host/joint/<jid>         联演监控台（实时分支等待/阻塞原因）
 *   #/host/joint/<jid>/edit    联演编排编辑器（依赖 DAG、结局校验）
 *   #/host/session/<sid>       某会话的主持台（实时）
 *   #/host/drill/<drillId>     剧本版本管理
 *   #/join/<sid>[?invite=...]  参与者输入邀请码
 *   #/play/<sid>               参与者控制台（实时）
 *   #/replay/<sid>             确定性回放
 */

const app = document.getElementById("app");
const nav = document.getElementById("nav");
const navContext = document.getElementById("nav-context");
document.getElementById("nav-home").addEventListener("click", (e) => {
  e.preventDefault();
  location.hash = "#/";
});

// --------------------------------------------------------------------------- //
// utils
// --------------------------------------------------------------------------- //
const HOST_KEY = () => localStorage.getItem("host_key") || "";
const credKey = (sid) => `pt_${sid}`;
const getCred = (sid) => {
  try { return JSON.parse(localStorage.getItem(credKey(sid)) || "null"); }
  catch { return null; }
};
const setCred = (sid, c) => localStorage.setItem(credKey(sid), JSON.stringify(c));

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );

function toast(msg, kind = "") {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.className = `toast ${kind}`;
  clearTimeout(toast._tm);
  toast._tm = setTimeout(() => t.classList.add("hidden"), 3200);
}

function el(html) {
  const wrap = document.createElement("div");
  wrap.innerHTML = html.trim();
  return wrap.firstElementChild;
}

async function api(path, opts = {}) {
  const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
  if (opts.host) headers["X-Host-Key"] = HOST_KEY();
  if (opts.token) headers["Authorization"] = `Bearer ${opts.token}`;
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (!res.ok) {
    const err = new Error((data && data.detail) || `请求失败 (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return data;
}

function copyText(text) {
  navigator.clipboard?.writeText(text).then(
    () => toast("已复制到剪贴板", "ok"),
    () => toast("复制失败")
  );
}

function fmtTime(ts) {
  if (ts == null) return "—";
  return new Date(ts * 1000).toLocaleString("zh-CN", { hour12: false });
}
function fmtClock(ts) {
  if (ts == null) return "--:--:--";
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
}
function roleName(snap, rid) {
  const r = (snap.roles || []).find((x) => x.id === rid);
  if (r) return r.name;
  const p = (snap.participants || []).find((x) => x.role_id === rid);
  return p ? p.role_name : rid;
}
function statusChip(status) {
  return ({
    created: ["未开始", "warn"],
    running: ["进行中", "ok"],
    ended: ["已结束", "danger"],
  }[status] || [status, ""]);
}

// --------------------------------------------------------------------------- //
// live WebSocket with auto reconnect
// --------------------------------------------------------------------------- //
class Live {
  constructor(sessionId, viewer, handlers) {
    this.sessionId = sessionId;
    this.viewer = viewer;             // {host:'key'} or {token:'...'}
    this.onSnapshot = handlers.onSnapshot || (() => {});
    this.onResult = handlers.onResult || (() => {});
    this.onStatus = handlers.onStatus || (() => {});
    this.closedByUser = false;
    this.retry = 0;
    this.serverOffset = 0;
    this.connect();
  }
  url() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const q = this.viewer.host
      ? `host=${encodeURIComponent(this.viewer.host)}`
      : `token=${encodeURIComponent(this.viewer.token)}`;
    return `${proto}://${location.host}/ws/${this.sessionId}?${q}`;
  }
  connect() {
    const ws = new WebSocket(this.url());
    this.ws = ws;
    this.onStatus("connecting");
    ws.onopen = () => this.onStatus("open");
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "snapshot") {
        this.retry = 0;
        this.serverOffset = (msg.snapshot.server_time || 0) - Date.now() / 1000;
        this.onSnapshot(msg.snapshot);
      } else if (msg.type === "pong") {
        this.serverOffset = msg.server_time - Date.now() / 1000;
      } else if (msg.type === "submit_result") {
        this.onResult(msg);
      } else if (msg.type === "error") {
        toast(msg.detail || "操作被拒绝", "error");
        if (msg.code === "auth") this.close();
      }
    };
    ws.onclose = () => {
      if (this.closedByUser) return;
      this.retry = Math.min(this.retry + 1, 6);
      const delay = Math.min(500 * 2 ** this.retry, 10000);
      this.onStatus("reconnecting", delay);
      setTimeout(() => { if (!this.closedByUser) this.connect(); }, delay);
    };
  }
  send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
      return true;
    }
    toast("连接中断，正在重连…", "error");
    return false;
  }
  now() { return Date.now() / 1000 + this.serverOffset; }
  close() { this.closedByUser = true; try { this.ws.close(); } catch {} }
}

// --------------------------------------------------------------------------- //
// router
// --------------------------------------------------------------------------- //
let intervals = [];
let liveHandle = null;
function clearTimers() {
  intervals.forEach(clearInterval);
  intervals = [];
}
function teardownLive() {
  if (liveHandle) { liveHandle.close(); liveHandle = null; }
}

function route() {
  teardownLive();
  clearTimers();
  const raw = (location.hash || "#/").replace(/^#\/?/, "");
  const [path, query] = raw.split("?");
  const params = new URLSearchParams(query || "");
  const parts = path.split("/").filter(Boolean);
  nav.classList.toggle("hidden", parts.length === 0);
  navContext.textContent = "";

  if (parts[0] === "host" && parts[1] === "session" && parts[2]) {
    navContext.textContent = `主持人 · 会话 ${parts[2]}`;
    return hostSessionPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "joint" && parts[2] && parts[3] === "edit") {
    navContext.textContent = `主持人 · 编排联演 ${parts[2]}`;
    return jointEditPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "joint" && parts[2]) {
    navContext.textContent = `主持人 · 联演监控 ${parts[2]}`;
    return jointMonitorPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "drill" && parts[2]) {
    navContext.textContent = `主持人 · 剧本版本 ${parts[2]}`;
    return drillVersionPage(parts[2]);
  }
  if (parts[0] === "host") { navContext.textContent = "主持人控制台"; return hostPage(); }
  if (parts[0] === "join" && parts[1]) return joinPage(parts[1], params);
  if (parts[0] === "play" && parts[1]) return playPage(parts[1]);
  if (parts[0] === "replay" && parts[1]) return replayPage(parts[1]);
  return homePage();
}
window.addEventListener("hashchange", route);

// --------------------------------------------------------------------------- //
// home
// --------------------------------------------------------------------------- //
function homePage() {
  app.innerHTML = "";
  app.appendChild(el(`
    <div class="card">
      <h2>主持人入口</h2>
      <p class="muted">管理 YAML 剧本、创建会话、实时结算、回放与导出全量记录。</p>
      <label>主持人密钥（HOST_KEY）</label>
      <input type="password" id="hk" value="${esc(HOST_KEY())}" placeholder="默认 host-1234" />
      <div class="row" style="margin-top:14px">
        <button id="go-host">进入主持人控制台</button>
      </div>
    </div>`));
  app.appendChild(el(`
    <div class="card">
      <h2>参与者入口</h2>
      <p class="muted">凭主持人发放的会话编号与角色邀请码加入；只能看到本角色的信息。</p>
      <div class="row">
        <input type="text" id="sid" placeholder="会话编号（8 位）" style="max-width:280px" />
        <button id="go-join" class="ghost">加入演练</button>
      </div>
    </div>`));
  document.getElementById("go-host").onclick = () => {
    localStorage.setItem("host_key", document.getElementById("hk").value.trim());
    location.hash = "#/host";
  };
  document.getElementById("go-join").onclick = () => {
    const sid = document.getElementById("sid").value.trim();
    if (!sid) return toast("请输入会话编号");
    location.hash = `#/join/${encodeURIComponent(sid)}`;
  };
}

// --------------------------------------------------------------------------- //
// host: drills & sessions
// --------------------------------------------------------------------------- //
async function hostPage() {
  app.innerHTML = `<div class="card"><h2>主持人控制台</h2><p class="muted">加载中…</p></div>`;
  let drills, sessions, jointsList;
  try {
    [drills, sessions, jointsList] = await Promise.all([
      api("/api/drills", { host: true }),
      api("/api/sessions", { host: true }),
      api("/api/joints", { host: true }),
    ]);
  } catch (e) {
    app.innerHTML = "";
    app.appendChild(el(`<div class="card"><h2>主持人控制台</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <p class="muted small">请返回首页检查主持人密钥。</p></div>`));
    return;
  }

  const card = el(`<div class="card">
    <h2>演练剧本（YAML）</h2>
    <div id="drill-list"></div>
    <h3>上传 / 编辑剧本（生成草稿版本）</h3>
    <p class="muted small" id="base-hint">
      保存只生成不可变<strong>草稿</strong>并计算内容摘要；草稿发布后才能用于新会话。
      若已存在发布版，编辑器会自动以<strong>当前发布版</strong>为基准（乐观锁：
      期间他人发布了新版本时发布会被拒绝）。
    </p>
    <textarea id="yaml" placeholder="粘贴 YAML 剧本（顶层 id 需保证唯一）"></textarea>
    <div class="row" style="margin-top:10px">
      <button id="upload">保存为草稿版本</button>
      <span class="muted small" id="upload-state"></span>
    </div>
  </div>`);

  const list = card.querySelector("#drill-list");
  if (!drills.length) {
    list.innerHTML = `<p class="muted">暂无剧本。将 YAML 放入 <code>drills/</code> 目录，或在下方粘贴上传（首次保存为草稿，发布后生效）。</p>`;
  } else {
    for (const d of drills) {
      const pub = d.published_version;
      const draft = d.draft_version;
      const row = el(`<div class="box">
        <div class="row"><strong>${esc(d.name)}</strong>
          <span class="chip mono">${esc(d.id)}</span>
          ${pub ? `<span class="chip ok">已发布 v${pub}</span>`
                : '<span class="chip warn">未发布</span>'}
          ${draft ? `<span class="chip ${pub ? "warn" : "host"}">草稿 v${draft}</span>` : ""}
          <span class="spacer"></span>
          <button class="ghost" data-act="versions">版本管理</button>
          ${pub ? `<button data-act="create">创建会话（v${pub}）</button>`
                : '<button disabled title="发布草稿后才能创建会话">不可创建会话</button>'}
        </div>
        <div class="muted small">${esc(d.description || "")}</div></div>`);
      row.querySelector('[data-act="versions"]').onclick = () => {
        location.hash = `#/host/drill/${encodeURIComponent(d.id)}`;
      };
      const createBtn = row.querySelector('[data-act="create"]');
      if (createBtn) {
        createBtn.onclick = async (ev) => {
          try {
            const r = await api(`/api/sessions?drill_id=${encodeURIComponent(d.id)}`,
              { method: "POST", host: true });
            toast(`会话 ${r.session_id} 已创建（绑定 v${r.drill_version}）`, "ok");
            location.hash = `#/host/session/${r.session_id}`;
          } catch (e2) { toast(e2.message, "error"); }
        };
      }
      list.appendChild(row);
    }
  }
  card.querySelector("#upload").onclick = async () => {
    const state = card.querySelector("#upload-state");
    // Resolve the base version from the parsed YAML's id at click time, so the
    // optimistic-lock check always reflects the freshest drill list.
    let yamlText = card.querySelector("#yaml").value;
    let drillId = null;
    try {
      // Cheap best-effort id read for base lookup; server validates for real.
      drillId = (yamlText.match(/^\s*id\s*:\s*['"]?([\w.-]+)/m) || [])[1];
    } catch { drillId = null; }
    let baseVersion = null;
    if (drillId) {
      const match = drills.find((x) => x.id === drillId);
      baseVersion = match ? match.published_version : null;
    }
    state.textContent = "保存中…";
    try {
      const r = await api("/api/drills", {
        method: "POST", host: true, body: { yaml: yamlText, base_version: baseVersion },
      });
      if (r.reused) {
        toast(`内容未变化，复用 v${r.version}（${r.digest_short}）`, "ok");
      } else {
        toast(`草稿 v${r.version} 已保存（摘要 ${r.digest_short}）`, "ok");
      }
      location.hash = `#/host/drill/${encodeURIComponent(r.id)}`;
    } catch (e) {
      state.textContent = "";
      toast(e.message, "error");
    }
  };

  const sc = el(`<div class="card"><h2>会话</h2><div id="session-list"></div></div>`);
  const sl = sc.querySelector("#session-list");
  if (!sessions.length) {
    sl.innerHTML = `<p class="muted">暂无会话。</p>`;
  } else {
    for (const s of sessions) {
      const [label, cls] = statusChip(s.status);
      sl.appendChild(el(`<div class="box">
        <div class="row">
          <strong class="mono">${esc(s.id)}</strong>
          <span class="chip ${cls}">${label}</span>
          <span class="muted small">${esc(s.drill_name || s.drill_id)}</span>
          <span class="chip mono" title="会话创建时固化的剧本版本与内容摘要">
            v${esc(s.drill_version)} · ${esc(s.digest_short || "")}
          </span>
          <span class="spacer"></span>
          <a href="#/host/session/${esc(s.id)}"><button class="ghost">主持台</button></a>
          <a href="#/replay/${esc(s.id)}"><button class="ghost">回放</button></a>
        </div></div>`));
    }
  }

  const jc = el(`<div class="card"><h2>🔗 联合演练编排</h2>
    <p class="muted small">把多个<strong>独立会话</strong>编入同一场联演，为各会话阶段配置有向依赖：
      当全部前置分支都达到指定结局（AND 屏障）后，系统向目标会话的指定阶段
      <strong>一次性</strong>追加联演注入，并按该会话的角色权限推送。
      保存前校验会话、阶段、结局有效且依赖无环；重复/乱序到达不会重复投递或提前解锁。</p>
    <div id="joint-list"></div>
    <div class="row" style="margin-top:10px">
      <a href="#/host/joint/new/edit"><button>＋ 编排新的联合演练</button></a>
    </div>
  </div>`);
  const jl = jc.querySelector("#joint-list");
  if (!jointsList.length) {
    jl.innerHTML = `<p class="muted small">暂无联演。先创建至少两个会话，再编排依赖。</p>`;
  } else {
    for (const j of jointsList) {
      jl.appendChild(el(`<div class="box">
        <div class="row">
          <strong class="mono">${esc(j.id)}</strong>
          <strong>${esc(j.name)}</strong>
          <span class="spacer"></span>
          <a href="#/host/joint/${esc(j.id)}"><button class="ghost">监控台</button></a>
          <a href="#/host/joint/${esc(j.id)}/edit"><button class="ghost">编排编辑</button></a>
        </div>
        <div class="muted small">创建于 ${fmtTime(Number(j.created_at) || null)}</div>
      </div>`));
    }
  }

  app.innerHTML = "";
  app.append(card, jc, sc);
}

// --------------------------------------------------------------------------- //
// host: script versions & release management
// --------------------------------------------------------------------------- //
const STATUS_CHIP = {
  published: ["已发布", "ok"],
  draft: ["草稿", "warn"],
  history: ["历史版本", ""],
  superseded: ["已废弃草稿", ""],
};

async function drillVersionPage(drillId) {
  app.innerHTML = `<div class="card"><h2>剧本版本管理 <span class="mono">${esc(drillId)}</span></h2>
    <p class="muted">加载中…</p></div>`;

  let versions;
  try {
    versions = await api(`/api/drills/${encodeURIComponent(drillId)}/versions`, { host: true });
  } catch (e) {
    app.innerHTML = "";
    app.appendChild(el(`<div class="card"><h2>剧本版本管理</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <a href="#/host"><button class="ghost">返回主持人控制台</button></a></div>`));
    return;
  }

  const published = versions.find((v) => v.status === "published");
  const draft = versions.find((v) => v.status === "draft");
  const selectable = [...versions].reverse();  // ascending for <select>

  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">版本概览</h2>
        <span class="spacer"></span>
        <a href="#/host"><button class="ghost">← 返回控制台</button></a>
      </div>
      <div class="row" style="margin-top:10px">
        ${published
          ? `<span class="chip ok">当前发布版 v${published.version}</span>
             <span class="chip mono" title="SHA-256 内容摘要">${esc(published.digest_short)}</span>`
          : '<span class="chip warn">尚无发布版（必须发布后才能创建会话）</span>'}
        ${draft
          ? `<span class="chip warn">开放草稿 v${draft.version}（基于 v${draft.base_version ?? "—"}）</span>
             <span class="chip mono">${esc(draft.digest_short)}</span>`
          : '<span class="muted small">当前无草稿</span>'}
      </div>
      <p class="muted small" style="margin-bottom:0">
        每个版本一经生成即<strong>不可变</strong>：保存为草稿 → 校对差异 → 发布；
        发布会校验基准版本未被他人更新，失败不会改变当前发布版。
        已创建/进行中的会话永久绑定其创建时的版本，后续编辑不影响其快照、分支与回放。
      </p>
    </div>
    <div class="card" id="editor-card">
      <h2>编辑草稿</h2>
      <div class="row">
        <button class="ghost" id="load-published">载入当前发布版 YAML</button>
        ${draft ? '<button class="ghost" id="load-draft">载入当前草稿 YAML</button>' : ""}
      </div>
      <textarea id="yaml-editor" placeholder="从上方载入 YAML，或直接粘贴新内容"></textarea>
      <div class="row" style="margin-top:8px">
        <button id="save-draft">保存为新草稿版本</button>
        <span class="muted small">基准版本：<b id="base-label">v${published ? published.version : "（新剧本）"}</b></span>
      </div>
    </div>
    <div class="card" id="diff-card">
      <h2>结构化差异对比</h2>
      <div class="row">
        <label style="margin:0">旧版
          <select id="diff-from"></select></label>
        <label style="margin:0">新版
          <select id="diff-to"></select></label>
        <button id="run-diff">对比</button>
      </div>
      <div id="diff-out" style="margin-top:12px"></div>
    </div>
    <div class="card">
      <h2>版本列表</h2>
      <div id="version-list"></div>
    </div>
    <div class="card" id="yaml-view" hidden></div>
  </div>`);

  const fillSelect = (sel, defVer) => {
    sel.innerHTML = selectable.map((v) =>
      `<option value="${v.version}" ${v.version === defVer ? "selected" : ""}>v${v.version} · ${esc(v.digest_short)} · ${esc(v.status)}</option>`
    ).join("");
  };
  fillSelect(root.querySelector("#diff-from"), published ? published.version : null);
  fillSelect(root.querySelector("#diff-to"), draft ? draft.version : (published ? published.version : null));

  // editor wiring
  const editor = root.querySelector("#yaml-editor");
  const setEditor = (text) => { editor.value = text; toast("YAML 已载入编辑器", "ok"); };
  root.querySelector("#load-published").onclick = async () => {
    if (!published) return toast("尚无发布版");
    const v = await api(`/api/drills/${drillId}/versions/${published.version}`, { host: true });
    setEditor(v.yaml);
  };
  const loadDraftBtn = root.querySelector("#load-draft");
  if (loadDraftBtn) loadDraftBtn.onclick = async () => {
    const v = await api(`/api/drills/${drillId}/versions/${draft.version}`, { host: true });
    setEditor(v.yaml);
  };
  root.querySelector("#save-draft").onclick = async () => {
    const base = published ? published.version : null;
    try {
      const res = await api("/api/drills", {
        method: "POST", host: true,
        body: { yaml: editor.value, base_version: base },
      });
      toast(res.reused
        ? `内容未变化，复用 v${res.version}`
        : `草稿 v${res.version} 已保存（${res.digest_short}）`, "ok");
      drillVersionPage(drillId);
    } catch (e) { toast(e.message, "error"); }
  };

  // diff wiring
  root.querySelector("#run-diff").onclick = async () => {
    const from = Number(root.querySelector("#diff-from").value);
    const to = Number(root.querySelector("#diff-to").value);
    const out = root.querySelector("#diff-out");
    if (from === to) { out.innerHTML = '<p class="muted small">同一版本无差异。</p>'; return; }
    out.innerHTML = '<p class="muted small">对比中…</p>';
    try {
      const diff = await api(
        `/api/drills/${encodeURIComponent(drillId)}/diff?from=${from}&to=${to}`,
        { host: true });
      out.innerHTML = "";
      out.appendChild(renderDiff(diff, from, to));
    } catch (e) { out.innerHTML = `<p class="chip danger">${esc(e.message)}</p>`; }
  };

  // version list
  const vl = root.querySelector("#version-list");
  for (const v of versions) {
    const [slabel, scls] = STATUS_CHIP[v.status] || [v.status, ""];
    const row = el(`<div class="box version-row">
      <div class="row">
        <strong class="mono">v${v.version}</strong>
        <span class="chip ${scls}">${slabel}</span>
        <span class="chip mono" title="SHA-256">${esc(v.digest_short)}</span>
        <span class="muted small">基准 v${v.base_version ?? "—"}</span>
        <span class="spacer"></span>
        <button class="ghost" data-act="yaml">查看 YAML</button>
        <button class="ghost" data-act="diff-prev">与上一版对比</button>
        ${v.status === "history" || v.status === "published" || v.status === "superseded"
          ? '<button class="ghost" data-act="copy">复制为新草稿</button>' : ""}
        ${v.status === "draft" ? '<button data-act="publish">▶ 发布此草稿</button>' : ""}
      </div>
      <div class="muted small">
        创建 ${fmtTime(v.created_at)}${v.published_at ? ` · 发布 ${fmtTime(v.published_at)}` : ""}
      </div>
    </div>`);

    row.querySelector('[data-act="yaml"]').onclick = () => showYaml(drillId, v, root);
    const prevBtn = row.querySelector('[data-act="diff-prev"]');
    prevBtn.onclick = async () => {
      const earlier = versions.find((x) => x.version === v.version - 1);
      if (!earlier) return toast("v1 没有上一版本可对比");
      root.querySelector("#diff-from").value = earlier.version;
      root.querySelector("#diff-to").value = v.version;
      root.querySelector("#run-diff").click();
      document.getElementById("diff-card").scrollIntoView({ behavior: "smooth" });
    };
    const copyBtn = row.querySelector('[data-act="copy"]');
    if (copyBtn) copyBtn.onclick = async () => {
      if (!confirm(`把 v${v.version} 的内容复制为新的可编辑草稿？（仍需发布后才生效）`)) return;
      try {
        const base = published ? published.version : null;
        const res = await api(`/api/drills/${encodeURIComponent(drillId)}/copy-draft`, {
          method: "POST", host: true,
          body: { source_version: v.version, base_version: base },
        });
        toast(res.reused
          ? `相同草稿已存在（v${res.version}）`
          : `已从 v${v.version} 复制为草稿 v${res.version}`, "ok");
        drillVersionPage(drillId);
      } catch (e) { toast(e.message, "error"); }
    };
    const pubBtn = row.querySelector('[data-act="publish"]');
    if (pubBtn) pubBtn.onclick = async () => {
      const base = v.base_version;
      if (!confirm(`确认发布草稿 v${v.version}（基于 v${base ?? "—"}）？发布后新会话将使用此版本。`)) return;
      try {
        await api(`/api/drills/${encodeURIComponent(drillId)}/versions/${v.version}/publish`, {
          method: "POST", host: true, body: { base_version: base },
        });
        toast(`v${v.version} 已发布`, "ok");
        drillVersionPage(drillId);
      } catch (e) {
        toast(`${e.message}。请刷新版本列表、重新复制/编辑后再发布。`, "error");
      }
    };
    vl.appendChild(row);
  }

  app.innerHTML = "";
  app.appendChild(root);
  if (draft) root.querySelector("#run-diff").click();
}

function showYaml(drillId, v, root) {
  const card = root.querySelector("#yaml-view");
  card.hidden = false;
  card.innerHTML = `<h2>v${v.version} 的 YAML（${esc(v.status)}）</h2>
    <pre class="yaml-pre">${esc(v.yaml || "（无原始 YAML）")}</pre>
    <div class="row"><button class="ghost" id="hide-yaml">收起</button></div>`;
  card.querySelector("#hide-yaml").onclick = () => { card.hidden = true; };
  card.scrollIntoView({ behavior: "smooth" });
}

// ---- structured diff rendering ----
function renderDiff(diff, fromV, toV) {
  const wrap = el(`<div>
    <div class="row">
      <strong>v${fromV} → v${toV}</strong>
      ${diff.has_changes
        ? `<span class="chip warn">有结构变化</span>`
        : '<span class="chip ok">内容相同（无结构差异）</span>'}
    </div>
    <div id="diff-body" class="diff-body"></div>
  </div>`);
  const body = wrap.querySelector("#diff-body");
  if (!diff.has_changes) return wrap;

  const addOldNew = (f) =>
    `<span class="diff-old">${esc(fmtVal(f.old))}</span>
     <span class="diff-arrow">→</span>
     <span class="diff-new">${esc(fmtVal(f.new))}</span>`;

  if (diff.meta.length) {
    body.appendChild(el(`<div class="diff-section">
      <h4>剧本属性</h4>
      ${diff.meta.map((f) => `<div class="diff-line"><b>${esc(f.label)}</b>：${addOldNew(f)}</div>`).join("")}
    </div>`));
  }

  const roles = diff.roles;
  if (roles.added.length || roles.removed.length || roles.changed.length) {
    body.appendChild(el(`<div class="diff-section"><h4>角色</h4>
      ${roles.added.map((r) => `<div class="diff-line diff-add">＋ 新增角色 ${esc(r.name)} <code>${esc(r.id)}</code>（邀请码 ${esc(r.invite)}）</div>`).join("")}
      ${roles.removed.map((r) => `<div class="diff-line diff-remove">－ 删除角色 ${esc(r.name)} <code>${esc(r.id)}</code></div>`).join("")}
      ${roles.changed.map((r) => `<div class="diff-line">✎ 角色 ${esc(r.name)}：
        ${r.fields.map((f) => `${esc(f.label)} ${addOldNew(f)}`).join("；")}</div>`).join("")}
    </div>`));
  }

  const stages = diff.stages;
  if (stages.added.length) {
    body.appendChild(el(`<div class="diff-section"><h4>新增阶段</h4>
      ${stages.added.map((s) => `<div class="diff-line diff-add">＋ ${esc(s.title)} <code>${esc(s.id)}</code></div>`).join("")}
    </div>`));
  }
  if (stages.removed.length) {
    body.appendChild(el(`<div class="diff-section"><h4>删除阶段</h4>
      ${stages.removed.map((s) => `<div class="diff-line diff-remove">－ ${esc(s.title)} <code>${esc(s.id)}</code></div>`).join("")}
    </div>`));
  }
  for (const s of stages.changed) {
    const sec = el(`<div class="diff-section">
      <h4>✎ 阶段 ${esc(s.title)} <code>${esc(s.id)}</code></h4></div>`);
    for (const f of s.fields) {
      sec.appendChild(el(`<div class="diff-line"><b>${esc(f.label)}</b>：${addOldNew(f)}</div>`));
    }
    // views
    for (const x of s.views.added) sec.appendChild(el(`<div class="diff-line diff-add">＋ 新增 ${esc(x.role_name)} 可见信息：${esc(x.text)}</div>`));
    for (const x of s.views.removed) sec.appendChild(el(`<div class="diff-line diff-remove">－ 删除 ${esc(x.role_name)} 可见信息：${esc(x.text)}</div>`));
    for (const x of s.views.changed) sec.appendChild(el(
      `<div class="diff-line">✎ ${esc(x.role_name)} 可见信息：
        <span class="diff-old">${esc(x.old)}</span><span class="diff-arrow">→</span>
        <span class="diff-new">${esc(x.new)}</span></div>`));
    // options
    for (const r of s.options.added_roles) {
      sec.appendChild(el(`<div class="diff-line diff-add">＋ 新增决策角色 ${esc(r.role_name)}：
        ${r.options.map((o) => `<code>${esc(o.id)}</code> ${esc(o.text)}${o.next ? `→${esc(o.next)}` : ""}`).join("；")}</div>`));
    }
    for (const r of s.options.removed_roles) {
      sec.appendChild(el(`<div class="diff-line diff-remove">－ 删除决策角色 ${esc(r.role_name)}</div>`));
    }
    for (const r of s.options.changed) {
      for (const o of r.added) sec.appendChild(el(`<div class="diff-line diff-add">＋ ${esc(r.role_name)} 新增选项 <code>${esc(o.id)}</code> ${esc(o.text)}${o.next ? ` → ${esc(o.next)}` : ""}</div>`));
      for (const o of r.removed) sec.appendChild(el(`<div class="diff-line diff-remove">－ ${esc(r.role_name)} 删除选项 <code>${esc(o.id)}</code> ${esc(o.text)}</div>`));
      for (const o of r.changed) sec.appendChild(el(`<div class="diff-line">✎ ${esc(r.role_name)} 选项 <code>${esc(o.id)}</code>：
        ${o.fields.map((f) => `${esc(f.label)} ${addOldNew(f)}`).join("；")}</div>`));
    }
    // transitions
    for (const t of s.transitions.added) sec.appendChild(el(
      `<div class="diff-line diff-add">＋ 新规则 当 ${fmtWhen(t.when)} → ${fmtNext(t.next)}</div>`));
    for (const t of s.transitions.removed) sec.appendChild(el(
      `<div class="diff-line diff-remove">－ 删规则 当 ${fmtWhen(t.when)} → ${fmtNext(t.next)}</div>`));
    for (const t of s.transitions.changed) sec.appendChild(el(
      `<div class="diff-line">✎ 规则 当 ${fmtWhen(t.new.when)}：跳转
        <span class="diff-old">${fmtNext(t.old.next)}</span><span class="diff-arrow">→</span>
        <span class="diff-new">${fmtNext(t.new.next)}</span></div>`));
    body.appendChild(sec);
  }
  return wrap;
}

function fmtVal(v) {
  if (v === null) return "（无 / 结束）";
  if (v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
function fmtWhen(when) {
  return Object.entries(when || {}).map(([k, v2]) => `${k}=${v2}`).join(" 且 ") || "（无条件）";
}
function fmtNext(n) { return n ? `<code>${esc(n)}</code>` : "（演练结束）"; }

// --------------------------------------------------------------------------- //
// host live console
// --------------------------------------------------------------------------- //
function hostSessionPage(sid) {
  app.innerHTML = `<div class="card"><h2>主持台 <span class="mono">${esc(sid)}</span></h2>
    <p id="connline" class="muted">正在建立实时连接…</p></div>`;
  let snapRef = null;

  liveHandle = new Live(sid, { host: HOST_KEY() }, {
    onStatus: (s, delay) => {
      const line = document.getElementById("connline");
      if (!line) return;
      if (s === "reconnecting") {
        line.innerHTML = `<span class="conn off">● 断开，${(delay / 1000).toFixed(0)}s 后自动重连…</span>`;
      } else if (s === "connecting") {
        line.innerHTML = `<span class="muted">● 连接中…</span>`;
      }
    },
    onSnapshot: (snap) => { snapRef = snap; renderHost(snap); },
  });

  function renderHost(snap) {
    clearTimers();
    const s = snap.session;
    const [lbl, cls] = statusChip(s.status);
    const banner = {
      created: ["wait", "会话尚未开始。参与者凭邀请码加入；就绪后开始演练。"],
      running: ["live", "演练进行中：服务端正向各客户端推送按角色裁剪的快照。"],
      ended: ["ended", `演练已结束（${esc(s.end_reason_label || s.end_reason || "")}）。可回放或导出记录。`],
    }[s.status];

    const root = el(`<div>
      <div class="banner ${banner[0]}">${banner[1]}</div>
      <div class="card">
        <div class="row">
          <h2 style="margin:0">控制台</h2>
          <span class="chip ${cls}">${lbl}</span>
          <span class="spacer"></span>
          <span class="conn on">● 已连接</span>
        </div>
        <div class="row" id="buttons" style="margin-top:12px"></div>
        <p class="muted small" style="margin-top:10px">
          会话单调版本号：<b class="mono">v${s.version}</b> ·
          绑定剧本版本：<b class="mono">v${esc(snap.drill.version)}</b>
          <span class="chip mono" title="SHA-256 内容摘要（会话创建时固化）">${esc(snap.drill.digest_short)}</span> ·
          服务端时间：<span id="srvclock"></span>
        </p>
      </div>
      <div class="card" id="people"></div>
      <div class="card" id="joints-card" hidden><h2>🔗 所属联合演练</h2><div id="joint-links"></div></div>
      <div class="card" id="stages"><h2>阶段（含各角色隐藏信息）</h2></div>
      <div class="card" id="exports"></div>
    </div>`);

    const btns = root.querySelector("#buttons");
    if (s.status === "created") {
      const b = el(`<button>▶ 开始演练</button>`);
      b.onclick = () => liveHandle.send({ type: "start" });
      btns.appendChild(b);
    }
    if (s.status === "running") {
      const b1 = el(`<button class="warn">⏭ 立即结算当前阶段（未提交按超时）</button>`);
      b1.onclick = () => liveHandle.send({ type: "settle" });
      const b2 = el(`<button class="danger">■ 结束整场演练</button>`);
      b2.onclick = () => {
        if (confirm("确定结束整场演练？当前未提交的角色将按超时结算。"))
          liveHandle.send({ type: "end" });
      };
      btns.append(b1, b2);
    }
    btns.appendChild(el(`<a href="#/replay/${esc(sid)}"><button class="ghost">↺ 确定性回放</button></a>`));
    btns.appendChild(el(`<a href="#/host/drill/${encodeURIComponent(snap.drill.id)}"><button class="ghost">剧本版本管理</button></a>`));

    // people
    const people = root.querySelector("#people");
    people.innerHTML = `<h2>角色、邀请码与参与者</h2>`;
    const joined = Object.fromEntries(snap.participants.map((p) => [p.role_id, p]));
    const table = el(`<table><thead><tr>
      <th>角色</th><th>邀请码</th><th>参与者</th><th>连接</th><th>邀请链接</th></tr></thead><tbody></tbody></table>`);
    for (const r of snap.roles) {
      const p = joined[r.id];
      const link = `${location.origin}/#/join/${sid}?invite=${encodeURIComponent(r.invite)}`;
      const tr = el(`<tr>
        <td>${esc(r.name)}</td>
        <td class="mono">${esc(r.invite)}</td>
        <td>${p ? esc(p.display_name) : '<span class="muted">未加入</span>'}</td>
        <td>${p ? (p.online ? '<span class="conn on">●在线</span>' : '<span class="conn off">○离线</span>') : ""}</td>
        <td><div class="copybox"><code>${esc(link)}</code><button class="ghost">复制</button></div></td>
      </tr>`);
      tr.querySelector("button").onclick = () => copyText(link);
      table.querySelector("tbody").appendChild(tr);
    }
    people.appendChild(table);

    // joints this session belongs to (host fetches the joint list once/render)
    api("/api/joints", { host: true }).then(async (all) => {
      const mine = [];
      for (const jrow of all) {
        try {
          const st = await api(`/api/joints/${jrow.id}`, { host: true });
          if (st.sessions.some((x) => x.id === sid)) mine.push(st);
        } catch { /* ignore */ }
      }
      const card2 = root.querySelector("#joints-card");
      const links = root.querySelector("#joint-links");
      if (!card2) return;
      if (!mine.length) { card2.hidden = true; return; }
      card2.hidden = false;
      for (const j of mine) {
        const delivered = j.rules.filter((r) => r.state === "delivered").length;
        const waiting = j.rules.filter((r) => r.state === "waiting").length;
        links.appendChild(el(`<div class="box">
          <div class="row">
            <strong>${esc(j.name)}</strong>
            <span class="chip mono">${esc(j.id)}</span>
            <span class="chip ok">已投递 ${delivered}</span>
            <span class="chip warn">等待 ${waiting}</span>
            <span class="spacer"></span>
            <a href="#/host/joint/${esc(j.id)}"><button class="ghost">打开联演监控台</button></a>
          </div></div>`));
      }
    }).catch(() => {});

    // stages
    const stagesCard = root.querySelector("#stages");
    const curFirst = [...snap.history].sort((a, b) =>
      (a.stage_id === s.current_stage ? -1 : b.stage_id === s.current_stage ? 1 : 0));
    for (const stg of curFirst) stagesCard.appendChild(hostStageBlock(stg, snap));

    // exports
    const ex = root.querySelector("#exports");
    ex.innerHTML = `<h2>记录导出</h2>
      <p class="muted small">主持人全量记录包含隐藏信息、全部决定与事件日志，并标注本会话固化的
        <b>剧本版本 v${esc(snap.drill.version)}（${esc(snap.drill.digest_short)}）</b>；
        个人记录由参与者用本人令牌导出，已剔除其他角色的任何隐藏信息。</p>
      <div class="row">
        <a href="/api/sessions/${sid}/record?fmt=md" target="_blank"><button>全量记录 Markdown</button></a>
        <a href="/api/sessions/${sid}/record?fmt=json" target="_blank"><button class="ghost">全量记录 JSON</button></a>
      </div>`;

    app.innerHTML = "";
    app.appendChild(root);

    const tick = () => {
      const clock = document.getElementById("srvclock");
      if (clock) clock.textContent = fmtClock(liveHandle.now());
    };
    intervals.push(setInterval(tick, 500));
    tick();
  }

  function hostStageBlock(stg, snap) {
    const s = snap.session;
    const isCurrent = stg.stage_id === s.current_stage;
    const cls = isCurrent ? "current" : stg.kind === "sealed" ? "sealed" : "";
    const node = el(`<div class="stage ${cls}">
      <div class="row">
        <span class="stage-title">${esc(stg.title)}</span>
        <span class="chip mono">${esc(stg.stage_id)}</span>
        ${isCurrent ? '<span class="chip ok">当前阶段</span>' : ""}
        ${stg.kind === "sealed" ? `<span class="chip">${esc(stg.reason_label)}</span>` : ""}
        ${!stg.reached ? '<span class="chip warn">未到达</span>' : ""}
        <span class="spacer"></span>
        ${isCurrent && stg.duration ? '<span class="timer" id="host-timer">--</span>' : ""}
      </div>
      <div class="box public"><div class="box-label">公开信息（全员可见）</div>${esc(stg.public)}</div>
      <div class="injections"></div>
      <div class="views"></div>
      <div class="opts"></div>
      ${stg.kind === "sealed" ? `<div class="muted small">下一阶段：<b>${esc(stg.next || "（结束）")}</b></div>` : ""}
    </div>`);

    for (const inj of stg.injections || []) {
      const node2 = node.querySelector(".injections");
      const box = el(`<div class="box injection">
        <div class="row">
          <span class="box-label">🔗 联演注入 · ${esc(inj.joint_name)}</span>
          ${inj.late ? '<span class="chip warn">迟到补投</span>' : '<span class="chip ok">已投递</span>'}
          <span class="spacer"></span>
          <span class="muted small mono">v${esc(inj.version)}</span>
        </div>
        <div><strong>${esc(inj.title)}</strong></div>
        <div>${esc(inj.public)}</div>
        <div class="inj-views"></div>
      </div>`);
      const vw = box.querySelector(".inj-views");
      for (const [rid, text] of Object.entries(inj.views || {})) {
        vw.appendChild(el(`<div class="box private"><div class="box-label">仅 ${esc(roleName(snap, rid))} 可见</div>${esc(text)}</div>`));
      }
      node2.appendChild(box);
    }

    for (const [rid, text] of Object.entries(stg.views || {})) {
      node.querySelector(".views").appendChild(el(
        `<div class="box private"><div class="box-label">仅 ${esc(roleName(snap, rid))} 可见</div>${esc(text)}</div>`));
    }

    for (const rid of stg.deciding_roles || []) {
      const choice = stg.choices?.[rid];
      let stateHtml;
      if (!stg.reached) stateHtml = '<span class="muted">—</span>';
      else if (stg.kind === "sealed" && !choice) stateHtml = '<span class="chip danger">超时未提交</span>';
      else if (choice) stateHtml = `<span class="chip ok">已提交：${esc(choice.label || choice.id)}</span>`;
      else stateHtml = '<span class="chip warn">待提交</span>';

      const optsHtml = (stg.options?.[rid] || []).map((o) => `
        <div class="option ${choice && choice.id === o.id ? "chosen" : "disabled"}">
          <span class="oid">${esc(o.id)}</span>${esc(o.text)}
          ${o.next ? `<span class="muted small"> → ${esc(o.next)}</span>` : ""}
        </div>`).join("");

      node.querySelector(".opts").appendChild(el(`<div class="box">
        <div class="row"><strong>${esc(roleName(snap, rid))}</strong>${stateHtml}</div>
        ${optsHtml}
      </div>`));
    }

    if (isCurrent) {
      const cd = snap.current_detail;
      node.appendChild(el(`<p class="muted small">
        决策角色 ${cd.deciding_roles.length} 人，已提交 ${cd.submitted_roles.length} 人。
        全员提交即自动结算；到达截止时刻未提交者按超时进入条件分支。</p>`));
      if (stg.duration) {
        const bar = el(`<div class="timerbar" id="host-bar"><div style="width:100%"></div></div>`);
        node.appendChild(bar);
      }
    }
    return node;
  }

  // host countdown refresh
  setInterval(() => {
    if (!snapRef || snapRef.session.status !== "running") return;
    const s = snapRef.session;
    const t = document.getElementById("host-timer");
    const bar = document.getElementById("host-bar");
    if (t && s.deadline) {
      const remain = s.deadline - liveHandle.now();
      t.textContent = remain > 0 ? `${Math.ceil(remain)} 秒` : "结算中…";
      t.classList.toggle("urgent", remain <= 10);
      if (bar) {
        const cur = snapRef.history.find((x) => x.stage_id === s.current_stage);
        const pct = cur?.duration ? Math.max(0, remain / cur.duration) * 100 : 0;
        bar.firstElementChild.style.width = `${pct}%`;
        bar.classList.toggle("urgent", remain <= 10);
      }
    }
  }, 300);
}

// --------------------------------------------------------------------------- //
// join
// --------------------------------------------------------------------------- //
function joinPage(sid, params) {
  const existing = getCred(sid);
  app.innerHTML = "";
  const card = el(`<div class="card">
    <h2>加入演练 <span class="mono">${esc(sid)}</span></h2>
    <p class="muted">请输入主持人向您提供的角色邀请码。加入后该角色即与您绑定。</p>
    <label>角色邀请码</label>
    <input type="text" id="invite" value="${esc(params.get("invite") || "")}" placeholder="如 CMD-2026" />
    <label>您的姓名（可选，默认使用角色名）</label>
    <input type="text" id="name" placeholder="姓名 / 工号" />
    <div class="row" style="margin-top:14px">
      <button id="do-join">加入</button>
      ${existing ? `<button class="ghost" id="resume">以「${esc(existing.role_name)}」重新进入</button>` : ""}
    </div>
    <p id="jerr" class="chip danger hidden" style="margin-top:12px"></p>
  </div>`);
  app.appendChild(card);

  const showErr = (m) => {
    const p = card.querySelector("#jerr");
    p.textContent = m;
    p.classList.remove("hidden");
  };
  card.querySelector("#do-join").onclick = async () => {
    const invite = card.querySelector("#invite").value.trim();
    const name = card.querySelector("#name").value.trim();
    if (!invite) return showErr("请输入邀请码");
    try {
      const r = await api(`/api/sessions/${encodeURIComponent(sid)}/join`, {
        method: "POST", body: { invite, name },
      });
      setCred(sid, {
        token: r.token, role_id: r.role_id, role_name: r.role_name,
        display_name: r.display_name,
      });
      toast(`已加入：${r.role_name}`, "ok");
      location.hash = `#/play/${sid}`;
    } catch (e) { showErr(e.message); }
  };
  const resume = card.querySelector("#resume");
  if (resume) resume.onclick = () => { location.hash = `#/play/${sid}`; };
}

// --------------------------------------------------------------------------- //
// participant live console
// --------------------------------------------------------------------------- //
function playPage(sid) {
  const cred = getCred(sid);
  if (!cred) { location.hash = `#/join/${sid}`; return; }

  app.innerHTML = `<div class="card"><h2>演练控制台</h2>
    <p id="connline" class="muted">正在建立实时连接…</p></div>`;
  let snapRef = null;

  liveHandle = new Live(sid, { token: cred.token }, {
    onStatus: (s, delay) => {
      const line = document.getElementById("connline");
      if (!line) return;
      line.innerHTML = s === "reconnecting"
        ? `<span class="conn off">● 断线，${(delay / 1000).toFixed(0)}s 后自动重连（提交不会丢失）…</span>`
        : `<span class="muted">● 连接中…</span>`;
    },
    onSnapshot: (snap) => { snapRef = snap; renderPlay(snap); },
    onResult: (r) => {
      if (r.duplicate) toast("已提交过，本次为重复请求（幂等返回）", "ok");
      else if (r.accepted) toast("选择已提交", "ok");
      else if (r.reason === "deadline") toast("到达截止时刻，本阶段已结算", "error");
    },
  });

  function renderPlay(snap) {
    clearTimers();
    const s = snap.session;
    const me = snap.me;
    const [lbl] = statusChip(s.status);

    const root = el(`<div>
      <div class="card">
        <div class="row">
          <h2 style="margin:0">${esc(snap.drill.name)}</h2>
          <span class="spacer"></span>
          <span class="chip host">${esc(me.role_name)}</span>
          <span class="conn on">● 已连接</span>
        </div>
        <p class="muted small" style="margin:6px 0 0">
          会话 <span class="mono">${esc(s.id)}</span> · 状态：${lbl} ·
          会话版本 <b class="mono">v${s.version}</b> ·
          剧本 <b class="mono">v${esc(snap.drill.version)}</b>
          <span class="chip mono" title="内容摘要（创建时固化，后续编辑不影响本会话）">${esc(snap.drill.digest_short)}</span>
          · 服务端时间 <span id="srvclock"></span>
        </p>
      </div>
      <div id="banner-slot"></div>
      <div class="card" id="current"></div>
      <div class="card" id="history"><h2>已发生的阶段</h2></div>
      <div class="card" id="exports"></div>
    </div>`);

    const bannerSlot = root.querySelector("#banner-slot");
    if (s.status === "created") {
      bannerSlot.appendChild(el(`<div class="banner wait">演练尚未开始，请等待主持人开始。</div>`));
    } else if (s.status === "ended") {
      bannerSlot.appendChild(el(`<div class="banner ended">
        演练已结束（${esc(s.end_reason_label || "")}）。可在下方导出您的个人记录。</div>`));
    } else {
      bannerSlot.appendChild(el(`<div class="banner live">演练进行中。</div>`));
    }

    // current stage (the last block is current when kind == current)
    const curCard = root.querySelector("#current");
    const current = snap.history.find((x) => x.kind === "current");
    if (s.status === "running" && current) {
      curCard.innerHTML = "";
      curCard.appendChild(roleStageBlock(current, snap, true));
    } else {
      curCard.innerHTML = s.status === "created"
        ? "<h2>等待开始…</h2>"
        : "<h2>当前无开放阶段</h2>";
    }

    const hist = root.querySelector("#history");
    const sealed = snap.history.filter((x) => x.kind === "sealed");
    if (!sealed.length) {
      hist.innerHTML = `<h2>已发生的阶段</h2><p class="muted small">暂无。</p>`;
    } else {
      for (const stg of sealed) hist.appendChild(roleStageBlock(stg, snap, false));
    }

    // exports (visible always, most useful after end)
    const ex = root.querySelector("#exports");
    const tk = encodeURIComponent(cred.token);
    ex.innerHTML = `<h2>我的个人记录</h2>
      <p class="muted small">仅含公开信息与您本人的可见内容和选择，绝不含其他角色的隐藏信息。</p>
      <div class="row">
        <a href="/api/sessions/${sid}/record?fmt=md&token=${tk}" target="_blank"><button>导出 Markdown</button></a>
        <a href="/api/sessions/${sid}/record?fmt=json&token=${tk}" target="_blank"><button class="ghost">导出 JSON</button></a>
        <a href="#/replay/${sid}"><button class="ghost">我的回放</button></a>
        <span class="spacer"></span>
        <button class="ghost" id="leave">清除本地登录信息</button>
      </div>`;
    ex.querySelector("#leave").onclick = () => {
      localStorage.removeItem(credKey(sid));
      location.hash = "#/";
    };

    app.innerHTML = "";
    app.appendChild(root);

    const tick = () => {
      const clock = document.getElementById("srvclock");
      if (clock) clock.textContent = fmtClock(liveHandle.now());
      const t = document.getElementById("role-timer");
      const bar = document.getElementById("role-bar");
      if (t && snapRef.session.deadline) {
        const remain = snapRef.session.deadline - liveHandle.now();
        t.textContent = remain > 0 ? `剩余 ${Math.ceil(remain)} 秒` : "正在结算…";
        t.classList.toggle("urgent", remain <= 10);
        if (bar) {
          const dur = current?.duration || 0;
          bar.firstElementChild.style.width = `${dur ? Math.max(0, remain / dur) * 100 : 0}%`;
          bar.classList.toggle("urgent", remain <= 10);
        }
      }
    };
    intervals.push(setInterval(tick, 250));
    tick();
  }

  function roleStageBlock(stg, snap, isCurrent) {
    const node = el(`<div class="stage ${isCurrent ? "current" : "sealed"}">
      <div class="row">
        <span class="stage-title">${esc(stg.title)}</span>
        ${isCurrent ? '<span class="chip ok">当前阶段</span>' : ""}
        ${!isCurrent ? `<span class="chip">${esc(stg.reason_label)}</span>` : ""}
        <span class="spacer"></span>
        ${isCurrent && stg.duration ? '<span class="timer" id="role-timer">--</span>' : ""}
      </div>
      ${isCurrent && stg.duration ? '<div class="timerbar" id="role-bar"><div style="width:100%"></div></div>' : ""}
      <div class="box public"><div class="box-label">公开信息</div>${esc(stg.public)}</div>
      <div class="injections"></div>
      ${stg.private ? `<div class="box private"><div class="box-label">仅您可见</div>${esc(stg.private)}</div>` : ""}
      <div class="decision"></div>
      ${!isCurrent ? `<div class="muted small">阶段后续：<b>${esc(stg.next ? "进入下一阶段" : "演练结束")}</b></div>` : ""}
    </div>`);

    for (const inj of stg.injections || []) {
      const wrap = node.querySelector(".injections");
      const mine = (inj.views || {})[snap.me.role_id] || "";
      wrap.appendChild(el(`<div class="box injection">
        <div class="row">
          <span class="box-label">🔗 联演注入 · ${esc(inj.joint_name)}</span>
          ${inj.late ? '<span class="chip warn">迟到补投</span>' : '<span class="chip ok">新消息</span>'}
        </div>
        <div><strong>${esc(inj.title)}</strong></div>
        <div>${esc(inj.public)}</div>
        ${mine ? `<div class="box private"><div class="box-label">仅您可见</div>${esc(mine)}</div>` : ""}
      </div>`));
    }


    const dec = node.querySelector(".decision");
    if (stg.deciding) {
      const chosen = stg.my_choice;
      if (isCurrent) {
        if (chosen) {
          dec.appendChild(el(`<p><span class="chip ok">您已提交：${esc(stg.my_choice_label || chosen)}</span></p>`));
          dec.appendChild(el(`<p class="muted small">提交不可更改。如因断线重复发送，服务器幂等处理。</p>`));
        } else {
          dec.appendChild(el(`<p class="muted small">请在限时内选择（截止后未选按超时处理）：</p>`));
        }
      } else if (stg.timed_out) {
        dec.appendChild(el(`<p><span class="chip danger">您未在截止前提交（超时）</span></p>`));
      } else if (chosen) {
        dec.appendChild(el(`<p><span class="chip ok">您的决定：${esc(stg.my_choice_label || chosen)}</span></p>`));
      }

      for (const o of stg.options || []) {
        const btn = el(`<button class="option ${chosen === o.id ? "chosen" : ""} ${chosen ? "disabled" : ""}">
          <span class="oid">${esc(o.id)}</span>${esc(o.text)}</button>`);
        if (!chosen && isCurrent) {
          btn.onclick = () => {
            if (!confirm(`确认选择「${o.text}」？提交后不可更改。`)) return;
            liveHandle.send({
              type: "submit",
              choice_id: o.id,
              client_stage: stg.stage_id,
            });
            btn.disabled = true;
            toast("已发送，等待服务器确认…");
          };
        }
        dec.appendChild(btn);
      }
    } else if (isCurrent) {
      dec.appendChild(el(`<p class="muted small">本阶段您无需决策，请等待其他角色提交或主持人结算。</p>`));
    }
    return node;
  }
}

// --------------------------------------------------------------------------- //
// joint exercise orchestration: editor + live monitor
// --------------------------------------------------------------------------- //
const END_OUTCOME = "@end";

class JointLive {
  constructor(jointId, onSnapshot, onStatus) {
    this.jointId = jointId;
    this.onSnapshot = onSnapshot;
    this.onStatus = onStatus || (() => {});
    this.closedByUser = false;
    this.retry = 0;
    this.connect();
  }
  connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(
      `${proto}://${location.host}/ws/joint/${this.jointId}?host=${encodeURIComponent(HOST_KEY())}`
    );
    this.ws = ws;
    this.onStatus("connecting");
    ws.onopen = () => { this.retry = 0; this.onStatus("open"); };
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "joint_snapshot") this.onSnapshot(msg.joint);
      else if (msg.type === "error") toast(msg.detail || "联演连接错误", "error");
    };
    ws.onclose = () => {
      if (this.closedByUser) return;
      this.retry = Math.min(this.retry + 1, 6);
      const delay = Math.min(500 * 2 ** this.retry, 10000);
      this.onStatus("reconnecting", delay);
      setTimeout(() => { if (!this.closedByUser) this.connect(); }, delay);
    };
  }
  close() { this.closedByUser = true; try { this.ws.close(); } catch {} }
}

function jointStatusChip(state) {
  return ({
    waiting: ["等待前置", "warn"],
    ready: ["前置已满足", "ok"],
    delivering: ["补投中", "warn"],
    delivered: ["已投递", "ok"],
  }[state] || [state, ""]);
}

// ---------- editor ---------- //
async function jointEditPage(jid) {
  const isNew = jid === "new";
  app.innerHTML = `<div class="card"><h2>${isNew ? "编排新的联合演练" : `编辑联演 ${esc(jid)}`}</h2>
    <p class="muted">加载会话列表…</p></div>`;

  let sessions;
  try {
    sessions = await api("/api/sessions", { host: true });
  } catch (e) {
    app.innerHTML = "";
    app.appendChild(el(`<div class="card"><p class="chip danger">${esc(e.message)}</p>
      <a href="#/host"><button class="ghost">返回控制台</button></a></div>`));
    return;
  }

  // Outline cache (pinned script version per session): stages + outcomes + roles.
  const outlineCache = {};
  const outline = async (sid) => {
    if (!outlineCache[sid]) {
      outlineCache[sid] = await api(`/api/sessions/${encodeURIComponent(sid)}/outline`, { host: true });
    }
    return outlineCache[sid];
  };

  // Editor state.
  const state0 = {
    name: "",
    memberIds: [],
    rules: [],
  };
  let existing = null;
  if (!isNew) {
    try {
      existing = await api(`/api/joints/${encodeURIComponent(jid)}`, { host: true });
    } catch (e) {
      app.innerHTML = "";
      app.appendChild(el(`<div class="card"><p class="chip danger">${esc(e.message)}</p>
        <a href="#/host"><button class="ghost">返回控制台</button></a></div>`));
      return;
    }
    state0.name = existing.name;
    state0.memberIds = existing.sessions.map((s) => s.id);
    state0.rules = existing.rules.map((r) => ({
      id: r.id,
      target_session_id: r.target_session_id,
      target_stage_id: r.target_stage_id,
      title: r.title,
      public: r.public,
      views: Object.entries(r.views || {}).map(([role_id, text]) => ({ role_id, text })),
      branches: r.branches.map((b) => ({
        source_session_id: b.source_session_id,
        source_stage_id: b.source_stage_id,
        outcome: b.outcome,
      })),
    }));
  }

  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">${isNew ? "编排新的联合演练" : "编排编辑"}</h2>
        <span class="spacer"></span>
        <a href="#/host"><button class="ghost">← 返回控制台</button></a>
        ${!isNew ? `<a href="#/host/joint/${esc(jid)}"><button class="ghost">监控台</button></a>` : ""}
      </div>
      <label>联演名称</label>
      <input type="text" id="j-name" placeholder="如：化工厂泄漏—消防联动联演" />
      <label>编入联演的独立会话（至少 2 个；各会话保留各自剧本版本与事件记录）</label>
      <div id="member-picker" class="member-picker"></div>
    </div>
    <div class="card">
      <h2>依赖与注入规则</h2>
      <p class="muted small">每条规则：选择<strong>目标会话/阶段</strong>与按角色推送的注入内容；
        再添加若干<strong>前置分支</strong>（上游会话阶段达到指定结局）。
        多个分支构成 AND 屏障，<strong>全部满足</strong>才释放一次；
        结局 <code>@end</code> 表示该阶段结算后整场演练结束。保存时统一校验无环与有效性。</p>
      <div id="rules"></div>
      <div class="row" style="margin-top:10px">
        <button id="add-rule" class="ghost">＋ 添加规则</button>
        <span class="spacer"></span>
        <button id="save-joint">💾 保存联演编排</button>
      </div>
      <p id="save-msg" class="small" style="margin-top:8px"></p>
    </div>
  </div>`);
  app.innerHTML = "";
  app.appendChild(root);

  const st = state0;
  root.querySelector("#j-name").value = st.name;

  // member checkboxes
  const picker = root.querySelector("#member-picker");
  if (!sessions.length) {
    picker.innerHTML = `<p class="muted small">还没有会话。请先在控制台基于已发布剧本创建。</p>`;
  }
  for (const s of sessions) {
    const cb = el(`<label class="member-chip">
      <input type="checkbox" value="${esc(s.id)}" ${st.memberIds.includes(s.id) ? "checked" : ""}/>
      <span class="mono">${esc(s.id)}</span>
      <span>${esc(s.drill_name || s.drill_id)} v${esc(s.drill_version)}</span>
      <span class="chip ${statusChip(s.status)[1]}">${statusChip(s.status)[0]}</span>
    </label>`);
    picker.appendChild(cb);
  }
  const selectedMembers = () =>
    [...picker.querySelectorAll("input:checked")].map((c) => c.value);

  // ensure every referenced session outline is loaded for option building
  async function ensureOutlines(ids) {
    await Promise.all(ids.map((sid) => outline(sid).catch(() => null)));
  }

  const rulesWrap = root.querySelector("#rules");

  function stageOptions(sid, selectedStage) {
    const o = outlineCache[sid];
    if (!o) return `<option value="">（加载中）</option>`;
    return o.stages.map((s2) =>
      `<option value="${esc(s2.id)}" ${s2.id === selectedStage ? "selected" : ""}>
        ${esc(s2.title)}（${esc(s2.id)}）</option>`).join("");
  }
  function outcomeOptions(sid, stageId, selected) {
    const o = outlineCache[sid];
    const stg = o && o.stages.find((x) => x.id === stageId);
    if (!stg) return `<option value="">—</option>`;
    const outs = stg.outcomes.includes(END_OUTCOME)
      ? stg.outcomes
      : [...stg.outcomes, END_OUTCOME];
    return outs.map((oc) =>
      `<option value="${esc(oc)}" ${oc === selected ? "selected" : ""}>
        ${oc === END_OUTCOME ? "@end（演练结束）" : esc(oc)}</option>`).join("");
  }

  function renderRules() {
    rulesWrap.innerHTML = "";
    st.rules.forEach((rule, ri) => rulesWrap.appendChild(ruleCard(rule, ri)));
  }

  function ruleCard(rule, ri) {
    const node = el(`<div class="box rule-card">
      <div class="row">
        <strong>规则 ${ri + 1}</strong>
        ${rule.id ? `<span class="chip mono" title="稳定规则标识">${esc(rule.id)}</span>` : ""}
        <span class="spacer"></span>
        <button class="ghost danger" data-act="del-rule">删除规则</button>
      </div>
      <label>规则标题（注入标题）</label>
      <input type="text" data-f="title" placeholder="如：跨厂区增援通报" />
      <div class="grid2">
        <div><label>目标会话</label><select data-f="target_session_id"></select></div>
        <div><label>目标阶段</label><select data-f="target_stage_id"></select></div>
      </div>
      <label>公开注入文本（目标会话全员可见，可留空）</label>
      <textarea data-f="public" rows="2" placeholder="注入到目标阶段的公开通报"></textarea>
      <div class="row" style="margin-top:6px">
        <strong class="small">按角色推送（仅该角色可见）</strong>
        <span class="spacer"></span>
        <button class="ghost small" data-act="add-view">＋ 添加角色密报</button>
      </div>
      <div data-views></div>
      <div class="row" style="margin-top:10px">
        <strong class="small">前置分支（全部满足才释放）</strong>
        <span class="spacer"></span>
        <button class="ghost small" data-act="add-branch">＋ 添加前置分支</button>
      </div>
      <div data-branches></div>
    </div>`);
    node.querySelector('[data-f="title"]').value = rule.title || "";
    node.querySelector('[data-f="public"]').value = rule.public || "";

    const memberIds = selectedMembers();
    const tSel = node.querySelector('[data-f="target_session_id"]');
    tSel.innerHTML = `<option value="">选择目标会话…</option>` +
      memberIds.map((sid) =>
        `<option value="${esc(sid)}" ${sid === rule.target_session_id ? "selected" : ""}>${esc(sid)}</option>`
      ).join("");
    const stageSel = node.querySelector('[data-f="target_stage_id"]');

    const rebuildStageSel = () => {
      const sid = tSel.value;
      stageSel.innerHTML = sid ? stageOptions(sid, rule.target_stage_id)
        : `<option value="">先选目标会话</option>`;
      rule.target_session_id = sid;
      rule.target_stage_id = stageSel.value || "";
      renderViews();
    };
    tSel.onchange = rebuildStageSel;
    stageSel.onchange = () => { rule.target_stage_id = stageSel.value; renderViews(); };

    // role-targeted views, based on target session's pinned roles
    const viewsWrap = node.querySelector("[data-views]");
    function renderViews() {
      viewsWrap.innerHTML = "";
      const o = outlineCache[rule.target_session_id];
      const roles = o ? o.roles : [];
      rule.views.forEach((v, vi) => {
        const row = el(`<div class="view-row">
          <div class="grid2">
            <select data-v="role"></select>
            <button class="ghost danger small" data-v="del">移除</button>
          </div>
          <textarea data-v="text" rows="2" placeholder="仅该角色可见的注入文本"></textarea>
        </div>`);
        row.querySelector('[data-v="role"]').innerHTML =
          roles.map((r) =>
            `<option value="${esc(r.id)}" ${r.id === v.role_id ? "selected" : ""}>${esc(r.name)}（${esc(r.id)}）</option>`
          ).join("") || `<option value="">（目标会话角色加载中）</option>`;
        row.querySelector('[data-v="text"]').value = v.text || "";
        row.querySelector('[data-v="role"]').onchange = (e) => { v.role_id = e.target.value; };
        row.querySelector('[data-v="text"]').oninput = (e) => { v.text = e.target.value; };
        row.querySelector('[data-v="del"]').onclick = () => {
          rule.views.splice(vi, 1); renderViews();
        };
        viewsWrap.appendChild(row);
      });
    }
    node.querySelector('[data-act="add-view"]').onclick = () => {
      const o = outlineCache[rule.target_session_id];
      if (!o) return toast("请先选择目标会话");
      rule.views.push({ role_id: o.roles[0]?.id || "", text: "" });
      renderViews();
    };

    // branches
    const brWrap = node.querySelector("[data-branches]");
    function renderBranches() {
      brWrap.innerHTML = "";
      rule.branches.forEach((b, bi) => {
        const row = el(`<div class="box branch-row">
          <div class="row small muted">分支 ${bi + 1}：当上游会话阶段结算到指定结局
            <span class="spacer"></span>
            <button class="ghost danger small" data-b="del">移除</button></div>
          <div class="grid3">
            <div><label>上游会话</label><select data-b="session"></select></div>
            <div><label>上游阶段</label><select data-b="stage"></select></div>
            <div><label>指定结局</label><select data-b="outcome"></select></div>
          </div>
          <p data-b="hint" class="muted small" style="margin:4px 0 0"></p>
        </div>`);
        const sSel = row.querySelector('[data-b="session"]');
        const stSel = row.querySelector('[data-b="stage"]');
        const oSel = row.querySelector('[data-b="outcome"]');
        const hint = row.querySelector('[data-b="hint"]');
        sSel.innerHTML = `<option value="">选择上游会话…</option>` +
          memberIds.map((sid) =>
            `<option value="${esc(sid)}" ${sid === b.source_session_id ? "selected" : ""}>${esc(sid)}</option>`
          ).join("");
        const rebuildStages = () => {
          const sid = sSel.value;
          stSel.innerHTML = sid ? stageOptions(sid, b.source_stage_id)
            : `<option value="">先选上游会话</option>`;
          b.source_session_id = sid;
          rebuildOutcomes();
        };
        const rebuildOutcomes = () => {
          const sid = sSel.value, stgId = stSel.value;
          oSel.innerHTML = (sid && stgId) ? outcomeOptions(sid, stgId, b.outcome)
            : `<option value="">—</option>`;
          b.source_stage_id = stgId || "";
          b.outcome = oSel.value || "";
          if (sid && stgId) {
            const o = outlineCache[sid];
            const stg = o && o.stages.find((x) => x.id === stgId);
            hint.textContent = stg
              ? `可产生结局：${stg.outcomes.map((x) => x === END_OUTCOME ? "@end" : x).join("、")}`
              : "";
          } else hint.textContent = "";
        };
        sSel.onchange = rebuildStages;
        stSel.onchange = rebuildOutcomes;
        oSel.onchange = () => { b.outcome = oSel.value; };
        row.querySelector('[data-b="del"]').onclick = () => {
          rule.branches.splice(bi, 1); renderBranches();
        };
        brWrap.appendChild(row);
        // populate dependent dropdowns if editing
        if (b.source_session_id) { rebuildStages(); stSel.value = b.source_stage_id; rebuildOutcomes(); oSel.value = b.outcome; }
      });
    }
    node.querySelector('[data-act="add-branch"]').onclick = () => {
      rule.branches.push({ source_session_id: "", source_stage_id: "", outcome: "" });
      renderBranches();
    };
    node.querySelector('[data-act="del-rule"]').onclick = () => {
      if (!confirm("删除这条规则？未投递规则的等待状态将一并清除。")) return;
      st.rules.splice(ri, 1); renderRules();
    };
    node.querySelector('[data-f="title"]').oninput = (e) => { rule.title = e.target.value; };
    node.querySelector('[data-f="public"]').oninput = (e) => { rule.public = e.target.value; };

    rulesWrap.appendChild(node);
    // async populate once the node is in DOM
    rebuildStageSel();
    if (rule.target_session_id) { stageSel.value = rule.target_stage_id; renderViews(); }
    renderBranches();
    return node;
  }

  root.querySelector("#add-rule").onclick = () => {
    st.rules.push({
      id: null, target_session_id: "", target_stage_id: "",
      title: "", public: "", views: [], branches: [],
    });
    renderRules();
  };

  // Re-render rules when membership changes (session option lists change).
  picker.addEventListener("change", async () => {
    const ids = selectedMembers();
    await ensureOutlines(ids);
    // drop rule selections referencing sessions no longer enrolled
    for (const r of st.rules) {
      if (r.target_session_id && !ids.includes(r.target_session_id)) {
        r.target_session_id = ""; r.target_stage_id = ""; r.views = [];
      }
      r.branches = r.branches.filter((b) => ids.includes(b.source_session_id));
    }
    renderRules();
  });

  root.querySelector("#save-joint").onclick = async () => {
    const msg = root.querySelector("#save-msg");
    const ids = selectedMembers();
    const payload = {
      name: root.querySelector("#j-name").value.trim(),
      session_ids: ids,
      rules: st.rules.map((r) => ({
        ...(r.id ? { id: r.id } : {}),
        target_session_id: r.target_session_id,
        target_stage_id: r.target_stage_id,
        title: (r.title || "").trim(),
        public: r.public || "",
        views: Object.fromEntries(
          r.views.filter((v) => v.role_id && (v.text || "").trim())
                 .map((v) => [v.role_id, v.text])
        ),
        branches: r.branches.map((b) => ({
          source_session_id: b.source_session_id,
          source_stage_id: b.source_stage_id,
          outcome: b.outcome,
        })),
      })),
    };
    msg.innerHTML = '<span class="muted">校验并保存中…</span>';
    try {
      const res = isNew
        ? await api("/api/joints", { method: "POST", host: true, body: payload })
        : await api(`/api/joints/${encodeURIComponent(jid)}`, { method: "PUT", host: true, body: payload });
      msg.innerHTML = '<span class="chip ok">已保存</span>';
      toast("联合演练编排已保存", "ok");
      location.hash = `#/host/joint/${res.id}`;
    } catch (e) {
      msg.innerHTML = `<span class="chip danger">${esc(e.message)}</span>`;
      toast(e.message, "error");
    }
  };

  // boot: load outlines for preselected members then render
  await ensureOutlines(st.memberIds);
  renderRules();
}

// ---------- live monitor ---------- //
function jointMonitorPage(jid) {
  app.innerHTML = `<div class="card"><h2>联演监控台 <span class="mono">${esc(jid)}</span></h2>
    <p id="connline" class="muted">正在建立实时连接…</p></div>`;

  liveHandle = new JointLive(jid, render, (s, delay) => {
    const line = document.getElementById("connline");
    if (!line) return;
    line.innerHTML = s === "reconnecting"
      ? `<span class="conn off">● 断开，${(delay / 1000).toFixed(0)}s 后重连…</span>`
      : s === "open" ? '<span class="conn on">● 已连接</span>'
      : '<span class="muted">● 连接中…</span>';
  });

  function render(j) {
    const root = el(`<div>
      <div class="card">
        <div class="row">
          <h2 style="margin:0">🔗 ${esc(j.name)}</h2>
          <span class="chip mono">${esc(j.id)}</span>
          <span class="spacer"></span>
          <a href="#/host/joint/${esc(j.id)}/edit"><button class="ghost">编排编辑</button></a>
          <a href="#/host"><button class="ghost">← 控制台</button></a>
        </div>
        <p class="muted small" style="margin-bottom:0">
          成员会话 ${j.summary.members} 个 · 规则 ${j.summary.rules} 条 ·
          <span class="chip ok">已投递 ${j.summary.delivered}</span>
          <span class="chip warn">等待中 ${j.summary.waiting}</span>
          每个成员会话保留各自的剧本版本与独立事件记录；注入仅进入目标会话的事件日志。
        </p>
      </div>
      <div class="card"><h2>成员会话</h2><div id="sessions" class="session-grid"></div></div>
      <div class="card"><h2>分支等待与阻塞原因</h2><div id="rules"></div></div>
    </div>`);

    const sg = root.querySelector("#sessions");
    for (const s of j.sessions) {
      const [lbl, cls] = statusChip(s.status);
      sg.appendChild(el(`<a class="box sess-box" href="#/host/session/${esc(s.id)}">
        <div class="row"><strong class="mono">${esc(s.id)}</strong>
          <span class="chip ${cls}">${lbl}</span></div>
        <div class="small">${esc(s.drill_name || s.drill_id || "?")}
          <span class="chip mono">v${esc(s.drill_version ?? "?")}</span></div>
        <div class="muted small">${s.current_stage ? `当前阶段：${esc(s.current_stage)}` : "无开放阶段"}</div>
      </a>`));
    }

    const rw = root.querySelector("#rules");
    if (!j.rules.length) rw.innerHTML = `<p class="muted small">暂无规则。</p>`;
    for (const r of j.rules) {
      const [slabel, scls] = jointStatusChip(r.state);
      const card = el(`<div class="box rule-mon">
        <div class="row">
          <strong>${esc(r.title)}</strong>
          <span class="chip ${scls}">${slabel}</span>
          ${r.delivery?.late ? '<span class="chip warn">迟到补投</span>' : ""}
          <span class="spacer"></span>
          <a href="#/host/session/${esc(r.target_session_id)}"><button class="ghost small">目标会话 ${esc(r.target_session_id)}</button></a>
        </div>
        <div class="muted small">注入到
          <b>${esc(r.target_session_id)}</b> 的阶段
          <b>「${esc(r.target_stage_title)}」</b>
          （${r.target_sealed ? "已结算" : r.target_reached ? "已到达/开放" : "尚未到达"}）</div>
        <div class="muted small" style="color:var(--warn)">${esc(r.state_reason)}</div>
        <table><thead><tr><th>#</th><th>上游会话/阶段</th><th>要求结局</th><th>状态</th><th>阻塞原因</th></tr></thead>
        <tbody></tbody></table>
        ${r.delivery ? `<div class="small muted">已于 v${r.delivery.version} 投递 · ${fmtTime(r.delivery.at)}</div>` : ""}
      </div>`);
      const tb = card.querySelector("tbody");
      r.branches.forEach((b, i) => {
        const ok = b.state === "matched";
        tb.appendChild(el(`<tr>
          <td>${i + 1}</td>
          <td><a href="#/host/session/${esc(b.source_session_id)}"><span class="mono">${esc(b.source_session_id)}</span></a>
              ·「${esc(b.source_stage_title)}」</td>
          <td class="mono">${esc(b.outcome === END_OUTCOME ? "@end（演练结束）" : b.outcome)}</td>
          <td>${ok
            ? `<span class="chip ok">✓ 已满足 v${b.matched_version}</span>`
            : '<span class="chip warn">等待</span>'}</td>
          <td class="small ${ok ? "muted" : ""}">${ok ? `于 ${fmtTime(b.matched_at)} 命中` : esc(b.block_reason)}</td>
        </tr>`));
      });
      rw.appendChild(card);
    }

    app.innerHTML = "";
    app.appendChild(root);
  }
}

// --------------------------------------------------------------------------- //
// deterministic replay
// --------------------------------------------------------------------------- //
async function replayPage(sid) {
  app.innerHTML = `<div class="card"><h2>确定性回放 <span class="mono">${esc(sid)}</span></h2>
    <p class="muted">正在加载事件日志…</p></div>`;

  const cred = getCred(sid);
  let data = null;
  let asRole = false;
  try {
    data = await api(`/api/sessions/${sid}/replay`, { host: true });
  } catch (e) {
    if (cred) {
      try {
        data = await fetch(`/api/sessions/${sid}/replay?token=${encodeURIComponent(cred.token)}`).then((r) => {
          if (!r.ok) throw new Error("无权重放");
          return r.json();
        });
        asRole = true;
      } catch { /* fallthrough */ }
    }
    if (!data) {
      app.innerHTML = "";
      app.appendChild(el(`<div class="card"><h2>回放</h2>
        <p class="chip danger">${esc(e.message)}</p>
        <p class="muted small">主持人请在首页输入正确的 HOST_KEY；参与者请先从邀请链接加入。</p></div>`));
      return;
    }
  }

  const frames = data.frames;
  let idx = frames.length - 1;

  const card = el(`<div class="card">
    <div class="row">
      <h2 style="margin:0">${esc(data.drill_name)}</h2>
      <span class="chip ${asRole ? "warn" : "host"}">${asRole ? "个人视角回放（已按角色裁剪）" : "主持人视角回放（全量）"}</span>
      <span class="spacer"></span>
      <span class="chip mono" title="会话创建时固化的剧本版本与内容摘要">
        剧本 v${esc(data.drill_version)} · ${esc((data.drill_sha256 || "").slice(0, 12))}
      </span>
      <span class="muted small">会话最终版本 v${data.final_version}</span>
    </div>
    <p class="muted small" style="margin:6px 0 12px">
      每个会话版本号对应一次原子提交。事件日志不可变，任意版本的快照都可被确定性重建；
      本回放始终使用会话绑定的剧本版本，不受后续剧本编辑影响。
    </p>
    <div id="frame"></div>
  </div>
  <div id="replay-bar">
    <button class="ghost" id="prev">◀</button>
    <input type="range" id="slider" min="0" max="${frames.length - 1}" value="${idx}" />
    <button class="ghost" id="next">▶</button>
    <span class="mono small" id="vlabel">v${idx}</span>
  </div>`);

  app.innerHTML = "";
  app.appendChild(card);

  function renderFrame() {
    const f = frames[idx];
    const wrap = card.querySelector("#frame");
    const changes = f.changes.length
      ? f.changes.map((c) => `<div class="logline">· ${fmtTime(c.at)} ${esc(c.label || c.type)}</div>`).join("")
      : '<div class="logline muted">· （会话已创建，尚未开始）</div>';

    const snap = f.snapshot;
    const s = snap.session;
    const hostStage = (stg) => {
      const choices = (stg.deciding_roles || []).map((rid) => {
        const c = stg.choices?.[rid];
        const text = !stg.reached ? "—"
          : stg.kind === "sealed" && !c ? "⏱ 超时"
          : c ? esc(c.label || c.id) : "待提交";
        return `<li>${esc(roleName(snap, rid))}：${text}</li>`;
      }).join("");
      const injHtml = (stg.injections || []).map((inj) => `
        <div class="box injection small">
          <div class="box-label">🔗 联演注入 · ${esc(inj.joint_name)}${inj.late ? "（迟到补投）" : ""}</div>
          <strong>${esc(inj.title)}</strong>
          <div>${esc(inj.public)}</div>
          ${Object.entries(inj.views || {}).map(([rid, t]) =>
            `<div class="small">仅 ${esc(roleName(snap, rid))}：${esc(t)}</div>`).join("")}
        </div>`).join("");
      return `<div class="stage ${stg.kind}">
        <div class="stage-title">${esc(stg.title)} <span class="chip mono">${esc(stg.stage_id)}</span></div>
        <div class="box public">${esc(stg.public)}</div>
        ${injHtml}
        ${choices ? `<ul class="small" style="margin:6px 0">${choices}</ul>` : ""}
      </div>`;
    };
    const roleStage = (stg) => {
      let mine = "";
      if (stg.deciding) {
        mine = stg.timed_out ? "⏱ 超时未提交"
          : stg.my_choice ? esc(stg.my_choice_label || stg.my_choice) : "未提交";
      }
      const injHtml = (stg.injections || []).map((inj) => {
        const priv = (inj.views || {})[snap.me?.role_id || ""] || "";
        return `<div class="box injection small">
          <div class="box-label">🔗 联演注入${inj.late ? "（迟到补投）" : ""}</div>
          <strong>${esc(inj.title)}</strong>
          <div>${esc(inj.public)}</div>
          ${priv ? `<div class="small">仅您可见：${esc(priv)}</div>` : ""}
        </div>`;
      }).join("");
      return `<div class="stage ${stg.kind}">
        <div class="stage-title">${esc(stg.title)}</div>
        <div class="box public">${esc(stg.public)}</div>
        ${injHtml}
        ${stg.private ? `<div class="box private">${esc(stg.private)}</div>` : ""}
        ${mine ? `<div class="small">我的决定：${mine}</div>` : ""}
      </div>`;
    };
    const stagesHtml = (snap.history || [])
      .map((stg) => (snap.kind === "host" ? hostStage(stg) : roleStage(stg)))
      .join("");

    wrap.innerHTML = `
      <h3>v${f.version} 的事件</h3>${changes}
      <h3 style="margin-top:16px">该版本时刻的${snap.kind === "host" ? "全量" : "角色"}快照
        <span class="chip ${s.status === "ended" ? "danger" : s.status === "running" ? "ok" : "warn"}">
          ${esc(statusChip(s.status)[0])}</span></h3>
      ${stagesHtml || '<p class="muted small">无</p>'}`;
    card.querySelector("#vlabel").textContent = `v${f.version} / v${data.final_version}`;
    card.querySelector("#slider").value = idx;
  }

  card.querySelector("#slider").oninput = (e) => { idx = Number(e.target.value); renderFrame(); };
  card.querySelector("#prev").onclick = () => { idx = Math.max(0, idx - 1); renderFrame(); };
  card.querySelector("#next").onclick = () => { idx = Math.min(frames.length - 1, idx + 1); renderFrame(); };
  renderFrame();
}

// boot
route();
