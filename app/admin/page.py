def render_admin_page() -> str:
    return """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Crypto Admin</title>
    <style>
      :root {
        --bg: #0c1117;
        --panel: #131b24;
        --panel-2: #1a2531;
        --line: #253446;
        --text: #eef3f8;
        --muted: #95a6b8;
        --ok: #3ecf8e;
        --warn: #ffb84d;
        --bad: #ff6b6b;
        --accent: #77d0ff;
        --accent-2: #b2ffcc;
        --shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
      }

      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(119, 208, 255, 0.16), transparent 28%),
          radial-gradient(circle at top right, rgba(178, 255, 204, 0.12), transparent 20%),
          linear-gradient(180deg, #0a0f14 0%, #0f1720 100%);
      }

      .shell {
        max-width: 1280px;
        margin: 0 auto;
        padding: 32px 20px 48px;
      }

      .hero {
        display: grid;
        grid-template-columns: 1.4fr 0.9fr;
        gap: 20px;
        margin-bottom: 20px;
      }

      .panel {
        background: linear-gradient(180deg, rgba(19, 27, 36, 0.96), rgba(14, 20, 28, 0.96));
        border: 1px solid var(--line);
        border-radius: 22px;
        box-shadow: var(--shadow);
      }

      .hero-main {
        padding: 28px;
      }

      .hero-side {
        padding: 24px;
        display: flex;
        flex-direction: column;
        gap: 14px;
      }

      .eyebrow {
        color: var(--accent-2);
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-size: 12px;
        margin-bottom: 10px;
      }

      h1 {
        font-size: clamp(32px, 5vw, 56px);
        line-height: 0.95;
        margin: 0 0 12px;
      }

      .subtitle {
        color: var(--muted);
        max-width: 60ch;
        margin: 0 0 20px;
      }

      .status-strip {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }

      .chip {
        border: 1px solid var(--line);
        border-radius: 999px;
        padding: 8px 12px;
        font-size: 13px;
        color: var(--muted);
        background: rgba(255, 255, 255, 0.02);
      }

      .chip strong { color: var(--text); }
      .ok { color: var(--ok); }
      .warn { color: var(--warn); }
      .bad { color: var(--bad); }

      .side-stat {
        padding: 14px 16px;
        border-radius: 16px;
        background: var(--panel-2);
        border: 1px solid rgba(255, 255, 255, 0.04);
      }

      .side-stat label {
        display: block;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
      }

      .side-stat .value {
        font-size: 20px;
        font-weight: 700;
      }

      .controls {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 20px;
        margin-bottom: 20px;
      }

      .control-card {
        padding: 22px;
      }

      .control-card h2,
      .data-card h2 {
        margin: 0 0 8px;
        font-size: 20px;
      }

      .control-card p,
      .data-card p {
        margin: 0 0 16px;
        color: var(--muted);
      }

      .button-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }

      button {
        border: 0;
        border-radius: 12px;
        padding: 12px 16px;
        font: inherit;
        font-weight: 700;
        color: #08111a;
        background: linear-gradient(135deg, var(--accent), var(--accent-2));
        cursor: pointer;
        transition: transform 120ms ease, opacity 120ms ease;
      }

      button.secondary {
        color: var(--text);
        background: #223142;
      }

      button.danger {
        color: white;
        background: linear-gradient(135deg, #ff7a7a, #ff4d6d);
      }

      button:hover { transform: translateY(-1px); }
      button:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

      .message {
        margin-top: 12px;
        min-height: 20px;
        color: var(--muted);
        font-size: 14px;
      }

      .grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 20px;
      }

      .data-card {
        padding: 22px;
        min-height: 240px;
      }

      pre {
        margin: 0;
        padding: 16px;
        border-radius: 16px;
        background: #0b1219;
        border: 1px solid rgba(255, 255, 255, 0.04);
        color: #d2e5f7;
        font-size: 12px;
        line-height: 1.5;
        overflow: auto;
        max-height: 340px;
      }

      .footer-note {
        margin-top: 20px;
        color: var(--muted);
        font-size: 13px;
      }

      @media (max-width: 960px) {
        .hero,
        .controls,
        .grid {
          grid-template-columns: 1fr;
        }

        .shell {
          padding: 20px 14px 36px;
        }
      }
    </style>
  </head>
  <body>
    <main class="shell">
      <section class="hero">
        <div class="panel hero-main">
          <div class="eyebrow">Crypto Trading MVP</div>
          <h1>Admin Console</h1>
          <p class="subtitle">
            Monitor runtime state, inspect paper-trading records, and control the scheduler and kill
            switch without dropping into curl commands.
          </p>
          <div class="status-strip" id="status-strip">
            <div class="chip">Loading health...</div>
          </div>
        </div>
        <div class="panel hero-side">
          <div class="side-stat">
            <label>Health</label>
            <div class="value" id="health-status">Loading</div>
          </div>
          <div class="side-stat">
            <label>Scheduler</label>
            <div class="value" id="scheduler-status">Loading</div>
          </div>
          <div class="side-stat">
            <label>Kill Switch</label>
            <div class="value" id="kill-switch-status">Loading</div>
          </div>
          <div class="side-stat">
            <label>Last Refresh</label>
            <div class="value" id="last-refresh">Never</div>
          </div>
        </div>
      </section>

      <section class="controls">
        <div class="panel control-card">
          <h2>Pipeline</h2>
          <p>Run one full trading cycle and inspect the returned execution summary.</p>
          <div class="button-row">
            <button data-action="pipeline">Run Pipeline</button>
            <button class="secondary" data-refresh="all">Refresh Data</button>
          </div>
          <div class="message" id="pipeline-message"></div>
        </div>
        <div class="panel control-card">
          <h2>Scheduler</h2>
          <p>Pause or resume automatic execution without touching launchd state directly.</p>
          <div class="button-row">
            <button class="secondary" data-action="scheduler-start">Start</button>
            <button class="danger" data-action="scheduler-stop">Stop</button>
          </div>
          <div class="message" id="scheduler-message"></div>
        </div>
        <div class="panel control-card">
          <h2>Kill Switch</h2>
          <p>Block new pipeline executions immediately while keeping observability online.</p>
          <div class="button-row">
            <button class="danger" data-action="kill-enable">Enable</button>
            <button class="secondary" data-action="kill-disable">Disable</button>
          </div>
          <div class="message" id="kill-message"></div>
        </div>
      </section>

      <section class="grid">
        <article class="panel data-card">
          <h2>Health Report</h2>
          <p>Full health payload from the API.</p>
          <pre id="health-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Positions</h2>
          <p>Current position and realized PnL state.</p>
          <pre id="positions-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Orders</h2>
          <p>Latest paper-trading orders.</p>
          <pre id="orders-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>PnL Snapshots</h2>
          <p>Latest mark-to-market snapshots.</p>
          <pre id="pnl-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Scheduler Logs</h2>
          <p>Recent scheduler output lines.</p>
          <pre id="logs-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Pipeline Result</h2>
          <p>Last manual pipeline action run from this page.</p>
          <pre id="pipeline-json">No manual pipeline run yet.</pre>
        </article>
      </section>

      <div class="footer-note">
        Refreshes are manual on purpose. This page is for control and inspection, not unattended monitoring.
      </div>
    </main>

    <script>
      const el = (id) => document.getElementById(id);

      function formatJson(value) {
        return JSON.stringify(value, null, 2);
      }

      function statusClass(status) {
        if (status === "ok") return "ok";
        if (status === "degraded") return "warn";
        if (status === "error" || status === "blocked") return "bad";
        return "";
      }

      async function api(path, options = {}) {
        const response = await fetch(path, {
          headers: { "Content-Type": "application/json" },
          ...options,
        });
        const contentType = response.headers.get("content-type") || "";
        const payload = contentType.includes("application/json")
          ? await response.json()
          : await response.text();
        if (!response.ok) {
          throw new Error(typeof payload === "string" ? payload : JSON.stringify(payload));
        }
        return payload;
      }

      function updateHeadline(health) {
        el("health-status").textContent = health.status.toUpperCase();
        el("health-status").className = `value ${statusClass(health.status)}`;

        const scheduler = health.checks.scheduler;
        el("scheduler-status").textContent = scheduler.stopped ? "STOPPED" : scheduler.status.toUpperCase();
        el("scheduler-status").className = `value ${statusClass(scheduler.status)}`;

        const killSwitch = health.checks.kill_switch;
        el("kill-switch-status").textContent = killSwitch.enabled ? "ENABLED" : "DISABLED";
        el("kill-switch-status").className = `value ${statusClass(killSwitch.status)}`;

        el("last-refresh").textContent = new Date().toLocaleTimeString();

        const strip = el("status-strip");
        strip.innerHTML = "";
        const chips = [
          ["health", health.status],
          ["scheduler", scheduler.stopped ? "stopped" : scheduler.status],
          ["kill switch", killSwitch.enabled ? "enabled" : "disabled"],
          ["db", health.checks.database.status],
          ["candles", health.checks.candles.status],
        ];
        for (const [label, value] of chips) {
          const chip = document.createElement("div");
          chip.className = "chip";
          chip.innerHTML = `<strong>${label}</strong>: <span class="${statusClass(value)}">${value}</span>`;
          strip.appendChild(chip);
        }
      }

      async function refreshAll() {
        const [health, positions, orders, pnl, logs] = await Promise.all([
          api("/health"),
          api("/positions?limit=10"),
          api("/orders?limit=10"),
          api("/pnl?limit=10"),
          api("/scheduler/logs?lines=20"),
        ]);

        updateHeadline(health);
        el("health-json").textContent = formatJson(health);
        el("positions-json").textContent = formatJson(positions);
        el("orders-json").textContent = formatJson(orders);
        el("pnl-json").textContent = formatJson(pnl);
        el("logs-json").textContent = formatJson(logs);
      }

      async function runAction(type) {
        const messages = {
          pipeline: "pipeline-message",
          "scheduler-start": "scheduler-message",
          "scheduler-stop": "scheduler-message",
          "kill-enable": "kill-message",
          "kill-disable": "kill-message",
        };
        const target = el(messages[type]);
        target.textContent = "Running...";

        try {
          let result;
          if (type === "pipeline") {
            result = await api("/pipeline/run", { method: "POST" });
            el("pipeline-json").textContent = formatJson(result);
          } else if (type === "scheduler-start") {
            result = await api("/scheduler/start", { method: "POST" });
          } else if (type === "scheduler-stop") {
            result = await api("/scheduler/stop", { method: "POST" });
          } else if (type === "kill-enable") {
            result = await api("/kill-switch/enable", { method: "POST" });
          } else if (type === "kill-disable") {
            result = await api("/kill-switch/disable", { method: "POST" });
          }
          target.textContent = JSON.stringify(result);
          await refreshAll();
        } catch (error) {
          target.textContent = `Error: ${error.message}`;
        }
      }

      document.querySelectorAll("[data-action]").forEach((button) => {
        button.addEventListener("click", () => runAction(button.dataset.action));
      });

      document.querySelectorAll("[data-refresh]").forEach((button) => {
        button.addEventListener("click", refreshAll);
      });

      refreshAll().catch((error) => {
        el("health-json").textContent = `Failed to load data: ${error.message}`;
      });
    </script>
  </body>
</html>
"""
