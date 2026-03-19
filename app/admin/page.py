def render_admin_page() -> str:
    from app.core.settings import DEFAULT_STRATEGY_NAME
    from app.strategy.registry import list_registered_strategies

    strategy_options = "\n".join(
        (
            f'              <option value="{name}"'
            + (' selected' if name == DEFAULT_STRATEGY_NAME else '')
            + f">{name}</option>"
        )
        for name in list_registered_strategies()
    )
    html = """<!DOCTYPE html>
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

      .issue-strip {
        margin-top: 16px;
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

      .inline-note {
        margin-top: 6px;
        color: var(--muted);
        font-size: 12px;
        line-height: 1.4;
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

      .inline-controls {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 16px;
        flex-wrap: wrap;
      }

      select {
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px 12px;
        font: inherit;
        color: var(--text);
        background: #13202c;
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
        min-height: 120px;
        max-height: 220px;
        overflow: auto;
        padding: 14px 16px;
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.04);
        background: #0b1219;
        color: #d2e5f7;
        font-size: 12px;
        line-height: 1.5;
        white-space: pre-wrap;
        word-break: break-word;
        overflow-wrap: anywhere;
        font-family: "SFMono-Regular", "Menlo", monospace;
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

      .worker-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 16px;
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

      .auto-refresh {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-top: 16px;
        color: var(--muted);
        font-size: 13px;
      }

      @media (max-width: 960px) {
        .hero,
        .controls,
        .grid,
        .worker-grid {
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
          <div class="issue-strip" id="issue-strip">
            <div class="chip">Checking degraded reasons...</div>
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
            <div class="inline-note" id="scheduler-detail">Checking scheduler runtime state...</div>
          </div>
          <div class="side-stat">
            <label>Kill Switch</label>
            <div class="value" id="kill-switch-status">Loading</div>
          </div>
          <div class="side-stat">
            <label>Last Refresh</label>
            <div class="value" id="last-refresh">Never</div>
          </div>
          <div class="side-stat">
            <label>Alerts</label>
            <div class="value" id="alerts-status">Loading</div>
            <div class="inline-note" id="alerts-detail">Checking Telegram delivery state...</div>
          </div>
          <div class="side-stat">
            <label>Last Pipeline</label>
            <div class="value" id="pipeline-status">Loading</div>
            <div class="inline-note" id="pipeline-detail">Checking pipeline run summary...</div>
          </div>
          <div class="side-stat">
            <label>Market Data</label>
            <div class="value" id="market-data-status">Loading</div>
            <div class="inline-note" id="market-data-detail">Checking market data heartbeat...</div>
          </div>
          <div class="side-stat">
            <label>Alerting</label>
            <div class="value" id="alerting-runtime-status">Loading</div>
            <div class="inline-note" id="alerting-runtime-detail">Checking alerting heartbeat...</div>
          </div>
        </div>
      </section>

      <section class="controls">
        <div class="panel control-card">
          <h2>Pipeline</h2>
          <p>Run one full trading cycle and inspect the returned execution summary.</p>
          <div class="inline-controls">
            <label for="pipeline-strategy-select">Strategy</label>
            <select id="pipeline-strategy-select">
__STRATEGY_OPTIONS__
            </select>
          </div>
          <div class="button-row">
            <button data-action="pipeline">Run Pipeline</button>
            <button class="secondary" data-refresh="all">Refresh Data</button>
          </div>
          <div class="auto-refresh">
            <button class="secondary" data-action="auto-refresh-toggle">Pause Auto Refresh</button>
            <span id="auto-refresh-status">Auto refresh every 10 seconds.</span>
          </div>
          <div class="message" id="pipeline-message"></div>
        </div>
        <div class="panel control-card">
          <h2>Scheduler</h2>
          <p>Pause or resume automatic execution without touching launchd state directly.</p>
          <div class="inline-controls">
            <label for="scheduler-strategy-select">Active Strategy</label>
            <select id="scheduler-strategy-select" multiple size="2">
__STRATEGY_OPTIONS__
            </select>
            <button class="secondary" data-action="scheduler-strategy-save">Apply Strategy</button>
          </div>
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
          <h2>Runtime Heartbeats</h2>
          <p>Latest liveness records for scheduler, pipeline, market data, split workers, and alerting.</p>
          <div class="worker-grid">
            <div class="side-stat">
              <label>Data Worker</label>
              <div class="value" id="data-worker-status">Loading</div>
              <div class="inline-note" id="data-worker-detail">Checking data worker heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Strategy Worker</label>
              <div class="value" id="strategy-worker-status">Loading</div>
              <div class="inline-note" id="strategy-worker-detail">Checking strategy worker heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Execution Worker</label>
              <div class="value" id="execution-worker-status">Loading</div>
              <div class="inline-note" id="execution-worker-detail">Checking execution worker heartbeat...</div>
            </div>
          </div>
          <pre id="heartbeats-json">Loading...</pre>
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
          <div class="inline-controls">
            <label for="logs-mode-select">Mode</label>
            <select id="logs-mode-select">
              <option value="all">all</option>
              <option value="pipeline">pipeline</option>
              <option value="market-data-only">market-data-only</option>
              <option value="strategy-only">strategy-only</option>
              <option value="execution-only">execution-only</option>
            </select>
          </div>
          <pre id="logs-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Pipeline Result</h2>
          <p>Last manual pipeline action run from this page.</p>
          <pre id="pipeline-json">No manual pipeline run yet.</pre>
        </article>
        <article class="panel data-card">
          <h2>Audit Events</h2>
          <p>Recent structured events for pipeline, risk, scheduler, and kill switch actions.</p>
          <pre id="audit-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Alert Delivery</h2>
          <p>Telegram configuration and the latest delivery attempt recorded in audit events.</p>
          <div class="button-row" style="margin-bottom: 16px;">
            <button data-action="alert-test">Send Test Alert</button>
          </div>
          <div class="message" id="alerts-message">No test alert sent from this page yet.</div>
          <pre id="alerts-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Soak Validation</h2>
          <p>Record runtime validation snapshots and inspect the most recent soak history.</p>
          <div class="button-row" style="margin-bottom: 16px;">
            <button data-action="soak-record">Record Snapshot</button>
          </div>
          <div class="message" id="soak-message">No soak validation snapshot recorded from this page yet.</div>
          <pre id="soak-json">Loading...</pre>
        </article>
      </section>

      <div class="footer-note">
        Auto refresh runs every 10 seconds. Use Pause Auto Refresh before inspecting a fixed snapshot.
      </div>
    </main>

    <script>
      const el = (id) => document.getElementById(id);
      const AUTO_REFRESH_INTERVAL_MS = 10000;
      let autoRefreshTimer = null;
      let autoRefreshEnabled = true;
      let schedulerLogsMode = "all";

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
        const schedulerStrategy = window.__schedulerStrategyStatus || null;
        el("scheduler-detail").textContent = schedulerStrategy
          ? `active strategies: ${(schedulerStrategy.strategy_names || [schedulerStrategy.strategy_name]).join(", ")}`
          : "Scheduler strategy not loaded yet.";

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

        const issueStrip = el("issue-strip");
        issueStrip.innerHTML = "";
        const issues = Object.entries(health.checks)
          .filter(([, check]) => ["degraded", "error"].includes(check.status))
          .map(([name, check]) => ({
            name,
            reason: check.reason || "Status requires attention.",
            status: check.status,
          }));

        if (issues.length === 0) {
          const chip = document.createElement("div");
          chip.className = "chip";
          chip.innerHTML = `<strong>health issues</strong>: <span class="ok">none</span>`;
          issueStrip.appendChild(chip);
          return;
        }

        for (const issue of issues) {
          const chip = document.createElement("div");
          chip.className = "chip";
          chip.innerHTML =
            `<strong>${issue.name}</strong>: ` +
            `<span class="${statusClass(issue.status)}">${issue.reason}</span>`;
          issueStrip.appendChild(chip);
        }

        const heartbeatCheck = health.checks.heartbeats || { components: [] };
        const heartbeatMap = Object.fromEntries(
          (heartbeatCheck.components || []).map((item) => [item.component, item])
        );

        const marketData = heartbeatMap.market_data;
        el("market-data-status").textContent = marketData ? String(marketData.status).toUpperCase() : "NONE";
        el("market-data-status").className = `value ${statusClass(marketData ? marketData.status : "degraded")}`;
        el("market-data-detail").textContent = marketData
          ? `${marketData.last_seen_at} | ${marketData.message}`
          : "No market data heartbeat recorded yet.";

        const alertingRuntime = heartbeatMap.alerting;
        el("alerting-runtime-status").textContent = alertingRuntime
          ? String(alertingRuntime.status).toUpperCase()
          : "NONE";
        el("alerting-runtime-status").className = `value ${statusClass(alertingRuntime ? alertingRuntime.status : "degraded")}`;
        el("alerting-runtime-detail").textContent = alertingRuntime
          ? `${alertingRuntime.last_seen_at} | ${alertingRuntime.message}`
          : "No alerting heartbeat recorded yet.";

        const dataWorker = heartbeatMap.data_worker;
        el("data-worker-status").textContent = dataWorker
          ? String(dataWorker.status).toUpperCase()
          : "NONE";
        el("data-worker-status").className = `value ${statusClass(dataWorker ? dataWorker.status : "degraded")}`;
        el("data-worker-detail").textContent = dataWorker
          ? `${dataWorker.last_seen_at} | ${dataWorker.message}`
          : "No data worker heartbeat recorded yet.";

        const strategyWorker = heartbeatMap.strategy_worker;
        el("strategy-worker-status").textContent = strategyWorker
          ? String(strategyWorker.status).toUpperCase()
          : "NONE";
        el("strategy-worker-status").className = `value ${statusClass(strategyWorker ? strategyWorker.status : "degraded")}`;
        el("strategy-worker-detail").textContent = strategyWorker
          ? `${strategyWorker.last_seen_at} | ${strategyWorker.message}`
          : "No strategy worker heartbeat recorded yet.";

        const executionWorker = heartbeatMap.execution_worker;
        el("execution-worker-status").textContent = executionWorker
          ? String(executionWorker.status).toUpperCase()
          : "NONE";
        el("execution-worker-status").className = `value ${statusClass(executionWorker ? executionWorker.status : "degraded")}`;
        el("execution-worker-detail").textContent = executionWorker
          ? `${executionWorker.last_seen_at} | ${executionWorker.message}`
          : "No execution worker heartbeat recorded yet.";

        const heartbeatIssues = (heartbeatCheck.components || [])
          .filter((item) => ["failed", "stopped"].includes(item.status))
          .map((item) => ({
            name: `heartbeat:${item.component}`,
            status: item.status,
            reason: item.message,
          }));

        for (const issue of heartbeatIssues) {
          const chip = document.createElement("div");
          chip.className = "chip";
          chip.innerHTML =
            `<strong>${issue.name}</strong>: ` +
            `<span class="${statusClass(issue.status)}">${issue.reason}</span>`;
          issueStrip.appendChild(chip);
        }
      }

      function updateAlerts(alertStatus, auditEvents) {
        const deliveries = auditEvents.filter((event) => event.event_type === "alert_delivery");
        const latest = deliveries[0] || null;
        const configured = Boolean(alertStatus.telegram_configured);
        const displayStatus = configured ? (latest ? latest.status.toUpperCase() : "READY") : "DISABLED";
        const displayClass =
          latest && latest.status === "failed"
            ? "bad"
            : configured
              ? "ok"
              : "warn";

        el("alerts-status").textContent = displayStatus;
        el("alerts-status").className = `value ${displayClass}`;
        el("alerts-detail").textContent = latest
          ? `${latest.created_at} | ${latest.message}`
          : configured
            ? "Telegram configured. No delivery attempts recorded yet."
            : "Telegram bot token or chat id is not configured.";

        el("alerts-json").textContent = formatJson({
          alert_status: alertStatus,
          latest_delivery: latest,
        });
      }

      function updatePipelineSummary(auditEvents) {
        const runs = auditEvents.filter((event) => event.event_type === "pipeline_run");
        const latestCompleted = runs.find((event) => event.status !== "started") || runs[0] || null;

        if (!latestCompleted) {
          el("pipeline-status").textContent = "NONE";
          el("pipeline-status").className = "value warn";
          el("pipeline-detail").textContent = "No pipeline runs recorded yet.";
          return;
        }

        const displayStatus = String(latestCompleted.status || "unknown").toUpperCase();
        el("pipeline-status").textContent = displayStatus;
        el("pipeline-status").className = `value ${statusClass(latestCompleted.status)}`;
        el("pipeline-detail").textContent = `${latestCompleted.created_at} | ${latestCompleted.message}`;
      }

      function updateSoakValidation(currentReport, history) {
        el("soak-json").textContent = formatJson({
          current_report: currentReport,
          recent_history: history,
        });
      }

      function updateHeartbeats(health) {
        const heartbeatCheck = health.checks.heartbeats || { components: [] };
        const lines = (heartbeatCheck.components || []).map((item) =>
          `${item.component} | ${String(item.status).toUpperCase()} | ${item.last_seen_at} | ${item.message}`
        );
        el("heartbeats-json").textContent = lines.length ? lines.join("\\n") : "No runtime heartbeats recorded yet.";
      }

      function updateAutoRefreshStatus() {
        const button = document.querySelector('[data-action="auto-refresh-toggle"]');
        if (!button) return;
        button.textContent = autoRefreshEnabled ? "Pause Auto Refresh" : "Resume Auto Refresh";
        el("auto-refresh-status").textContent = autoRefreshEnabled
          ? "Auto refresh every 10 seconds."
          : "Auto refresh paused.";
      }

      function scheduleAutoRefresh() {
        if (autoRefreshTimer) {
          clearInterval(autoRefreshTimer);
          autoRefreshTimer = null;
        }
        if (!autoRefreshEnabled) {
          updateAutoRefreshStatus();
          return;
        }
        autoRefreshTimer = setInterval(() => {
          refreshAll().catch((error) => {
            el("health-json").textContent = `Failed to load data: ${error.message}`;
          });
        }, AUTO_REFRESH_INTERVAL_MS);
        updateAutoRefreshStatus();
      }

      async function refreshAll() {
        schedulerLogsMode = el("logs-mode-select")?.value || "all";
        const [health, positions, orders, pnl, logs, auditEvents, alertStatus, soakReport, soakHistory, strategies, schedulerStrategy] = await Promise.all([
          api("/health"),
          api("/positions?limit=10"),
          api("/orders?limit=10"),
          api("/pnl?limit=10"),
          api(`/scheduler/logs?lines=20&mode=${encodeURIComponent(schedulerLogsMode)}`),
          api("/audit-events?limit=20"),
          api("/alerts/status"),
          api("/validation/soak"),
          api("/validation/soak/history?limit=10"),
          api("/strategies"),
          api("/scheduler/strategy"),
        ]);

        window.__schedulerStrategyStatus = schedulerStrategy;
        const strategySelect = el("pipeline-strategy-select");
        if (strategySelect && strategies?.default_strategy && !strategySelect.dataset.initialized) {
          strategySelect.value = strategies.default_strategy;
          strategySelect.dataset.initialized = "true";
        }
        const schedulerStrategySelect = el("scheduler-strategy-select");
        if (schedulerStrategySelect && schedulerStrategy?.strategy_names) {
          Array.from(schedulerStrategySelect.options).forEach((option) => {
            option.selected = schedulerStrategy.strategy_names.includes(option.value);
          });
        }
        updateHeadline(health);
        updateAlerts(alertStatus, auditEvents);
        updatePipelineSummary(auditEvents);
        updateSoakValidation(soakReport, soakHistory);
        updateHeartbeats(health);
        el("health-json").textContent = formatJson(health);
        el("positions-json").textContent = formatJson(positions);
        el("orders-json").textContent = formatJson(orders);
        el("pnl-json").textContent = formatJson(pnl);
        el("logs-json").textContent = formatJson(logs);
        el("audit-json").textContent = formatJson(auditEvents);
      }

      async function runAction(type) {
        const messages = {
          pipeline: "pipeline-message",
          "auto-refresh-toggle": "pipeline-message",
          "scheduler-start": "scheduler-message",
          "scheduler-stop": "scheduler-message",
          "scheduler-strategy-save": "scheduler-message",
          "kill-enable": "kill-message",
          "kill-disable": "kill-message",
          "alert-test": "alerts-message",
          "soak-record": "soak-message",
        };
        const target = el(messages[type]);
        target.textContent = "Running...";

        try {
          let result;
          if (type === "auto-refresh-toggle") {
            autoRefreshEnabled = !autoRefreshEnabled;
            scheduleAutoRefresh();
            result = {
              auto_refresh_enabled: autoRefreshEnabled,
              interval_seconds: AUTO_REFRESH_INTERVAL_MS / 1000,
            };
          } else if (type === "pipeline") {
            result = await api("/pipeline/run", {
              method: "POST",
              body: JSON.stringify({
                strategy_name: el("pipeline-strategy-select")?.value || "__DEFAULT_STRATEGY_NAME__",
              }),
            });
            el("pipeline-json").textContent = formatJson(result);
          } else if (type === "scheduler-start") {
            result = await api("/scheduler/start", { method: "POST" });
          } else if (type === "scheduler-stop") {
            result = await api("/scheduler/stop", { method: "POST" });
          } else if (type === "scheduler-strategy-save") {
            const selectedSchedulerStrategies = Array.from(
              el("scheduler-strategy-select")?.selectedOptions || []
            ).map((option) => option.value);
            result = await api("/scheduler/strategy", {
              method: "POST",
              body: JSON.stringify({
                strategy_names: selectedSchedulerStrategies.length
                  ? selectedSchedulerStrategies
                  : ["__DEFAULT_STRATEGY_NAME__"],
              }),
            });
          } else if (type === "kill-enable") {
            result = await api("/kill-switch/enable", { method: "POST" });
          } else if (type === "kill-disable") {
            result = await api("/kill-switch/disable", { method: "POST" });
          } else if (type === "alert-test") {
            result = await api("/alerts/test", {
              method: "POST",
              body: JSON.stringify({ message: "Crypto admin dashboard test alert." }),
            });
          } else if (type === "soak-record") {
            result = await api("/validation/soak/record", { method: "POST" });
          }
          target.textContent = formatJson(result);
          await refreshAll();
        } catch (error) {
          target.textContent = `Error:\\n${error.message}`;
        }
      }

      document.querySelectorAll("[data-action]").forEach((button) => {
        button.addEventListener("click", () => runAction(button.dataset.action));
      });

      document.querySelectorAll("[data-refresh]").forEach((button) => {
        button.addEventListener("click", refreshAll);
      });

      el("logs-mode-select")?.addEventListener("change", () => {
        refreshAll().catch((error) => {
          el("logs-json").textContent = `Failed to load logs: ${error.message}`;
        });
      });

      updateAutoRefreshStatus();
      scheduleAutoRefresh();
      refreshAll().catch((error) => {
        el("health-json").textContent = `Failed to load data: ${error.message}`;
      });
    </script>
  </body>
</html>
"""
    return (
        html.replace("__STRATEGY_OPTIONS__", strategy_options)
        .replace("__DEFAULT_STRATEGY_NAME__", DEFAULT_STRATEGY_NAME)
    )
