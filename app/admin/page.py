def render_admin_page() -> str:
    from app.core.settings import DEFAULT_PIPELINE_ORCHESTRATION
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
    closed_trade_strategy_options = "\n".join(
        ['              <option value="all">all</option>']
        + [f'              <option value="{name}">{name}</option>' for name in list_registered_strategies()]
    )
    pipeline_orchestration_options = "\n".join(
        (
            f'              <option value="{name}"'
            + (' selected' if name == DEFAULT_PIPELINE_ORCHESTRATION else '')
            + f">{name}</option>"
        )
        for name in ("direct", "queue_dispatch", "queue_drain", "queue_batch")
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

      input[type="number"] {
        width: 84px;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px 12px;
        font: inherit;
        color: var(--text);
        background: #13202c;
      }

      input[type="text"] {
        min-width: 180px;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px 12px;
        font: inherit;
        color: var(--text);
        background: #13202c;
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

      .strategy-board {
        display: grid;
        gap: 12px;
      }

      .strategy-card {
        border: 1px solid rgba(255, 255, 255, 0.05);
        background: #0b1219;
        border-radius: 16px;
        padding: 16px;
      }

      .strategy-card.clickable {
        cursor: pointer;
        transition: border-color 120ms ease, transform 120ms ease, background 120ms ease;
      }

      .strategy-card.clickable:hover {
        border-color: rgba(119, 208, 255, 0.5);
        background: #0e1620;
      }

      .strategy-card.selected {
        border-color: rgba(119, 208, 255, 0.85);
        box-shadow: inset 0 0 0 1px rgba(119, 208, 255, 0.25);
      }

      .strategy-card-header {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        gap: 10px;
        margin-bottom: 10px;
      }

      .strategy-card-header strong {
        font-size: 16px;
      }

      .strategy-card-actions {
        display: flex;
        align-items: center;
        gap: 8px;
      }

      .strategy-card-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px 12px;
        font-size: 12px;
      }

      .strategy-metric {
        color: var(--muted);
      }

      .strategy-metric strong {
        color: var(--text);
        display: block;
        margin-bottom: 2px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .trade-list {
        display: grid;
        gap: 10px;
      }

      .trade-row {
        border: 1px solid rgba(255, 255, 255, 0.05);
        background: #0b1219;
        border-radius: 14px;
        padding: 14px;
        display: grid;
        grid-template-columns: 1.2fr 1fr 1fr 1fr;
        gap: 10px;
        font-size: 12px;
      }

      .trade-row strong {
        display: block;
        margin-bottom: 4px;
        color: var(--text);
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
            <div class="inline-note" id="pipeline-symbols">Symbols: loading...</div>
            <div class="inline-note" id="pipeline-counts">Counts: loading...</div>
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
          <div class="side-stat">
            <label>Queue</label>
            <div class="value" id="queue-status">Loading</div>
            <div class="inline-note" id="queue-detail">Checking queued worker jobs...</div>
          </div>
          <div class="side-stat">
            <label>Execution Backend</label>
            <div class="value" id="execution-backend-status">Loading</div>
            <div class="inline-note" id="execution-backend-detail">Checking execution backend...</div>
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
          <div class="inline-controls">
            <label for="pipeline-symbol-select">Symbols</label>
            <select id="pipeline-symbol-select" multiple size="3"></select>
          </div>
          <div class="inline-controls">
            <label for="pipeline-orchestration-select">Orchestration</label>
            <select id="pipeline-orchestration-select">
__PIPELINE_ORCHESTRATION_OPTIONS__
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
          </div>
          <div class="inline-controls">
            <label for="scheduler-disabled-strategy-select">Disabled Strategy</label>
            <select id="scheduler-disabled-strategy-select" multiple size="2">
__STRATEGY_OPTIONS__
            </select>
          </div>
          <div class="inline-controls">
            <label for="scheduler-symbol-select">Active Symbols</label>
            <select id="scheduler-symbol-select" multiple size="3"></select>
            <button class="secondary" data-action="scheduler-strategy-save">Apply Strategy State</button>
          </div>
          <div class="inline-controls">
            <label for="execution-backend-select">Execution Backend</label>
            <select id="execution-backend-select">
              <option value="paper">paper</option>
              <option value="noop">noop</option>
              <option value="simulated_live">simulated_live</option>
              <option value="binance">binance</option>
            </select>
            <button class="secondary" data-action="execution-backend-save">Apply Execution Backend</button>
          </div>
          <div class="inline-controls" id="scheduler-priority-controls">
            <span class="chip">Priority: lower number runs first.</span>
          </div>
          <div class="inline-controls" id="scheduler-disabled-note-controls">
            <span class="chip">Disabled note: explain why a strategy is turned off.</span>
          </div>
          <div class="inline-controls">
            <button class="secondary" type="button" data-action="scheduler-clear-notes">Clear notes</button>
          </div>
          <div class="inline-controls">
            <label for="scheduler-effective-limit-input">Effective Limit</label>
            <input id="scheduler-effective-limit-input" type="number" step="1" min="1" placeholder="all" />
          </div>
          <div class="inline-controls">
            <span class="chip">Presets</span>
            <button class="secondary" type="button" data-action="scheduler-preset-top1">Apply top-1</button>
            <button class="secondary" type="button" data-action="scheduler-preset-top2">Apply top-2</button>
            <button class="secondary" type="button" data-action="scheduler-preset-all">All enabled</button>
            <button class="secondary" type="button" data-action="scheduler-priority-sequential">Sequential</button>
            <button class="secondary" type="button" data-action="scheduler-priority-reverse">Reverse</button>
            <button class="secondary" type="button" data-action="scheduler-priority-active-first">Active first</button>
            <button class="secondary" type="button" data-action="scheduler-reset-priorities">Reset priorities</button>
          </div>
          <div class="inline-note" id="scheduler-preset-detail">
            Limit presets change how many enabled strategies run. Priority presets reorder the scheduler execution sequence.
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
          <h2>Strategy Activity</h2>
          <p>Latest signal, risk, fills, and trade outcomes grouped by strategy.</p>
          <div class="inline-controls">
            <label for="strategy-sort-select">Sort</label>
            <select id="strategy-sort-select">
              <option value="gross_realized_pnl">gross pnl</option>
              <option value="winning_trade_count">wins</option>
              <option value="filled_order_count">filled orders</option>
              <option value="latest_activity_at">latest activity</option>
              <option value="latest_closed_pnl">latest closed pnl</option>
              <option value="realized_trade_count">realized trades</option>
              <option value="strategy_name">name</option>
            </select>
            <label for="strategy-filter-select">Filter</label>
            <select id="strategy-filter-select">
              <option value="all">all</option>
              <option value="active">active only</option>
              <option value="open_positions">open positions</option>
              <option value="winners">winners</option>
              <option value="fresh">fresh</option>
              <option value="stale">stale</option>
              <option value="idle">idle</option>
            </select>
          </div>
          <div class="strategy-board" id="strategy-summary-board">
            <div class="strategy-card">Loading...</div>
          </div>
        </article>
        <article class="panel data-card">
          <h2>Recent Closed Trades</h2>
          <p>Latest realized trade outcomes grouped by strategy.</p>
          <div class="inline-controls">
            <label for="closed-trades-strategy-select">Strategy</label>
            <select id="closed-trades-strategy-select">
__CLOSED_TRADE_STRATEGY_OPTIONS__
            </select>
            <button class="secondary" type="button" id="closed-trades-reset-button">Reset</button>
          </div>
          <div class="trade-list" id="strategy-closed-trades-board">
            <div class="strategy-card">Loading...</div>
          </div>
        </article>
        <article class="panel data-card">
          <h2>Selected Strategy Details</h2>
          <p>Focused detail view for the strategy currently selected from the leaderboard or closed-trades filter.</p>
          <div class="strategy-board" id="selected-strategy-board">
            <div class="strategy-card">Select a strategy card to inspect a single strategy.</div>
          </div>
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
          <h2>Control Activity</h2>
          <p>Recent scheduler and execution backend operations extracted from structured audit actions.</p>
          <div class="inline-controls" id="scheduler-preset-quick-actions">
            <span class="chip">Recent presets loading...</span>
          </div>
          <div class="inline-controls">
            <label for="scheduler-control-filter-select">Filter</label>
            <select id="scheduler-control-filter-select">
              <option value="all">all</option>
              <option value="priority">priority</option>
              <option value="limit">limit</option>
              <option value="enable_disable">enable/disable</option>
            </select>
            <button class="secondary" type="button" id="scheduler-control-reset-button">Reset</button>
          </div>
          <div class="trade-list" id="scheduler-control-board">
            <div class="strategy-card">Loading...</div>
          </div>
        </article>
        <article class="panel data-card">
          <h2>Queue Summary</h2>
          <p>Queued worker job counts and the latest queue entries.</p>
          <div class="inline-controls">
            <label for="queue-filter-select">Filter</label>
            <select id="queue-filter-select">
              <option value="all">all</option>
              <option value="failed">failed only</option>
              <option value="queued">queued only</option>
              <option value="market_data">market_data</option>
              <option value="strategy">strategy</option>
              <option value="execution">execution</option>
            </select>
          </div>
          <div class="button-row" style="margin-bottom: 16px;">
            <button class="secondary" data-action="queue-recover-pipeline">Recover Stale Pipeline Batch</button>
            <button class="secondary" data-action="queue-clear-pipeline">Clear Stale Pipeline Batch</button>
            <button class="secondary" data-action="queue-enqueue-strategy">Enqueue Strategy Job</button>
            <button class="secondary" data-action="queue-drain-strategy">Drain Strategy Job</button>
            <button class="secondary" data-action="queue-drain-execution">Drain Execution Job</button>
            <button class="secondary" data-action="queue-retry-strategy">Retry Failed Strategy Job</button>
            <button class="secondary" data-action="queue-retry-execution">Retry Failed Execution Job</button>
          </div>
          <div class="inline-note" style="margin-bottom: 12px;">Pipeline dispatch/drain now lives in the Pipeline panel via the orchestration selector.</div>
          <div class="message" id="queue-message">No queue action triggered from this page yet.</div>
          <div class="trade-list" id="queue-board">
            <div class="strategy-card">Loading...</div>
          </div>
          <pre id="queue-json">Loading...</pre>
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
      let schedulerControlFilterMode = "all";
      let strategySortMode = "gross_realized_pnl";
      let strategyFilterMode = "all";
      let closedTradesStrategyFilter = "all";
      let queueSummaryState = null;
      let queueFilterMode = "all";
      const STRATEGY_STALE_AFTER_MINUTES = 15;

      function formatJson(value) {
        return JSON.stringify(value, null, 2);
      }

      function statusClass(status) {
        if (status === "ok") return "ok";
        if (status === "degraded") return "warn";
        if (status === "error" || status === "blocked") return "bad";
        return "";
      }

      function parseDashboardTimestamp(value) {
        if (!value || typeof value !== "string") return null;
        const normalized = value.includes("T") ? value : value.replace(" ", "T");
        const parsed = new Date(normalized);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
      }

      function classifyStrategyActivity(item) {
        if (!item.has_activity || !item.latest_activity_at) {
          return { label: "IDLE", className: "warn" };
        }
        const latestActivityAt = parseDashboardTimestamp(item.latest_activity_at);
        if (!latestActivityAt) {
          return { label: "ACTIVE", className: "ok" };
        }
        const ageMinutes = (Date.now() - latestActivityAt.getTime()) / 60000;
        if (ageMinutes <= STRATEGY_STALE_AFTER_MINUTES) {
          return { label: "FRESH", className: "ok" };
        }
        return { label: "STALE", className: "warn" };
      }

      function matchesStrategyFilter(item, activityState) {
        if (strategyFilterMode === "active") return Boolean(item.has_activity);
        if (strategyFilterMode === "open_positions") return Number(item.net_position_qty || 0) !== 0;
        if (strategyFilterMode === "winners") return Number(item.gross_realized_pnl || 0) > 0;
        if (strategyFilterMode === "fresh") return activityState.label === "FRESH";
        if (strategyFilterMode === "stale") return activityState.label === "STALE";
        if (strategyFilterMode === "idle") return activityState.label === "IDLE";
        return true;
      }

      function sortableStrategyValue(item, sortMode) {
        if (sortMode === "latest_activity_at") {
          const parsed = parseDashboardTimestamp(item.latest_activity_at);
          return parsed ? parsed.getTime() : -1;
        }
        if (sortMode === "latest_closed_pnl") {
          return Number(item.latest_closed_trade?.realized_pnl || 0);
        }
        return Number(item[sortMode] || 0);
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
        const schedulerSymbols = window.__schedulerSymbolsStatus || null;
        el("health-status").textContent = health.status.toUpperCase();
        el("health-status").className = `value ${statusClass(health.status)}`;

        const scheduler = health.checks.scheduler;
        el("scheduler-status").textContent = scheduler.stopped ? "STOPPED" : scheduler.status.toUpperCase();
        el("scheduler-status").className = `value ${statusClass(scheduler.status)}`;
        const schedulerStrategy = window.__schedulerStrategyStatus || null;
        if (schedulerStrategy) {
          const effectiveOrder = schedulerStrategy.effective_strategy_names || schedulerStrategy.strategy_names || [schedulerStrategy.strategy_name];
          const orderLabel = effectiveOrder.length ? effectiveOrder.join(" -> ") : "none";
          const limitedStrategies = (schedulerStrategy.strategy_entries || [])
            .filter((item) => item.active && item.enabled && !item.effective)
            .map((item) => item.strategy_name);
          const disabledNotes = Object.entries(schedulerStrategy.disabled_strategy_notes || {})
            .map(([name, note]) => `${name}: ${note}`);
          const limitLabel = schedulerStrategy.effective_strategy_limit || "all";
          const warning = effectiveOrder.length
            ? ""
            : " | warning: no enabled active strategies"
          const disabledSummary = disabledNotes.length
            ? ` | disabled notes: ${disabledNotes.join("; ")}`
            : "";
          const limitedSummary = limitedStrategies.length
            ? ` | excluded by limit: ${limitedStrategies.join(", ")}`
            : "";
          const symbolSummary = schedulerSymbols?.symbol_names?.length
            ? ` | symbols: ${schedulerSymbols.symbol_names.join(", ")}`
            : "";
          el("scheduler-detail").textContent = `effective order: ${orderLabel} | limit: ${limitLabel}${symbolSummary}${limitedSummary}${warning}${disabledSummary}`;
        } else {
          el("scheduler-detail").textContent = "Scheduler strategy not loaded yet.";
        }

        const killSwitch = health.checks.kill_switch;
        el("kill-switch-status").textContent = killSwitch.enabled ? "ENABLED" : "DISABLED";
        el("kill-switch-status").className = `value ${statusClass(killSwitch.status)}`;
        const queue = health.checks.queue || { status: "degraded", counts: {} };
        const staleBatch = queue.reason === "Queue contains stale incomplete batches." ? (queue.latest_incomplete_batch || null) : null;
        el("queue-status").textContent = staleBatch ? "STALE" : String(queue.status || "unknown").toUpperCase();
        el("queue-status").className = `value ${statusClass(queue.status)}`;
        const queueCounts = queue.counts || {};
        const queueDetail = [`queued=${queueCounts.queued ?? 0}`, `leased=${queueCounts.leased ?? 0}`, `failed=${queueCounts.failed ?? 0}`];
        if (staleBatch) {
          queueDetail.push(`stale_batch_age=${staleBatch.age_seconds ?? "n/a"}s`);
          queueDetail.push(`source=${staleBatch.source || "unknown"}`);
          queueDetail.push(`orchestration=${staleBatch.orchestration || "n/a"}`);
        } else if (queue.reason) {
          queueDetail.push(`reason=${queue.reason}`);
        }
        el("queue-detail").textContent = queueDetail.join(" ");
        const executionBackend = health.checks.execution_backend || { status: "degraded" };
        const brokerProtection = health.checks.broker_protection || { status: "ok" };
        el("execution-backend-status").textContent = String(executionBackend.backend || "unknown").toUpperCase();
        el("execution-backend-status").className = `value ${statusClass(executionBackend.status)}`;
        const executionDetail = [
          `${executionBackend.description || "unknown backend"}`,
          `dry_run=${Boolean(executionBackend.dry_run)}`,
          `can_execute_orders=${Boolean(executionBackend.can_execute_orders)}`,
        ];
          if (brokerProtection.status === "degraded") {
          executionDetail.push(`broker_protection=${brokerProtection.reason || "degraded"}`);
          if (brokerProtection.reason_code) {
            executionDetail.push(`code=${brokerProtection.reason_code}`);
          }
          if (brokerProtection.severity) {
            executionDetail.push(`severity=${brokerProtection.severity}`);
          }
          if (brokerProtection.recommended_action) {
            executionDetail.push(`action=${brokerProtection.recommended_action}`);
          }
          if (brokerProtection.rejected_risk_streak !== undefined) {
            executionDetail.push(`reject_streak=${brokerProtection.rejected_risk_streak}`);
          }
          if (brokerProtection.latest_order?.status) {
            executionDetail.push(`latest_order_status=${brokerProtection.latest_order.status}`);
          }
        }
        el("execution-backend-detail").textContent = executionDetail.join(" | ");

        el("last-refresh").textContent = new Date().toLocaleTimeString();

        const strip = el("status-strip");
        strip.innerHTML = "";
        const chips = [
          ["health", health.status],
          ["scheduler", scheduler.stopped ? "stopped" : scheduler.status],
          ["kill switch", killSwitch.enabled ? "enabled" : "disabled"],
          ["db", health.checks.database.status],
          ["candles", health.checks.candles.status],
          ["execution", health.checks.execution_backend?.backend || "unknown"],
          ["broker", brokerProtection.status || "unknown"],
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
          const detailBits = [];
          let issueActionButton = "";
          if (issue.name === "broker_protection") {
            const brokerCheck = health.checks.broker_protection || {};
            if (brokerCheck.backend) detailBits.push(`backend=${brokerCheck.backend}`);
            if (brokerCheck.reason_code) detailBits.push(`code=${brokerCheck.reason_code}`);
            if (brokerCheck.severity) detailBits.push(`severity=${brokerCheck.severity}`);
            if (brokerCheck.recommended_action) detailBits.push(`action=${brokerCheck.recommended_action}`);
            if (brokerCheck.approved_risk_count !== undefined) detailBits.push(`approved=${brokerCheck.approved_risk_count}`);
            if (brokerCheck.rejected_risk_streak !== undefined) detailBits.push(`reject_streak=${brokerCheck.rejected_risk_streak}`);
            if (brokerCheck.latest_order?.status) detailBits.push(`latest_order=${brokerCheck.latest_order.status}`);
            if (brokerCheck.latest_order?.age_seconds !== undefined) detailBits.push(`latest_order_age=${brokerCheck.latest_order.age_seconds}s`);
            if (brokerCheck.recommended_action === "switch_to_paper_backend") {
              issueActionButton = ' <button type="button" class="secondary" data-action="broker-switch-paper">Switch to paper</button>';
            } else if (brokerCheck.recommended_action === "pause_scheduler") {
              issueActionButton = ' <button type="button" class="secondary" data-action="broker-pause-scheduler">Pause scheduler</button>';
            } else if (brokerCheck.recommended_action === "enable_kill_switch") {
              issueActionButton = ' <button type="button" class="danger" data-action="broker-enable-kill">Enable kill switch</button>';
            } else if (brokerCheck.recommended_action === "inspect_and_reconcile_orders") {
              issueActionButton = ' <button type="button" class="secondary" data-action="broker-reconcile-orders">Reconcile orders</button>';
            }
          }
          chip.className = "chip";
          chip.innerHTML =
            `<strong>${issue.name}</strong>: ` +
            `<span class="${statusClass(issue.status)}">${issue.reason}${detailBits.length ? ` | ${detailBits.join(" | ")}` : ""}</span>${issueActionButton}`;
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
          ? `${dataWorker.last_seen_at} | ${dataWorker.message}${(dataWorker.payload?.symbol_names || []).length ? ` | symbols: ${dataWorker.payload.symbol_names.join(", ")}` : ""}`
          : "No data worker heartbeat recorded yet.";

        const strategyWorker = heartbeatMap.strategy_worker;
        el("strategy-worker-status").textContent = strategyWorker
          ? String(strategyWorker.status).toUpperCase()
          : "NONE";
        el("strategy-worker-status").className = `value ${statusClass(strategyWorker ? strategyWorker.status : "degraded")}`;
        el("strategy-worker-detail").textContent = strategyWorker
          ? `${strategyWorker.last_seen_at} | ${strategyWorker.message}${(strategyWorker.payload?.symbol_names || []).length ? ` | symbols: ${strategyWorker.payload.symbol_names.join(", ")}` : ""}`
          : "No strategy worker heartbeat recorded yet.";

        const executionWorker = heartbeatMap.execution_worker;
        el("execution-worker-status").textContent = executionWorker
          ? String(executionWorker.status).toUpperCase()
          : "NONE";
        el("execution-worker-status").className = `value ${statusClass(executionWorker ? executionWorker.status : "degraded")}`;
        el("execution-worker-detail").textContent = executionWorker
          ? `${executionWorker.last_seen_at} | ${executionWorker.message}${(executionWorker.payload?.symbol_names || []).length ? ` | symbols: ${executionWorker.payload.symbol_names.join(", ")}` : ""}`
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
        const latestPipelineRun = window.__latestHealth?.checks?.pipeline?.latest_run || null;
        const runs = auditEvents.filter((event) => event.event_type === "pipeline_run");
        const latestCompleted = runs.find((event) => event.status !== "started") || runs[0] || null;

        if (!latestCompleted && !latestPipelineRun) {
          el("pipeline-status").textContent = "NONE";
          el("pipeline-status").className = "value warn";
          el("pipeline-detail").textContent = "No pipeline runs recorded yet.";
          el("pipeline-symbols").textContent = "Symbols: none";
          el("pipeline-counts").textContent = "Counts: none";
          return;
        }

        if (latestPipelineRun) {
          const displayStatus = String(latestPipelineRun.status || "unknown").toUpperCase();
          const strategyLabel = (latestPipelineRun.strategy_names || []).length
            ? latestPipelineRun.strategy_names.join(", ")
            : (latestPipelineRun.strategy_name || "n/a");
          const symbolLabel = (latestPipelineRun.symbol_names || []).length
            ? latestPipelineRun.symbol_names.join(", ")
            : "none";
          const executionBackend = latestPipelineRun.execution_backend || "unknown";
          const executionBackendStatus = latestPipelineRun.execution_backend_status || {};
          el("pipeline-status").textContent = displayStatus;
          el("pipeline-status").className = `value ${statusClass(latestPipelineRun.status)}`;
          el("pipeline-detail").textContent =
            `${latestPipelineRun.created_at} | ${latestPipelineRun.message} | strategies: ${strategyLabel} | execution: ${executionBackend} | dry_run=${Boolean(executionBackendStatus.dry_run)} | can_execute_orders=${Boolean(executionBackendStatus.can_execute_orders)}`;
          el("pipeline-symbols").textContent = `Symbols: ${symbolLabel}`;
          el("pipeline-counts").textContent =
            `Counts: signals=${latestPipelineRun.generated_signal_count ?? 0}, ` +
            `approved=${latestPipelineRun.approved_risk_count ?? 0}, ` +
            `rejected=${latestPipelineRun.rejected_risk_count ?? 0}, ` +
            `fills=${latestPipelineRun.filled_execution_count ?? 0}`;
          return;
        }

        const displayStatus = String(latestCompleted.status || "unknown").toUpperCase();
        el("pipeline-status").textContent = displayStatus;
        el("pipeline-status").className = `value ${statusClass(latestCompleted.status)}`;
        el("pipeline-detail").textContent = `${latestCompleted.created_at} | ${latestCompleted.message}`;
        el("pipeline-symbols").textContent = "Symbols: unavailable";
        el("pipeline-counts").textContent = "Counts: unavailable";
      }

      function renderSchedulerPriorityControls(schedulerStrategy) {
        const container = el("scheduler-priority-controls");
        if (!container) return;
        const availableStrategies = schedulerStrategy?.available_strategies || [];
        const priorities = schedulerStrategy?.strategy_priorities || {};
        if (!availableStrategies.length) {
          container.innerHTML = '<div class="chip">No strategies available.</div>';
          return;
        }
        container.innerHTML = availableStrategies.map((strategyName, index) => {
          const priority = Object.prototype.hasOwnProperty.call(priorities, strategyName)
            ? priorities[strategyName]
            : index;
          return `
            <label for="priority-${strategyName}">${strategyName}</label>
            <input id="priority-${strategyName}" data-strategy-priority="${strategyName}" type="number" step="1" value="${priority}" />
          `;
        }).join("");
      }

      function renderSchedulerDisabledNoteControls(schedulerStrategy) {
        const container = el("scheduler-disabled-note-controls");
        if (!container) return;
        const availableStrategies = schedulerStrategy?.available_strategies || [];
        const disabledNotes = schedulerStrategy?.disabled_strategy_notes || {};
        if (!availableStrategies.length) {
          container.innerHTML = '<div class="chip">No strategies available.</div>';
          return;
        }
        container.innerHTML = availableStrategies.map((strategyName) => `
          <label for="disabled-note-${strategyName}">${strategyName}</label>
          <input id="disabled-note-${strategyName}" data-strategy-disabled-note="${strategyName}" type="text" value="${disabledNotes[strategyName] || ""}" placeholder="optional disable note" />
        `).join("");
      }

      function updateSoakValidation(currentReport, history) {
        el("soak-json").textContent = formatJson({
          current_report: currentReport,
          recent_history: history,
        });
      }

      function updateStrategySummary(strategySummary) {
        const board = el("strategy-summary-board");
        if (!Array.isArray(strategySummary) || strategySummary.length === 0) {
          board.innerHTML = '<div class="strategy-card">No strategy activity recorded yet.</div>';
          updateSelectedStrategyDetails([], []);
          return;
        }

        const strategyEntries = window.__schedulerStrategyStatus?.strategy_entries || [];
        const strategyEntryMap = Object.fromEntries(strategyEntries.map((item) => [item.strategy_name, item]));
        const filteredStrategies = strategySummary.filter((item) => matchesStrategyFilter(item, classifyStrategyActivity(item)));

        const sortedStrategies = [...filteredStrategies].sort((left, right) => {
          if (strategySortMode === "strategy_name") {
            return String(left.strategy_name).localeCompare(String(right.strategy_name));
          }
          return sortableStrategyValue(right, strategySortMode) - sortableStrategyValue(left, strategySortMode);
        });

        if (sortedStrategies.length === 0) {
          board.innerHTML = '<div class="strategy-card">No strategies match the current filter.</div>';
          updateSelectedStrategyDetails(strategySummary, []);
          return;
        }

        board.innerHTML = sortedStrategies.map((item) => {
          const activityState = classifyStrategyActivity(item);
          const strategyEntry = strategyEntryMap[item.strategy_name] || {};
          const latestSignal = item.latest_signal?.signal_type || "none";
          const latestRisk = item.latest_risk?.decision || "none";
          const latestOrder = item.latest_order?.status || "none";
          const latestFill = item.latest_fill?.side || "none";
          const latestActivityAt = item.latest_activity_at || "none";
          const latestOrderAt = item.latest_order_at || "none";
          const latestFillAt = item.latest_fill_at || "none";
          const latestClosedTrade = item.latest_closed_trade || null;
          const latestClosedSymbol = latestClosedTrade?.symbol || "none";
          const latestClosedStatus = latestClosedTrade?.status || "none";
          const latestClosedAt = latestClosedTrade?.closed_at || "none";
          const latestClosedPnl = latestClosedTrade
            ? Number(latestClosedTrade.realized_pnl || 0).toFixed(6)
            : "n/a";
          const latestClosedPnlClass = latestClosedTrade
            ? Number(latestClosedTrade.realized_pnl || 0) > 0
              ? "ok"
              : Number(latestClosedTrade.realized_pnl || 0) < 0
                ? "bad"
                : "warn"
            : "warn";
          const pnl = Number(item.gross_realized_pnl || 0).toFixed(6);
          const pnlClass = Number(item.gross_realized_pnl || 0) > 0
            ? "ok"
            : Number(item.gross_realized_pnl || 0) < 0
              ? "bad"
              : "warn";
          const enabledLabel = strategyEntry.enabled === false
            ? "DISABLED"
            : strategyEntry.effective === false && strategyEntry.active
              ? "LIMITED"
              : activityState.label;
          const enabledClass = strategyEntry.enabled === false
            ? "bad"
            : strategyEntry.effective === false && strategyEntry.active
              ? "warn"
              : activityState.className;
          const disabledReason = strategyEntry.disabled_reason || "none";
          const canPromote = strategyEntry.active;
          const canDemote = strategyEntry.active;

          return `
            <div class="strategy-card clickable ${closedTradesStrategyFilter === item.strategy_name ? "selected" : ""}" data-strategy-name="${item.strategy_name}" role="button" tabindex="0" title="Filter recent closed trades for ${item.strategy_name}">
              <div class="strategy-card-header">
                <strong>${item.strategy_name}</strong>
                <div class="strategy-card-actions">
                  ${canPromote ? `<button type="button" class="secondary" data-promote-strategy="${item.strategy_name}">Promote</button>` : ""}
                  ${canDemote ? `<button type="button" class="secondary" data-demote-strategy="${item.strategy_name}">Demote</button>` : ""}
                  ${strategyEntry.enabled !== false
                    ? `<button type="button" class="secondary" data-disable-strategy="${item.strategy_name}">Disable</button>`
                    : `<button type="button" class="secondary" data-enable-strategy="${item.strategy_name}">Enable</button>`}
                  <span class="${enabledClass}">${enabledLabel}</span>
                </div>
              </div>
              <div class="strategy-card-grid">
                <div class="strategy-metric"><strong>Signal</strong>${latestSignal}</div>
                <div class="strategy-metric"><strong>Risk</strong>${latestRisk}</div>
                <div class="strategy-metric"><strong>Order</strong>${latestOrder}</div>
                <div class="strategy-metric"><strong>Fill</strong>${latestFill}</div>
                <div class="strategy-metric"><strong>Disabled Reason</strong>${disabledReason}</div>
                <div class="strategy-metric"><strong>Filled Orders</strong>${item.filled_order_count}</div>
                <div class="strategy-metric"><strong>Filled Qty</strong>${item.filled_qty_total}</div>
                <div class="strategy-metric"><strong>Net Qty</strong>${item.net_position_qty}</div>
                <div class="strategy-metric"><strong>Gross PnL</strong><span class="${pnlClass}">${pnl}</span></div>
                <div class="strategy-metric"><strong>Wins</strong>${item.winning_trade_count}</div>
                <div class="strategy-metric"><strong>Losses</strong>${item.losing_trade_count}</div>
                <div class="strategy-metric"><strong>Latest Activity</strong>${latestActivityAt}</div>
                <div class="strategy-metric"><strong>Latest Order At</strong>${latestOrderAt}</div>
                <div class="strategy-metric"><strong>Latest Fill At</strong>${latestFillAt}</div>
                <div class="strategy-metric"><strong>Latest Closed Symbol</strong>${latestClosedSymbol}</div>
                <div class="strategy-metric"><strong>Latest Closed Status</strong><span class="${latestClosedPnlClass}">${latestClosedStatus}</span></div>
                <div class="strategy-metric"><strong>Latest Closed At</strong>${latestClosedAt}</div>
                <div class="strategy-metric"><strong>Latest Closed PnL</strong><span class="${latestClosedPnlClass}">${latestClosedPnl}</span></div>
              </div>
            </div>
          `;
        }).join("");

        updateSelectedStrategyDetails(strategySummary, window.__strategyClosedTrades || []);
      }

      function updateSelectedStrategyDetails(strategySummary, closedTrades) {
        const board = el("selected-strategy-board");
        if (!Array.isArray(strategySummary) || strategySummary.length === 0) {
          board.innerHTML = '<div class="strategy-card">No strategy details available yet.</div>';
          return;
        }
        if (closedTradesStrategyFilter === "all") {
          board.innerHTML = '<div class="strategy-card">Select a strategy card to inspect a single strategy.</div>';
          return;
        }

        const selected = strategySummary.find((item) => item.strategy_name === closedTradesStrategyFilter);
        if (!selected) {
          board.innerHTML = `<div class="strategy-card">No strategy details found for ${closedTradesStrategyFilter}.</div>`;
          return;
        }

        const activityState = classifyStrategyActivity(selected);
        const latestClosedTrade = selected.latest_closed_trade || null;
        const pnlClass = Number(selected.gross_realized_pnl || 0) > 0
          ? "ok"
          : Number(selected.gross_realized_pnl || 0) < 0
            ? "bad"
            : "warn";
        const closedTradeCount = Number(selected.realized_trade_count || 0);
        const winRate = closedTradeCount > 0
          ? `${((Number(selected.winning_trade_count || 0) / closedTradeCount) * 100).toFixed(1)}%`
          : "n/a";
        const lastClosedStatus = latestClosedTrade?.status || "none";
        const lastClosedStatusClass = latestClosedTrade
          ? Number(latestClosedTrade.realized_pnl || 0) > 0
            ? "ok"
            : Number(latestClosedTrade.realized_pnl || 0) < 0
              ? "bad"
              : "warn"
          : "warn";
        const recentClosedTrades = Array.isArray(closedTrades) ? closedTrades.slice(0, 3) : [];
        const recentClosedTradesHtml = recentClosedTrades.length
          ? recentClosedTrades.map((item) => {
              const itemPnlClass = Number(item.realized_pnl || 0) > 0
                ? "ok"
                : Number(item.realized_pnl || 0) < 0
                  ? "bad"
                  : "warn";
              return `
                <div class="strategy-metric">
                  <strong>${item.symbol} · ${item.closed_at}</strong>
                  <span class="${itemPnlClass}">${item.status} / ${Number(item.realized_pnl || 0).toFixed(6)}</span>
                </div>
              `;
            }).join("")
          : '<div class="strategy-metric"><strong>Recent Closed Trades</strong>none</div>';
        board.innerHTML = `
          <div class="strategy-card selected">
            <div class="strategy-card-header">
              <strong>${selected.strategy_name}</strong>
              <span class="${activityState.className}">${activityState.label}</span>
            </div>
            <div class="strategy-card-grid">
              <div class="strategy-metric"><strong>Latest Signal</strong>${selected.latest_signal?.signal_type || "none"}</div>
              <div class="strategy-metric"><strong>Latest Risk</strong>${selected.latest_risk?.decision || "none"}</div>
              <div class="strategy-metric"><strong>Latest Order</strong>${selected.latest_order?.status || "none"}</div>
              <div class="strategy-metric"><strong>Latest Fill</strong>${selected.latest_fill?.side || "none"}</div>
              <div class="strategy-metric"><strong>Realized Trades</strong>${selected.realized_trade_count}</div>
              <div class="strategy-metric"><strong>Closed Trades</strong>${closedTradeCount}</div>
              <div class="strategy-metric"><strong>Win Rate</strong>${winRate}</div>
              <div class="strategy-metric"><strong>Last Closed Result</strong><span class="${lastClosedStatusClass}">${lastClosedStatus}</span></div>
              <div class="strategy-metric"><strong>Gross PnL</strong><span class="${pnlClass}">${Number(selected.gross_realized_pnl || 0).toFixed(6)}</span></div>
              <div class="strategy-metric"><strong>Latest Activity</strong>${selected.latest_activity_at || "none"}</div>
              <div class="strategy-metric"><strong>Latest Fill At</strong>${selected.latest_fill_at || "none"}</div>
              <div class="strategy-metric"><strong>Latest Closed Symbol</strong>${latestClosedTrade?.symbol || "none"}</div>
              <div class="strategy-metric"><strong>Latest Closed PnL</strong>${latestClosedTrade ? Number(latestClosedTrade.realized_pnl || 0).toFixed(6) : "n/a"}</div>
              ${recentClosedTradesHtml}
            </div>
          </div>
        `;
      }

      function updateClosedTrades(closedTrades) {
        const board = el("strategy-closed-trades-board");
        if (!Array.isArray(closedTrades) || closedTrades.length === 0) {
          board.innerHTML = '<div class="strategy-card">No closed trades recorded yet.</div>';
          return;
        }

        board.innerHTML = closedTrades.map((item) => {
          const pnlClass = Number(item.realized_pnl || 0) > 0
            ? "ok"
            : Number(item.realized_pnl || 0) < 0
              ? "bad"
              : "warn";
          return `
            <div class="trade-row">
              <div><strong>Strategy</strong>${item.strategy_name}<br>${item.symbol}</div>
              <div><strong>Qty / Status</strong>${item.qty}<br><span class="${pnlClass}">${item.status}</span></div>
              <div><strong>Entry / Exit</strong>${Number(item.entry_price).toFixed(4)}<br>${Number(item.exit_price).toFixed(4)}</div>
              <div><strong>Realized PnL</strong><span class="${pnlClass}">${Number(item.realized_pnl).toFixed(6)}</span><br>${item.closed_at}</div>
            </div>
          `;
        }).join("");
      }

      function updateSchedulerControlActivity(auditEvents) {
        const board = el("scheduler-control-board");
        const presetBar = el("scheduler-preset-quick-actions");
        const presetEvents = (Array.isArray(auditEvents) ? auditEvents : [])
          .filter((event) => event.event_type === "scheduler_control")
          .filter((event) => {
            const action = String(event.payload?.action || "");
            return action.startsWith("priority_preset:") || action.startsWith("limit_preset:");
          })
          .slice(0, 4);
        if (presetBar) {
          presetBar.innerHTML = presetEvents.length
            ? presetEvents.map((event) => {
                const action = event.payload?.action || "unknown";
                const preset = event.payload?.preset || "";
                return `<button type="button" class="secondary" data-replay-scheduler-preset="${preset}" data-replay-scheduler-action="${action}">Replay ${action}</button>`;
              }).join("")
            : '<span class="chip">No recent preset actions.</span>';
        }
        const schedulerEvents = (Array.isArray(auditEvents) ? auditEvents : [])
          .filter((event) => event.event_type === "scheduler_control" || event.event_type === "execution_control" || event.event_type === "kill_switch")
          .filter((event) => {
            if (event.event_type === "execution_control") {
              return schedulerControlFilterMode === "all";
            }
            if (event.event_type === "kill_switch") {
              return schedulerControlFilterMode === "all";
            }
            if (schedulerControlFilterMode === "all") return true;
            const action = String(event.payload?.action || "");
            if (schedulerControlFilterMode === "priority") {
              return action.startsWith("promote:")
                || action.startsWith("demote:")
                || action.startsWith("priority_preset:")
                || action === "set_strategy_priorities";
            }
            if (schedulerControlFilterMode === "limit") {
              return action.startsWith("limit_preset:") || action === "set_effective_strategy_limit";
            }
            if (schedulerControlFilterMode === "enable_disable") {
              return action.startsWith("enable:")
                || action.startsWith("disable:")
                || action === "set_disabled_strategies";
            }
            return true;
          })
          .slice(0, 6);
        if (schedulerEvents.length === 0) {
          board.innerHTML = '<div class="strategy-card">No scheduler control activity matches the current filter.</div>';
          return;
        }

        board.innerHTML = schedulerEvents.map((event, index) => {
          const action = event.payload?.action || "unknown";
          const preset = event.payload?.preset || "";
          const replayButton = preset && (action.startsWith("priority_preset:") || action.startsWith("limit_preset:"))
            ? `<button type="button" class="secondary" data-replay-scheduler-preset="${preset}" data-replay-scheduler-action="${action}">Replay Preset</button>`
            : "";
          const strategyNames = event.payload?.strategy_names || event.payload?.disabled_strategy_names || [];
          const strategyLabel = Array.isArray(strategyNames) && strategyNames.length
            ? strategyNames.join(", ")
            : event.payload?.strategy_name || "n/a";
          const backendLabel = event.payload?.backend || "";
          const statusClassName = statusClass(event.status || "degraded");
          const detailBits = [
            preset ? `preset=${preset}` : "",
            event.payload?.effective_strategy_limit != null ? `limit=${event.payload.effective_strategy_limit}` : "",
            strategyLabel !== "n/a" ? `strategies=${strategyLabel}` : "",
            backendLabel ? `backend=${backendLabel}` : "",
          ].filter(Boolean);
          return `
            <div class="trade-row">
              <div><strong>Action</strong>${action}${index === 0 ? ' <span class="ok">LATEST</span>' : ""}<br>${event.created_at}</div>
              <div><strong>Status</strong><span class="${statusClassName}">${event.status}</span><br>${event.source}</div>
              <div><strong>Message</strong>${event.message}<br>${detailBits.join(" | ") || "no extra detail"}<br><button type="button" class="secondary" data-copy-scheduler-action="${action}" data-copy-scheduler-preset="${preset}">Copy Action</button> ${replayButton}</div>
            </div>
          `;
        }).join("");
      }

      function applyClosedTradesStrategyFilter(strategyName) {
        const select = el("closed-trades-strategy-select");
        if (select) {
          select.value = strategyName;
        }
        closedTradesStrategyFilter = strategyName;
        refreshAll().catch((error) => {
          el("strategy-closed-trades-board").innerHTML = `<div class="strategy-card">Failed to filter closed trades: ${error.message}</div>`;
        });
      }

      function collectSchedulerStrategyPayload() {
        const selectedSchedulerStrategies = Array.from(
          el("scheduler-strategy-select")?.selectedOptions || []
        ).map((option) => option.value);
        const selectedSchedulerSymbols = Array.from(
          el("scheduler-symbol-select")?.selectedOptions || []
        ).map((option) => option.value);
        const selectedDisabledStrategies = Array.from(
          el("scheduler-disabled-strategy-select")?.selectedOptions || []
        ).map((option) => option.value);
        const strategyPriorities = Object.fromEntries(
          Array.from(document.querySelectorAll("[data-strategy-priority]")).map((input, index) => [
            input.dataset.strategyPriority,
            Number.parseInt(input.value || `${index}`, 10),
          ])
        );
        const disabledStrategyNotes = Object.fromEntries(
          Array.from(document.querySelectorAll("[data-strategy-disabled-note]"))
            .map((input) => [input.dataset.strategyDisabledNote, input.value.trim()])
            .filter(([, value]) => value)
        );
        const effectiveLimitRaw = el("scheduler-effective-limit-input")?.value?.trim() || "";
        return {
          symbol_names: selectedSchedulerSymbols,
          strategy_names: selectedSchedulerStrategies.length
            ? selectedSchedulerStrategies
            : ["__DEFAULT_STRATEGY_NAME__"],
          disabled_strategy_names: selectedDisabledStrategies,
          strategy_priorities: strategyPriorities,
          disabled_strategy_notes: disabledStrategyNotes,
          effective_strategy_limit: effectiveLimitRaw ? Number.parseInt(effectiveLimitRaw, 10) : null,
        };
      }

      async function promoteStrategyPriority(strategyName) {
        const payload = collectSchedulerStrategyPayload();
        const priorities = payload.strategy_priorities || {};
        const currentValues = Object.values(priorities).map((value) => Number(value)).filter((value) => Number.isFinite(value));
        const nextPriority = currentValues.length ? Math.min(...currentValues) - 1 : 0;
        priorities[strategyName] = nextPriority;
        payload.strategy_priorities = priorities;
        payload.audit_action = `promote:${strategyName}`;
        payload.audit_message = `Promoted strategy priority for ${strategyName}.`;
        const result = await api("/scheduler/strategy", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function demoteStrategyPriority(strategyName) {
        const payload = collectSchedulerStrategyPayload();
        const priorities = payload.strategy_priorities || {};
        const currentValues = Object.values(priorities).map((value) => Number(value)).filter((value) => Number.isFinite(value));
        const nextPriority = currentValues.length ? Math.max(...currentValues) + 1 : 1;
        priorities[strategyName] = nextPriority;
        payload.strategy_priorities = priorities;
        payload.audit_action = `demote:${strategyName}`;
        payload.audit_message = `Demoted strategy priority for ${strategyName}.`;
        const result = await api("/scheduler/strategy", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function disableStrategy(strategyName) {
        const payload = collectSchedulerStrategyPayload();
        const disabledStrategies = Array.from(new Set([...(payload.disabled_strategy_names || []), strategyName]));
        payload.disabled_strategy_names = disabledStrategies;
        if (!payload.disabled_strategy_notes[strategyName]) {
          payload.disabled_strategy_notes[strategyName] = "Disabled from strategy leaderboard.";
        }
        payload.audit_action = `disable:${strategyName}`;
        payload.audit_message = `Disabled strategy ${strategyName} from the leaderboard.`;
        const result = await api("/scheduler/strategy", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function enableStrategy(strategyName) {
        const payload = collectSchedulerStrategyPayload();
        payload.disabled_strategy_names = (payload.disabled_strategy_names || []).filter((name) => name !== strategyName);
        if (payload.disabled_strategy_notes[strategyName] === "Disabled from strategy leaderboard.") {
          delete payload.disabled_strategy_notes[strategyName];
        }
        payload.audit_action = `enable:${strategyName}`;
        payload.audit_message = `Enabled strategy ${strategyName} from the leaderboard.`;
        const result = await api("/scheduler/strategy", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function applySchedulerPreset(limit) {
        const preset = limit === 1 ? "top_1" : limit === 2 ? "top_2" : "all_enabled";
        const result = await api("/scheduler/strategy/limit-preset", {
          method: "POST",
          body: JSON.stringify({ preset }),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function resetStrategyPriorities() {
        const result = await api("/scheduler/strategy/preset", {
          method: "POST",
          body: JSON.stringify({ preset: "reset" }),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function applyPriorityPreset(mode) {
        const preset = mode === "active-first" ? "active_first" : mode;
        const result = await api("/scheduler/strategy/preset", {
          method: "POST",
          body: JSON.stringify({ preset }),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      async function clearDisabledStrategyNotes() {
        document.querySelectorAll("[data-strategy-disabled-note]").forEach((input) => {
          input.value = "";
        });
        const payload = collectSchedulerStrategyPayload();
        payload.disabled_strategy_notes = {};
        payload.audit_action = "clear_disabled_notes";
        payload.audit_message = "Cleared scheduler disabled strategy notes.";
        const result = await api("/scheduler/strategy", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        el("scheduler-message").textContent = formatJson(result);
        await refreshAll();
      }

      function updateHeartbeats(health) {
        const heartbeatCheck = health.checks.heartbeats || { components: [] };
        const lines = (heartbeatCheck.components || []).map((item) =>
          `${item.component} | ${String(item.status).toUpperCase()} | ${item.last_seen_at} | ${item.message}`
        );
        el("heartbeats-json").textContent = lines.length ? lines.join("\\n") : "No runtime heartbeats recorded yet.";
      }

      function renderQueueSummary(queueSummary) {
        const board = el("queue-board");
        if (!board) return;
        const counts = queueSummary?.counts || {};
        const metrics = queueSummary?.metrics || {};
        const byType = queueSummary?.job_type_counts || {};
        const latestFailedJob = queueSummary?.latest_failed_job || null;
        const latestRetryJob = queueSummary?.latest_retry_job || null;
        const recentBatches = Array.isArray(queueSummary?.recent_batches) ? queueSummary.recent_batches : [];
        const latestIncompleteBatch = queueSummary?.latest_incomplete_batch || null;
        const latestCompletedBatch = queueSummary?.latest_completed_batch || null;
        const jobs = Array.isArray(queueSummary?.latest_jobs) ? queueSummary.latest_jobs : [];
        const filteredJobs = jobs.filter((job) => {
          if (queueFilterMode === "all") return true;
          if (queueFilterMode === "failed") return job.status === "failed";
          if (queueFilterMode === "queued") return job.status === "queued";
          return job.job_type === queueFilterMode;
        });
        const summaryBits = [
          `total=${counts.total ?? 0}`,
          `queued=${counts.queued ?? 0}`,
          `leased=${counts.leased ?? 0}`,
          `failed=${counts.failed ?? 0}`,
          `fail%=${Number(metrics.failure_ratio || 0) * 100}%`,
          `avg attempts=${metrics.avg_attempt_count ?? 0}`,
          `retries=${metrics.retry_job_count ?? 0}`,
          `failure streak=${metrics.failure_streak ?? 0}`,
          `recent failed=${metrics.recent_failure_count ?? 0}`,
          `recent retries=${metrics.recent_retry_count ?? 0}`,
        ];
        const typeBits = ["market_data", "strategy", "execution"].map((jobType) => {
          const item = byType[jobType] || {};
          const latestTypeFailed = item.latest_failed_job ? ` latest_failed=#${item.latest_failed_job.id}` : "";
          const latestTypeRetry = item.latest_retry_job ? ` latest_retry=#${item.latest_retry_job.id}` : "";
          const trend = item.recent_terminal_trend
            ? ` trend=${item.recent_terminal_trend}`
            : "";
          return `${jobType}: q=${item.queued ?? 0} f=${item.failed ?? 0} t=${item.total ?? 0} fail%=${Number(item.failure_ratio || 0) * 100}% avg=${item.avg_attempt_count ?? 0}${latestTypeFailed}${latestTypeRetry}${trend}`;
        });
        const latestFailedBit = latestFailedJob
          ? `Latest failed: #${latestFailedJob.id} ${latestFailedJob.job_type} attempts=${latestFailedJob.attempt_count}${latestFailedJob.error_message ? ` error=${latestFailedJob.error_message}` : ""}`
          : "Latest failed: none";
        const latestRetryBit = latestRetryJob
          ? `Latest retry: #${latestRetryJob.id} ${latestRetryJob.job_type} attempts=${latestRetryJob.attempt_count}`
          : "Latest retry: none";
        const batchBits = recentBatches.length
          ? recentBatches.map((batch) => {
              const statuses = Object.entries(batch.statuses || {})
                .map(([jobType, status]) => `${jobType}=${status}`)
                .join(",");
              return `batch=${String(batch.batch_id).slice(0, 8)} age=${batch.age_seconds ?? "n/a"}s source=${batch.source || "unknown"} orchestration=${batch.orchestration || "n/a"} backend=${batch.execution_backend || "unknown"} statuses=${statuses}`;
            }).join(" | ")
          : "Recent batches: none";
        const incompleteBatchBit = latestIncompleteBatch
          ? `Incomplete batch: ${String(latestIncompleteBatch.batch_id).slice(0, 8)} age=${latestIncompleteBatch.age_seconds ?? "n/a"}s source=${latestIncompleteBatch.source || "unknown"} orchestration=${latestIncompleteBatch.orchestration || "n/a"} backend=${latestIncompleteBatch.execution_backend || "unknown"} statuses=${Object.entries(latestIncompleteBatch.statuses || {}).map(([jobType, status]) => `${jobType}=${status}`).join(",")}`
          : "Incomplete batch: none";
        const completedBatchBit = latestCompletedBatch
          ? `Completed batch: ${String(latestCompletedBatch.batch_id).slice(0, 8)} age=${latestCompletedBatch.age_seconds ?? "n/a"}s source=${latestCompletedBatch.source || "unknown"} orchestration=${latestCompletedBatch.orchestration || "n/a"} backend=${latestCompletedBatch.execution_backend || "unknown"} statuses=${Object.entries(latestCompletedBatch.statuses || {}).map(([jobType, status]) => `${jobType}=${status}`).join(",")}`
          : "Completed batch: none";
        if (filteredJobs.length === 0) {
          board.innerHTML = `<div class="strategy-card"><strong>Queue</strong><br>${summaryBits.join(" | ")}<br>${typeBits.join(" | ")}<br>${latestFailedBit}<br>${latestRetryBit}<br>${incompleteBatchBit}<br>${completedBatchBit}<br>${batchBits}<br>No queue jobs match the current filter.</div>`;
          return;
        }
        board.innerHTML = filteredJobs.map((job) => {
          const payloadText = job.payload ? formatJson(job.payload) : "{}";
          const errorText = job.error_message ? `<br><strong>Error</strong> ${job.error_message}` : "";
          const jobStatusClass = statusClass(job.status === "failed" ? "error" : job.status === "queued" ? "degraded" : "ok");
          return `
            <div class="strategy-card">
              <div class="chip"><strong>${job.job_type}</strong>: <span class="${jobStatusClass}">${String(job.status).toUpperCase()}</span></div>
              <div><strong>Job</strong> #${job.id} | attempts=${job.attempt_count} | created=${job.created_at}</div>
              <div><strong>Execution Backend</strong> ${job.payload?.execution_backend || "unknown"}</div>
              <div><strong>Payload</strong> ${payloadText}${errorText}</div>
            </div>
          `;
        }).join("") + `
          <div class="strategy-card">
            <strong>Queue Debug</strong><br>${summaryBits.join(" | ")}<br>${typeBits.join(" | ")}<br>${latestFailedBit}<br>${latestRetryBit}<br>${incompleteBatchBit}<br>${completedBatchBit}<br>${batchBits}
          </div>
        `;
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
        closedTradesStrategyFilter = el("closed-trades-strategy-select")?.value || "all";
        const closedTradesQuery = new URLSearchParams({ limit: "10" });
        if (closedTradesStrategyFilter !== "all") {
          closedTradesQuery.set("strategy_name", closedTradesStrategyFilter);
        }
        const [health, positions, orders, strategySummary, closedTrades, pnl, logs, auditEvents, alertStatus, soakReport, soakHistory, strategies, schedulerStrategy, schedulerSymbols, queueSummary] = await Promise.all([
          api("/health"),
          api("/positions?limit=10"),
          api("/orders?limit=10"),
          api("/strategies/summary"),
          api(`/strategies/closed-trades?${closedTradesQuery.toString()}`),
          api("/pnl?limit=10"),
          api(`/scheduler/logs?lines=20&mode=${encodeURIComponent(schedulerLogsMode)}`),
          api("/audit-events?limit=20"),
          api("/alerts/status"),
          api("/validation/soak"),
          api("/validation/soak/history?limit=10"),
          api("/strategies"),
          api("/scheduler/strategy"),
          api("/scheduler/symbols"),
          api("/queue/summary"),
        ]);

        window.__latestHealth = health;
        window.__schedulerSymbolsStatus = schedulerSymbols;
        window.__strategyClosedTrades = closedTrades;
        window.__schedulerStrategyStatus = schedulerStrategy;
        queueSummaryState = queueSummary;
        const strategySelect = el("pipeline-strategy-select");
        const executionBackendSelect = el("execution-backend-select");
        if (strategySelect && strategies?.default_strategy && !strategySelect.dataset.initialized) {
          strategySelect.value = strategies.default_strategy;
          strategySelect.dataset.initialized = "true";
        }
        if (executionBackendSelect) {
          executionBackendSelect.value = health?.checks?.execution_backend?.backend || "paper";
        }
        const schedulerStrategySelect = el("scheduler-strategy-select");
        if (schedulerStrategySelect && schedulerStrategy?.strategy_names) {
          Array.from(schedulerStrategySelect.options).forEach((option) => {
            option.selected = schedulerStrategy.strategy_names.includes(option.value);
          });
        }
        const schedulerDisabledStrategySelect = el("scheduler-disabled-strategy-select");
        if (schedulerDisabledStrategySelect && schedulerStrategy?.disabled_strategy_names) {
          Array.from(schedulerDisabledStrategySelect.options).forEach((option) => {
            option.selected = schedulerStrategy.disabled_strategy_names.includes(option.value);
          });
        }
        const schedulerSymbolSelect = el("scheduler-symbol-select");
        if (schedulerSymbolSelect && schedulerSymbols?.available_symbols) {
          schedulerSymbolSelect.innerHTML = schedulerSymbols.available_symbols
            .map((symbol) => `<option value="${symbol}">${symbol}</option>`)
            .join("");
          const selectedSchedulerSymbols = schedulerSymbols.symbol_names || [];
          Array.from(schedulerSymbolSelect.options).forEach((option) => {
            option.selected = selectedSchedulerSymbols.includes(option.value);
          });
        }
        const schedulerEffectiveLimitInput = el("scheduler-effective-limit-input");
        if (schedulerEffectiveLimitInput) {
          schedulerEffectiveLimitInput.value = schedulerStrategy?.effective_strategy_limit || "";
        }
        const pipelineSymbolSelect = el("pipeline-symbol-select");
        if (pipelineSymbolSelect && schedulerSymbols?.available_symbols) {
          pipelineSymbolSelect.innerHTML = schedulerSymbols.available_symbols
            .map((symbol) => `<option value="${symbol}">${symbol}</option>`)
            .join("");
          const selectedSymbols = schedulerSymbols.symbol_names || [];
          Array.from(pipelineSymbolSelect.options).forEach((option) => {
            option.selected = selectedSymbols.includes(option.value);
          });
        }
        renderSchedulerPriorityControls(schedulerStrategy);
        renderSchedulerDisabledNoteControls(schedulerStrategy);
        updateHeadline(health);
        updateAlerts(alertStatus, auditEvents);
        updatePipelineSummary(auditEvents);
        updateSoakValidation(soakReport, soakHistory);
        updateStrategySummary(strategySummary);
        updateClosedTrades(closedTrades);
        updateSchedulerControlActivity(auditEvents);
        renderQueueSummary(queueSummary);
        updateHeartbeats(health);
        el("health-json").textContent = formatJson(health);
        el("positions-json").textContent = formatJson(positions);
        el("orders-json").textContent = formatJson(orders);
        el("pnl-json").textContent = formatJson(pnl);
        el("logs-json").textContent = formatJson(logs);
        el("audit-json").textContent = formatJson(auditEvents);
        el("queue-json").textContent = formatJson(queueSummary);
      }

      async function runAction(type) {
        const messages = {
          pipeline: "pipeline-message",
          "auto-refresh-toggle": "pipeline-message",
          "scheduler-start": "scheduler-message",
          "scheduler-stop": "scheduler-message",
          "broker-pause-scheduler": "scheduler-message",
          "scheduler-strategy-save": "scheduler-message",
          "execution-backend-save": "scheduler-message",
          "broker-switch-paper": "scheduler-message",
          "broker-reconcile-orders": "scheduler-message",
          "queue-recover-pipeline": "queue-message",
          "queue-clear-pipeline": "queue-message",
          "queue-enqueue-strategy": "queue-message",
          "queue-drain-strategy": "queue-message",
          "queue-drain-execution": "queue-message",
          "queue-retry-strategy": "queue-message",
          "queue-retry-execution": "queue-message",
          "broker-enable-kill": "kill-message",
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
            const selectedSymbols = Array.from(el("pipeline-symbol-select")?.selectedOptions || []).map((option) => option.value);
            result = await api("/pipeline/run", {
              method: "POST",
              body: JSON.stringify({
                strategy_name: el("pipeline-strategy-select")?.value || "__DEFAULT_STRATEGY_NAME__",
                symbol_names: selectedSymbols,
                orchestration: el("pipeline-orchestration-select")?.value || "direct",
              }),
            });
            el("pipeline-json").textContent = formatJson(result);
          } else if (type === "scheduler-start") {
            result = await api("/scheduler/start", { method: "POST" });
          } else if (type === "scheduler-stop") {
            result = await api("/scheduler/stop", { method: "POST" });
          } else if (type === "broker-pause-scheduler") {
            result = await api("/scheduler/stop", {
              method: "POST",
              body: JSON.stringify({
                audit_action: "broker_protection:pause_scheduler",
                audit_message: "Scheduler paused from broker protection recommendation.",
              }),
            });
          } else if (type === "scheduler-strategy-save") {
            const payload = collectSchedulerStrategyPayload();
            payload.audit_action = "save_strategy_state";
            payload.audit_message = "Applied scheduler strategy state from admin.";
            await api("/scheduler/symbols", {
              method: "POST",
              body: JSON.stringify({
                symbol_names: payload.symbol_names,
              }),
            });
            result = await api("/scheduler/strategy", {
              method: "POST",
              body: JSON.stringify(payload),
            });
          } else if (type === "execution-backend-save") {
            result = await api("/execution/backend", {
              method: "POST",
              body: JSON.stringify({
                backend: el("execution-backend-select")?.value || "paper",
              }),
            });
          } else if (type === "broker-switch-paper") {
            const executionBackendSelect = el("execution-backend-select");
            if (executionBackendSelect) {
              executionBackendSelect.value = "paper";
            }
            result = await api("/execution/backend", {
              method: "POST",
              body: JSON.stringify({
                backend: "paper",
                audit_action: "broker_protection:switch_to_paper_backend",
                audit_message: "Execution backend switched to paper from broker protection recommendation.",
              }),
            });
          } else if (type === "broker-reconcile-orders") {
            result = await api("/orders/reconcile", {
              method: "POST",
              body: JSON.stringify({
                audit_action: "broker_protection:reconcile_orders",
                audit_message: "Order reconciliation triggered from broker protection recommendation.",
              }),
            });
          } else if (type === "scheduler-preset-top1") {
            await applySchedulerPreset(1);
            return;
          } else if (type === "scheduler-preset-top2") {
            await applySchedulerPreset(2);
            return;
          } else if (type === "scheduler-preset-all") {
            await applySchedulerPreset(null);
            return;
          } else if (type === "scheduler-priority-sequential") {
            await applyPriorityPreset("sequential");
            return;
          } else if (type === "scheduler-priority-reverse") {
            await applyPriorityPreset("reverse");
            return;
          } else if (type === "scheduler-priority-active-first") {
            await applyPriorityPreset("active-first");
            return;
          } else if (type === "scheduler-reset-priorities") {
            await resetStrategyPriorities();
            return;
          } else if (type === "scheduler-clear-notes") {
            await clearDisabledStrategyNotes();
            return;
          } else if (type === "queue-recover-pipeline") {
            const staleBatchId = queueSummaryState?.latest_incomplete_batch?.batch_id || null;
            if (!staleBatchId) {
              throw new Error("No stale pipeline batch is available to recover.");
            }
            result = await api("/pipeline/run", {
              method: "POST",
              body: JSON.stringify({ orchestration: "queue_drain", batch_id: staleBatchId }),
            });
          } else if (type === "queue-clear-pipeline") {
            const staleBatchId = queueSummaryState?.latest_incomplete_batch?.batch_id || null;
            if (!staleBatchId) {
              throw new Error("No stale pipeline batch is available to clear.");
            }
            result = await api(`/queue/batches/${encodeURIComponent(staleBatchId)}/clear`, {
              method: "POST",
            });
          } else if (type === "queue-enqueue-strategy") {
            const payload = collectSchedulerStrategyPayload();
            result = await api("/queue/jobs", {
              method: "POST",
              body: JSON.stringify({
                job_type: "strategy",
                strategy_name: payload.strategy_name,
                strategy_names: payload.strategy_names,
                symbol_names: payload.symbol_names,
                payload: { source: "admin_queue" },
              }),
            });
          } else if (type === "queue-drain-strategy") {
            result = await api("/queue/jobs/run-next", {
              method: "POST",
              body: JSON.stringify({ job_type: "strategy" }),
            });
          } else if (type === "queue-drain-execution") {
            result = await api("/queue/jobs/run-next", {
              method: "POST",
              body: JSON.stringify({ job_type: "execution" }),
            });
          } else if (type === "queue-retry-strategy" || type === "queue-retry-execution") {
            const jobType = type === "queue-retry-strategy" ? "strategy" : "execution";
            const latestFailedJob = (queueSummaryState?.latest_jobs || [])
              .find((job) => job.job_type === jobType && job.status === "failed");
            if (!latestFailedJob) {
              throw new Error(`No failed ${jobType} job available to retry.`);
            }
            result = await api(`/queue/jobs/${latestFailedJob.id}/retry`, {
              method: "POST",
            });
          } else if (type === "kill-enable") {
            result = await api("/kill-switch/enable", { method: "POST" });
          } else if (type === "broker-enable-kill") {
            result = await api("/kill-switch/enable", {
              method: "POST",
              body: JSON.stringify({
                reason: "Kill switch enabled from broker protection recommendation.",
                source: "broker_protection",
                notify_message: "Crypto alert: kill switch enabled from broker protection recommendation.",
              }),
            });
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

      el("issue-strip")?.addEventListener("click", (event) => {
        const button = event.target.closest("[data-action]");
        if (!button) return;
        runAction(button.dataset.action);
      });

      document.querySelectorAll("[data-refresh]").forEach((button) => {
        button.addEventListener("click", refreshAll);
      });

      el("logs-mode-select")?.addEventListener("change", () => {
        refreshAll().catch((error) => {
          el("logs-json").textContent = `Failed to load logs: ${error.message}`;
        });
      });

      el("scheduler-control-filter-select")?.addEventListener("change", (event) => {
        schedulerControlFilterMode = event.target.value;
        refreshAll().catch((error) => {
          el("scheduler-control-board").innerHTML = `<div class="strategy-card">Failed to filter scheduler control activity: ${error.message}</div>`;
        });
      });

      el("scheduler-control-reset-button")?.addEventListener("click", () => {
        schedulerControlFilterMode = "all";
        const select = el("scheduler-control-filter-select");
        if (select) {
          select.value = "all";
        }
        refreshAll().catch((error) => {
          el("scheduler-control-board").innerHTML = `<div class="strategy-card">Failed to reset scheduler control activity filter: ${error.message}</div>`;
        });
      });

      el("scheduler-control-board")?.addEventListener("click", async (event) => {
        const replayButton = event.target.closest("[data-replay-scheduler-preset]");
        if (replayButton) {
          const preset = replayButton.dataset.replaySchedulerPreset || "";
          const action = replayButton.dataset.replaySchedulerAction || "";
          try {
            if (action.startsWith("priority_preset:")) {
              await api("/scheduler/strategy/preset", {
                method: "POST",
                body: JSON.stringify({ preset }),
              });
            } else if (action.startsWith("limit_preset:")) {
              await api("/scheduler/strategy/limit-preset", {
                method: "POST",
                body: JSON.stringify({ preset }),
              });
            }
            el("scheduler-message").textContent = `Replayed scheduler preset: ${action}`;
            await refreshAll();
          } catch (error) {
            el("scheduler-message").textContent = `Failed to replay scheduler preset: ${error.message}`;
          }
          return;
        }
        const button = event.target.closest("[data-copy-scheduler-action]");
        if (!button) return;
        const action = button.dataset.copySchedulerAction || "unknown";
        const preset = button.dataset.copySchedulerPreset || "";
        const text = preset ? `action=${action} preset=${preset}` : `action=${action}`;
        try {
          if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(text);
          }
          el("scheduler-message").textContent = `Copied scheduler control action: ${text}`;
        } catch (error) {
          el("scheduler-message").textContent = `Failed to copy scheduler control action: ${error.message}`;
        }
      });

      el("strategy-sort-select")?.addEventListener("change", (event) => {
        strategySortMode = event.target.value;
        refreshAll().catch((error) => {
          el("strategy-summary-board").innerHTML = `<div class="strategy-card">Failed to sort strategies: ${error.message}</div>`;
        });
      });

      el("strategy-filter-select")?.addEventListener("change", (event) => {
        strategyFilterMode = event.target.value;
        refreshAll().catch((error) => {
          el("strategy-summary-board").innerHTML = `<div class="strategy-card">Failed to filter strategies: ${error.message}</div>`;
        });
      });

      el("closed-trades-strategy-select")?.addEventListener("change", (event) => {
        closedTradesStrategyFilter = event.target.value;
        refreshAll().catch((error) => {
          el("strategy-closed-trades-board").innerHTML = `<div class="strategy-card">Failed to filter closed trades: ${error.message}</div>`;
        });
      });

      el("queue-filter-select")?.addEventListener("change", (event) => {
        queueFilterMode = event.target.value;
        try {
          renderQueueSummary(queueSummaryState || {});
        } catch (error) {
          el("queue-board").innerHTML = `<div class="strategy-card">Failed to filter queue jobs: ${error.message}</div>`;
        }
      });

      el("strategy-summary-board")?.addEventListener("click", (event) => {
        const promoteButton = event.target.closest("[data-promote-strategy]");
        if (promoteButton) {
          promoteStrategyPriority(promoteButton.dataset.promoteStrategy).catch((error) => {
            el("scheduler-message").textContent = `Error:\n${error.message}`;
          });
          return;
        }
        const demoteButton = event.target.closest("[data-demote-strategy]");
        if (demoteButton) {
          demoteStrategyPriority(demoteButton.dataset.demoteStrategy).catch((error) => {
            el("scheduler-message").textContent = `Error:\n${error.message}`;
          });
          return;
        }
        const disableButton = event.target.closest("[data-disable-strategy]");
        if (disableButton) {
          disableStrategy(disableButton.dataset.disableStrategy).catch((error) => {
            el("scheduler-message").textContent = `Error:\n${error.message}`;
          });
          return;
        }
        const enableButton = event.target.closest("[data-enable-strategy]");
        if (enableButton) {
          enableStrategy(enableButton.dataset.enableStrategy).catch((error) => {
            el("scheduler-message").textContent = `Error:\n${error.message}`;
          });
          return;
        }
        const card = event.target.closest("[data-strategy-name]");
        if (!card) return;
        const nextStrategy = closedTradesStrategyFilter === card.dataset.strategyName ? "all" : card.dataset.strategyName;
        applyClosedTradesStrategyFilter(nextStrategy);
      });

      el("strategy-summary-board")?.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        const card = event.target.closest("[data-strategy-name]");
        if (!card) return;
        event.preventDefault();
        const nextStrategy = closedTradesStrategyFilter === card.dataset.strategyName ? "all" : card.dataset.strategyName;
        applyClosedTradesStrategyFilter(nextStrategy);
      });

      el("closed-trades-reset-button")?.addEventListener("click", () => {
        applyClosedTradesStrategyFilter("all");
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
        .replace("__CLOSED_TRADE_STRATEGY_OPTIONS__", closed_trade_strategy_options)
        .replace("__PIPELINE_ORCHESTRATION_OPTIONS__", pipeline_orchestration_options)
        .replace("__DEFAULT_STRATEGY_NAME__", DEFAULT_STRATEGY_NAME)
    )
