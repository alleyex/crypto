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
        line-height: 1.45;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(119, 208, 255, 0.16), transparent 28%),
          radial-gradient(circle at top right, rgba(178, 255, 204, 0.12), transparent 20%),
          linear-gradient(180deg, #0a0f14 0%, #0f1720 100%);
      }

      .shell {
        max-width: 1280px;
        margin: 0 auto;
        padding: 88px 20px 48px;
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
        backdrop-filter: blur(10px);
      }

      .hero-main {
        padding: 28px;
      }

      .hero-side {
        padding: 24px;
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
        font-size: 15px;
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
        padding: 7px 11px;
        font-size: 12px;
        color: var(--muted);
        background: rgba(255, 255, 255, 0.03);
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
        grid-template-columns: minmax(0, 1.15fr) minmax(0, 1.2fr) minmax(280px, 0.75fr);
        gap: 20px;
        margin-bottom: 20px;
      }

      .hero-stat-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
        height: 100%;
      }

      .hero-stat-card {
        padding: 18px;
        border-radius: 18px;
        background:
          linear-gradient(180deg, rgba(119, 208, 255, 0.08), rgba(119, 208, 255, 0.02)),
          var(--panel-2);
        border: 1px solid rgba(119, 208, 255, 0.12);
        min-height: 132px;
      }

      .hero-stat-card.wide {
        grid-column: span 2;
      }

      .hero-stat-card label {
        display: block;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 8px;
      }

      .hero-stat-card .value {
        font-size: 24px;
        font-weight: 700;
        margin-bottom: 6px;
      }

      .section-block {
        margin-bottom: 28px;
      }

      .section-header {
        display: flex;
        justify-content: space-between;
        align-items: end;
        gap: 16px;
        margin-bottom: 14px;
        flex-wrap: wrap;
      }

      .section-header h2 {
        margin: 4px 0 0;
        font-size: 24px;
      }

      .section-header p {
        margin: 0;
        color: var(--muted);
        max-width: 72ch;
      }

      .section-kicker {
        color: var(--accent-2);
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-size: 11px;
      }

      .status-board {
        display: grid;
        grid-template-columns: repeat(6, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 20px;
      }

      .status-card {
        padding: 16px 18px;
        border-radius: 18px;
        background: rgba(11, 18, 25, 0.86);
        border: 1px solid rgba(255, 255, 255, 0.05);
      }

      .status-card label {
        display: block;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
      }

      .status-card .value {
        font-size: 20px;
        font-weight: 700;
      }

      .metric-board {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 24px;
      }

      .metric-card {
        padding: 18px;
        border-radius: 18px;
        background: linear-gradient(180deg, rgba(19, 27, 36, 0.98), rgba(10, 16, 23, 0.98));
        border: 1px solid rgba(255, 255, 255, 0.06);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
      }

      .metric-card label {
        display: block;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 8px;
      }

      .metric-primary {
        font-size: 28px;
        font-weight: 700;
        margin-bottom: 8px;
      }

      .metric-secondary {
        color: var(--text);
        font-size: 13px;
        line-height: 1.5;
        margin-bottom: 6px;
      }

      .metric-detail {
        color: var(--muted);
        font-size: 12px;
        line-height: 1.45;
      }

      .overview {
        margin-bottom: 20px;
      }

      .overview-panel {
        padding: 24px;
      }

      .overview-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        gap: 16px;
        margin-bottom: 18px;
        flex-wrap: wrap;
      }

      .overview-header p {
        margin: 0;
        max-width: 72ch;
      }

      .pipeline-ribbon {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        margin-bottom: 18px;
      }

      .pipeline-step {
        position: relative;
        padding: 14px 16px;
        border-radius: 18px;
        border: 1px solid rgba(119, 208, 255, 0.18);
        background:
          linear-gradient(180deg, rgba(119, 208, 255, 0.08), rgba(119, 208, 255, 0.02)),
          #0b1219;
      }

      .pipeline-step:not(:last-child)::after {
        content: "→";
        position: absolute;
        right: -10px;
        top: 50%;
        transform: translateY(-50%);
        color: var(--accent);
        font-weight: 700;
      }

      .pipeline-step strong {
        display: block;
        margin-bottom: 6px;
        font-size: 13px;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }

      .pipeline-step span {
        display: block;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.45;
      }

      .feature-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
      }

      .feature-card {
        border: 1px solid rgba(255, 255, 255, 0.05);
        background: #0b1219;
        border-radius: 18px;
        padding: 18px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
      }

      .feature-card h3 {
        margin: 0 0 8px;
        font-size: 16px;
      }

      .feature-card p {
        margin: 0 0 12px;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.5;
      }

      .feature-points {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
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
        padding: 11px 15px;
        font: inherit;
        font-weight: 700;
        color: #08111a;
        background: linear-gradient(135deg, var(--accent), var(--accent-2));
        cursor: pointer;
        transition: transform 120ms ease, opacity 120ms ease, box-shadow 120ms ease;
        box-shadow: 0 10px 24px rgba(119, 208, 255, 0.16);
      }

      button.secondary {
        color: var(--text);
        background: #223142;
        box-shadow: none;
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

      button:hover { transform: translateY(-1px); box-shadow: 0 14px 30px rgba(119, 208, 255, 0.2); }
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

      .ops-card {
        border: 1px solid rgba(255, 255, 255, 0.05);
        background: #0b1219;
        border-radius: 16px;
        padding: 16px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
      }

      .ops-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 10px;
        flex-wrap: wrap;
      }

      .ops-card-title {
        font-size: 15px;
        font-weight: 700;
      }

      .ops-card-meta {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 10px;
      }

      .ops-card-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px 12px;
        font-size: 12px;
      }

      .ops-card-grid strong {
        display: block;
        margin-bottom: 2px;
        color: var(--text);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .ops-card-note {
        margin-top: 10px;
        color: var(--muted);
        font-size: 12px;
        line-height: 1.45;
      }

      .stats-inline {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin-bottom: 14px;
      }

      .table-title {
        margin: 0 0 8px;
        font-size: 13px;
        font-weight: 700;
        color: var(--text);
      }

      .table-note {
        color: var(--muted);
        font-size: 12px;
        line-height: 1.45;
      }

      .data-table-wrap {
        overflow: auto;
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        background: #0b1219;
        margin-bottom: 14px;
      }

      .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }

      .data-table th,
      .data-table td {
        padding: 12px 14px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      }

      .data-table th {
        text-align: left;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        background: rgba(255, 255, 255, 0.02);
      }

      .data-table tr:last-child td {
        border-bottom: 0;
      }

      .data-table td.num,
      .data-table th.num {
        text-align: right;
      }

      .mini-trade-grid {
        display: grid;
        gap: 10px;
        margin-top: 12px;
      }

      .mini-trade-card {
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 14px;
        background: rgba(255, 255, 255, 0.02);
        padding: 12px 14px;
      }

      .mini-trade-card strong {
        display: block;
        margin-bottom: 4px;
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

      .strategy-hero {
        display: grid;
        grid-template-columns: 1.2fr 0.8fr;
        gap: 14px;
        margin-bottom: 14px;
      }

      .strategy-hero-main {
        display: grid;
        gap: 10px;
      }

      .strategy-rank {
        color: var(--accent);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.1em;
      }

      .strategy-name-row {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }

      .strategy-name-row strong {
        font-size: 20px;
      }

      .strategy-summary-line {
        color: var(--muted);
        font-size: 13px;
        line-height: 1.5;
      }

      .strategy-kpi-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }

      .strategy-kpi {
        padding: 12px;
        border-radius: 14px;
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.04);
      }

      .strategy-kpi strong {
        display: block;
        margin-bottom: 4px;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .strategy-kpi span {
        font-size: 18px;
        font-weight: 700;
      }

      .strategy-secondary-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px 12px;
        font-size: 12px;
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

      details.collapsible {
        margin-top: 14px;
      }

      details.collapsible summary {
        cursor: pointer;
        list-style: none;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 12px 14px;
        border-radius: 14px;
        border: 1px solid rgba(255, 255, 255, 0.05);
        background: #0b1219;
        color: var(--text);
        font-size: 13px;
        font-weight: 600;
      }

      details.collapsible summary::-webkit-details-marker {
        display: none;
      }

      details.collapsible summary::after {
        content: "Show";
        color: var(--muted);
        font-size: 12px;
        font-weight: 500;
      }

      details.collapsible[open] summary::after {
        content: "Hide";
      }

      details.collapsible .collapsible-body {
        margin-top: 10px;
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
        .hero-stat-grid,
        .pipeline-ribbon,
        .feature-grid,
        .metric-board,
        .status-board,
        .strategy-hero,
        .strategy-secondary-grid,
        .grid,
        .worker-grid {
          grid-template-columns: 1fr;
        }

        .pipeline-step:not(:last-child)::after {
          content: "";
        }

        .shell {
          padding: 18px 12px 32px;
        }

        .hero-main,
        .hero-side,
        .overview-panel,
        .control-card,
        .data-card {
          padding: 18px;
        }

        .metric-primary,
        .hero-stat-card .value {
          font-size: 22px;
        }
      }
      /* ---- Tab navigation ---- */
      /* ---- Fixed top navbar ---- */
      .topbar {
        position: fixed;
        top: 0; left: 0; right: 0;
        z-index: 200;
        height: 56px;
        background: rgba(12, 17, 23, 0.92);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-bottom: 1px solid var(--line);
        display: flex;
        align-items: center;
      }
      .topbar-inner {
        max-width: 1280px;
        width: 100%;
        margin: 0 auto;
        padding: 0 20px;
        display: flex;
        align-items: center;
        gap: 24px;
      }
      .topbar-title {
        font-size: 13px;
        font-weight: 700;
        color: var(--accent);
        letter-spacing: 0.06em;
        text-transform: uppercase;
        white-space: nowrap;
        flex-shrink: 0;
      }
      .topbar-nav {
        display: flex;
        gap: 2px;
        flex: 1;
      }
      .tab-btn {
        padding: 7px 16px;
        background: transparent;
        border: none;
        border-radius: 8px;
        color: var(--muted);
        font-size: 13px;
        font-weight: 600;
        cursor: pointer;
        transition: background 0.15s, color 0.15s;
        white-space: nowrap;
      }
      .tab-btn:hover { background: var(--panel-2); color: var(--text); }
      .tab-btn.active {
        background: var(--panel-2);
        color: var(--accent);
        border-bottom: 2px solid var(--accent);
        border-radius: 8px 8px 0 0;
      }
      .tab-panel { display: none; }
      .tab-panel.active { display: block; }
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <span class="topbar-title">Crypto Admin</span>
        <nav class="topbar-nav">
          <button class="tab-btn active" data-tab="overview">總覽</button>
          <button class="tab-btn" data-tab="controls">控制</button>
          <button class="tab-btn" data-tab="market">市場資料</button>
          <button class="tab-btn" data-tab="monitor">監控</button>
          <button class="tab-btn" data-tab="ml">ML / AI</button>
          <button class="tab-btn" data-tab="diagnostics">診斷</button>
        </nav>
      </div>
    </header>
    <main class="shell">
      <div class="tab-panel active" id="tab-overview">
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
          <div class="hero-stat-grid">
            <div class="hero-stat-card">
              <label>System Health</label>
              <div class="value" id="health-status">Loading</div>
              <div class="inline-note">Top-level runtime health across scheduler, DB, queue, and execution.</div>
            </div>
            <div class="hero-stat-card">
              <label>Scheduler</label>
              <div class="value" id="scheduler-status">Loading</div>
              <div class="inline-note" id="scheduler-detail">Checking scheduler runtime state...</div>
            </div>
            <div class="hero-stat-card wide">
              <label>Last Pipeline</label>
              <div class="value" id="pipeline-status">Loading</div>
              <div class="inline-note" id="pipeline-detail">Checking pipeline run summary...</div>
              <div class="inline-note" id="pipeline-symbols">Symbols: loading...</div>
              <div class="inline-note" id="pipeline-counts">Counts: loading...</div>
            </div>
            <div class="hero-stat-card wide">
              <label>Execution Backend</label>
              <div class="value" id="execution-backend-status">Loading</div>
              <div class="inline-note" id="execution-backend-detail">Checking execution backend...</div>
            </div>
          </div>
        </div>
      </section>

      <section class="status-board">
        <div class="status-card">
          <label>Queue</label>
          <div class="value" id="queue-status">Loading</div>
          <div class="inline-note" id="queue-detail">Checking queued worker jobs...</div>
        </div>
        <div class="status-card">
          <label>Kill Switch</label>
          <div class="value" id="kill-switch-status">Loading</div>
          <div class="inline-note">Emergency block on new execution cycles.</div>
        </div>
        <div class="status-card">
          <label>Alerts</label>
          <div class="value" id="alerts-status">Loading</div>
          <div class="inline-note" id="alerts-detail">Checking Telegram delivery state...</div>
        </div>
        <div class="status-card">
          <label>Market Data</label>
          <div class="value" id="market-data-status">Loading</div>
          <div class="inline-note" id="market-data-detail">Checking market data heartbeat...</div>
        </div>
        <div class="status-card">
          <label>Alerting Runtime</label>
          <div class="value" id="alerting-runtime-status">Loading</div>
          <div class="inline-note" id="alerting-runtime-detail">Checking alerting heartbeat...</div>
        </div>
        <div class="status-card">
          <label>Last Refresh</label>
          <div class="value" id="last-refresh">Never</div>
          <div class="inline-note">Dashboard auto-refresh cadence.</div>
        </div>
      </section>

      <section class="metric-board">
        <div class="metric-card">
          <label>Pipeline Throughput</label>
          <div class="metric-primary" id="pipeline-kpi-primary">Loading</div>
          <div class="metric-secondary" id="pipeline-kpi-secondary">Checking latest pipeline counts...</div>
          <div class="metric-detail" id="pipeline-kpi-detail">Waiting for runtime summary.</div>
        </div>
        <div class="metric-card">
          <label>Queue Pressure</label>
          <div class="metric-primary" id="queue-kpi-primary">Loading</div>
          <div class="metric-secondary" id="queue-kpi-secondary">Checking queued and failed jobs...</div>
          <div class="metric-detail" id="queue-kpi-detail">Waiting for queue summary.</div>
        </div>
        <div class="metric-card">
          <label>Alert Readiness</label>
          <div class="metric-primary" id="alerts-kpi-primary">Loading</div>
          <div class="metric-secondary" id="alerts-kpi-secondary">Checking delivery posture...</div>
          <div class="metric-detail" id="alerts-kpi-detail">Waiting for alert status.</div>
        </div>
        <div class="metric-card">
          <label>Soak Progress</label>
          <div class="metric-primary" id="soak-kpi-primary">Loading</div>
          <div class="metric-secondary" id="soak-kpi-secondary">Checking accumulated healthy runtime...</div>
          <div class="metric-detail" id="soak-kpi-detail">Waiting for soak summary.</div>
        </div>
      </section>

      <section class="overview">
        <div class="panel overview-panel">
          <div class="overview-header">
            <div>
              <div class="eyebrow">Capability Overview</div>
              <h2>What This System Does</h2>
              <p>
                This admin surface runs the current trading runtime end to end: ingest market data,
                generate strategy signals, apply risk controls, execute via broker adapters, and keep
                the whole flow observable from one page.
              </p>
            </div>
            <div class="feature-points">
              <span class="chip"><strong>Queue-native</strong> orchestration</span>
              <span class="chip"><strong>Paper-first</strong> validation</span>
              <span class="chip"><strong>Multi-strategy</strong> runtime</span>
            </div>
          </div>
          <div class="pipeline-ribbon">
            <div class="pipeline-step">
              <strong>Market Data</strong>
              <span>Fetch candles, update symbols, and feed the next cycle with fresh inputs.</span>
            </div>
            <div class="pipeline-step">
              <strong>Strategy</strong>
              <span>Run registered strategies across active symbols and persist trade signals.</span>
            </div>
            <div class="pipeline-step">
              <strong>Risk</strong>
              <span>Approve, reject, and size signals before any order reaches execution.</span>
            </div>
            <div class="pipeline-step">
              <strong>Execution</strong>
              <span>Route approved intents to paper, noop, simulated live, or Binance adapters.</span>
            </div>
          </div>
          <div class="feature-grid">
            <article class="feature-card">
              <h3>Runtime Control</h3>
              <p>Operate the scheduler without shell commands and keep emergency controls close to the live state.</p>
              <div class="feature-points">
                <span class="chip">Start / stop scheduler</span>
                <span class="chip">Kill switch</span>
                <span class="chip">Execution backend switching</span>
                <span class="chip">Strategy and symbol selection</span>
              </div>
            </article>
            <article class="feature-card">
              <h3>Execution and Risk</h3>
              <p>Inspect how a signal moves through approval, rejection, order creation, fill capture, and position updates.</p>
              <div class="feature-points">
                <span class="chip">Risk worker</span>
                <span class="chip">Broker protection</span>
                <span class="chip">Portfolio exposure</span>
                <span class="chip">PnL snapshots</span>
              </div>
            </article>
            <article class="feature-card">
              <h3>Queue Operations</h3>
              <p>Run direct or queued orchestration modes and intervene on stale or failed jobs from the dashboard.</p>
              <div class="feature-points">
                <span class="chip">direct</span>
                <span class="chip">queue_dispatch</span>
                <span class="chip">queue_drain</span>
                <span class="chip">queue_batch</span>
              </div>
            </article>
            <article class="feature-card">
              <h3>Observability</h3>
              <p>Track health, heartbeats, audit events, alerts, soak validation, and strategy-by-strategy outcomes in one place.</p>
              <div class="feature-points">
                <span class="chip">Runtime heartbeats</span>
                <span class="chip">Audit trail</span>
                <span class="chip">Alert delivery</span>
                <span class="chip">Soak validation</span>
              </div>
            </article>
          </div>
        </div>
      </section>
      </div>

      <div class="tab-panel" id="tab-controls">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Operations</div>
            <h2>Execution Controls</h2>
          </div>
          <p>Operational controls stay near the top, while low-level diagnostics move further down the page.</p>
        </div>
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
      </section>
      </div>

      <div class="tab-panel" id="tab-monitor">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Monitoring</div>
            <h2>Live Runtime View</h2>
          </div>
          <p>Priority views for trading activity, worker liveness, queue flow, and recent execution behavior.</p>
        </div>
      <section class="grid">
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
          <h2>Queue Summary</h2>
          <p>Queued worker job counts, the latest queue entries, and direct queue recovery actions.</p>
          <div class="inline-controls">
            <label for="queue-filter-select">Filter</label>
            <select id="queue-filter-select">
              <option value="all">all</option>
              <option value="failed">failed only</option>
              <option value="queued">queued only</option>
              <option value="market_data">market_data</option>
              <option value="strategy">strategy</option>
              <option value="risk">risk</option>
              <option value="execution">execution</option>
            </select>
          </div>
          <div class="button-row" style="margin-bottom: 16px;">
            <button class="secondary" data-action="queue-recover-pipeline">Recover Stale Pipeline Batch</button>
            <button class="secondary" data-action="queue-clear-pipeline">Clear Stale Pipeline Batch</button>
            <button class="secondary" data-action="queue-enqueue-strategy">Enqueue Strategy Job</button>
            <button class="secondary" data-action="queue-drain-strategy">Drain Strategy Job</button>
            <button class="secondary" data-action="queue-drain-risk">Drain Risk Job</button>
            <button class="secondary" data-action="queue-drain-execution">Drain Execution Job</button>
            <button class="secondary" data-action="queue-retry-strategy">Retry Failed Strategy Job</button>
            <button class="secondary" data-action="queue-retry-risk">Retry Failed Risk Job</button>
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
              <label>Risk Worker</label>
              <div class="value" id="risk-worker-status">Loading</div>
              <div class="inline-note" id="risk-worker-detail">Checking risk worker heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Execution Worker</label>
              <div class="value" id="execution-worker-status">Loading</div>
              <div class="inline-note" id="execution-worker-detail">Checking execution worker heartbeat...</div>
            </div>
          </div>
          <details class="collapsible">
            <summary>View heartbeat event feed</summary>
            <div class="collapsible-body">
              <pre id="heartbeats-json">Loading...</pre>
            </div>
          </details>
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
              <option value="risk-only">risk-only</option>
              <option value="execution-only">execution-only</option>
            </select>
          </div>
          <pre id="logs-json">Loading...</pre>
        </article>
        <article class="panel data-card">
          <h2>Orders</h2>
          <p>Latest paper-trading orders.</p>
          <details class="collapsible">
            <summary>View raw orders payload</summary>
            <div class="collapsible-body">
              <pre id="orders-json">Loading...</pre>
            </div>
          </details>
        </article>
        <article class="panel data-card">
          <h2>Positions</h2>
          <p>Current position and realized PnL state.</p>
          <details class="collapsible">
            <summary>View raw positions payload</summary>
            <div class="collapsible-body">
              <pre id="positions-json">Loading...</pre>
            </div>
          </details>
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
          <h2>Portfolio Exposure</h2>
          <p>Cross-strategy position exposure and capital limit enforcement status.</p>
          <div id="portfolio-board">Loading...</div>
        </article>
        <article class="panel data-card">
          <h2>Risk Config Overrides</h2>
          <p>Per-strategy risk parameter overrides. Strategies without an override use global defaults.</p>
          <div id="risk-config-board">Loading...</div>
        </article>
      </section>
      </section>
      </div>

      <div class="tab-panel" id="tab-market">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Data Layer</div>
            <h2>市場資料</h2>
          </div>
          <p>管理市場資料的抓取、儲存狀態與特徵計算。</p>
        </div>
      <section class="grid">

        <article class="panel data-card">
          <h2>資料狀態</h2>
          <p>每個交易對的 candle 數量、最新時間與缺口估計。</p>
          <div class="button-row">
            <button class="secondary" data-action="market-status-refresh">Refresh Status</button>
          </div>
          <div id="market-status-board" style="margin-top:12px;">
            <span style="color:var(--muted);font-size:13px;">Click Refresh Status to load.</span>
          </div>
        </article>

        <article class="panel data-card">
          <h2>抓取市場資料</h2>
          <p>獨立觸發市場資料抓取，不需跑完整 pipeline。</p>
          <div class="inline-controls">
            <label for="market-fetch-symbols-input">Symbols（逗號分隔，留空使用 active symbols）</label>
            <input id="market-fetch-symbols-input" type="text" placeholder="BTCUSDT,ETHUSDT" />
          </div>
          <div class="button-row">
            <button data-action="market-fetch">Fetch Now</button>
          </div>
          <div class="message" id="market-fetch-message"></div>
          <pre id="market-fetch-json" style="margin-top:12px;display:none"></pre>
        </article>

        <article class="panel data-card">
          <h2>Feature Store</h2>
          <p>從已儲存的 candle 計算並更新特徵向量。</p>
          <div class="inline-controls">
            <label for="market-fs-symbol-input">Symbol</label>
            <input id="market-fs-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
          </div>
          <div class="inline-controls">
            <label for="market-fs-timeframe-input">Timeframe</label>
            <input id="market-fs-timeframe-input" type="text" placeholder="1m" value="1m" />
          </div>
          <div class="button-row">
            <button data-action="market-fs-materialize">Materialize Features</button>
            <button class="secondary" data-action="market-fs-latest">Latest Feature Vector</button>
          </div>
          <div class="message" id="market-fs-message"></div>
          <pre id="market-fs-json" style="margin-top:12px;display:none"></pre>
        </article>

      </section>
      </section>
      </div>

      <div class="tab-panel" id="tab-ml">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Machine Learning</div>
            <h2>ML / AI</h2>
          </div>
          <p>Feature store, training jobs, model registry, inference, and RL experiments.</p>
        </div>
      <section class="grid">

        <article class="panel data-card">
          <h2>Training Jobs</h2>
          <p>Trigger a supervised logistic regression training job on stored features.</p>
          <div class="inline-controls">
            <label for="ml-train-symbol-input">Symbol</label>
            <input id="ml-train-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
          </div>
          <div class="inline-controls">
            <label for="ml-train-timeframe-input">Timeframe</label>
            <input id="ml-train-timeframe-input" type="text" placeholder="1m" value="1m" />
          </div>
          <div class="inline-controls">
            <label for="ml-train-epochs-input">Epochs</label>
            <input id="ml-train-epochs-input" type="number" value="50" min="1" max="500" />
          </div>
          <div class="button-row">
            <button data-action="ml-train">Start Training</button>
            <button class="secondary" data-action="ml-train-list">View Recent Jobs</button>
          </div>
          <div class="message" id="ml-train-message"></div>
          <pre id="ml-train-json" style="margin-top:12px;display:none"></pre>
        </article>

        <article class="panel data-card">
          <h2>Model Registry</h2>
          <p>View champion model and promote or archive model versions.</p>
          <div class="inline-controls">
            <label for="ml-registry-symbol-input">Symbol</label>
            <input id="ml-registry-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
          </div>
          <div class="inline-controls">
            <label for="ml-registry-timeframe-input">Timeframe</label>
            <input id="ml-registry-timeframe-input" type="text" placeholder="1m" value="1m" />
          </div>
          <div class="inline-controls">
            <label for="ml-registry-model-id-input">Model ID (for promote/archive)</label>
            <input id="ml-registry-model-id-input" type="number" placeholder="1" />
          </div>
          <div class="button-row">
            <button class="secondary" data-action="ml-champion">View Champion</button>
            <button class="secondary" data-action="ml-registry-list">List Models</button>
            <button data-action="ml-promote">Promote</button>
            <button class="danger" data-action="ml-archive">Archive</button>
          </div>
          <div class="message" id="ml-registry-message"></div>
          <pre id="ml-registry-json" style="margin-top:12px;display:none"></pre>
        </article>

        <article class="panel data-card">
          <h2>Inference</h2>
          <p>Get latest prediction from the champion model.</p>
          <div class="inline-controls">
            <label for="ml-infer-symbol-input">Symbol</label>
            <input id="ml-infer-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
          </div>
          <div class="inline-controls">
            <label for="ml-infer-timeframe-input">Timeframe</label>
            <input id="ml-infer-timeframe-input" type="text" placeholder="1m" value="1m" />
          </div>
          <div class="button-row">
            <button data-action="ml-infer-status">Inference Status</button>
            <button data-action="ml-infer-predict">Predict Latest</button>
          </div>
          <div class="message" id="ml-infer-message"></div>
          <pre id="ml-infer-json" style="margin-top:12px;display:none"></pre>
        </article>

        <article class="panel data-card">
          <h2>RL Experiment</h2>
          <p>Train a REINFORCE agent and benchmark it against buy-and-hold and supervised champion.</p>
          <div class="inline-controls">
            <label for="ml-rl-symbol-input">Symbol</label>
            <input id="ml-rl-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
          </div>
          <div class="inline-controls">
            <label for="ml-rl-timeframe-input">Timeframe</label>
            <input id="ml-rl-timeframe-input" type="text" placeholder="1m" value="1m" />
          </div>
          <div class="inline-controls">
            <label for="ml-rl-episodes-input">Episodes</label>
            <input id="ml-rl-episodes-input" type="number" value="100" min="1" max="1000" />
          </div>
          <div class="inline-controls">
            <label>
              <input id="ml-rl-auto-promote" type="checkbox" />
              Auto-promote to champion
            </label>
          </div>
          <div class="button-row">
            <button data-action="ml-rl-train">Run RL Experiment</button>
          </div>
          <div class="message" id="ml-rl-message"></div>
          <pre id="ml-rl-json" style="margin-top:12px;display:none"></pre>
        </article>

      </section>
      </section>
      </div>

      <div class="tab-panel" id="tab-diagnostics">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Diagnostics</div>
            <h2>Deep Inspection</h2>
          </div>
          <p>Raw payloads, audit history, validation snapshots, and lower-level artifacts for incident analysis.</p>
        </div>
      <section class="grid">
        <article class="panel data-card">
          <h2>Health Report</h2>
          <p>Full health payload from the API.</p>
          <details class="collapsible">
            <summary>View raw health payload</summary>
            <div class="collapsible-body">
              <pre id="health-json">Loading...</pre>
            </div>
          </details>
        </article>
        <article class="panel data-card">
          <h2>PnL Snapshots</h2>
          <p>Latest mark-to-market snapshots.</p>
          <details class="collapsible">
            <summary>View raw PnL payload</summary>
            <div class="collapsible-body">
              <pre id="pnl-json">Loading...</pre>
            </div>
          </details>
        </article>
        <article class="panel data-card">
          <h2>Pipeline Result</h2>
          <p>Last manual pipeline action run from this page.</p>
          <details class="collapsible">
            <summary>View manual pipeline result</summary>
            <div class="collapsible-body">
              <pre id="pipeline-json">No manual pipeline run yet.</pre>
            </div>
          </details>
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
          <h2>Audit Events</h2>
          <p>Recent structured events for pipeline, risk, scheduler, and kill switch actions.</p>
          <details class="collapsible">
            <summary>View raw audit event payload</summary>
            <div class="collapsible-body">
              <pre id="audit-json">Loading...</pre>
            </div>
          </details>
        </article>
        <article class="panel data-card">
          <h2>Alert Delivery</h2>
          <p>Telegram configuration and the latest delivery attempt recorded in audit events.</p>
          <div class="button-row" style="margin-bottom: 16px;">
            <button data-action="alert-test">Send Test Alert</button>
          </div>
          <div class="message" id="alerts-message">No test alert sent from this page yet.</div>
          <details class="collapsible">
            <summary>View raw alert payload</summary>
            <div class="collapsible-body">
              <pre id="alerts-json">Loading...</pre>
            </div>
          </details>
        </article>
        <article class="panel data-card">
          <h2>Soak Validation</h2>
          <p>Record runtime validation snapshots and inspect the most recent soak history.</p>
          <div class="button-row" style="margin-bottom: 16px;">
            <button data-action="soak-record">Record Snapshot</button>
          </div>
          <div class="message" id="soak-message">No soak validation snapshot recorded from this page yet.</div>
          <details class="collapsible">
            <summary>View raw soak payload</summary>
            <div class="collapsible-body">
              <pre id="soak-json">Loading...</pre>
            </div>
          </details>
        </article>
      </section>
      </section>

      </div>

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

        const riskWorker = heartbeatMap.risk_worker;
        el("risk-worker-status").textContent = riskWorker
          ? String(riskWorker.status).toUpperCase()
          : "NONE";
        el("risk-worker-status").className = `value ${statusClass(riskWorker ? riskWorker.status : "degraded")}`;
        el("risk-worker-detail").textContent = riskWorker
          ? `${riskWorker.last_seen_at} | ${riskWorker.message}${(riskWorker.payload?.symbol_names || []).length ? ` | symbols: ${riskWorker.payload.symbol_names.join(", ")}` : ""}`
          : "No risk worker heartbeat recorded yet.";

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

        el("alerts-kpi-primary").textContent = configured ? "READY" : "DISABLED";
        el("alerts-kpi-primary").className = `metric-primary ${configured ? "ok" : "warn"}`;
        el("alerts-kpi-secondary").textContent = latest
          ? `${latest.status.toUpperCase()} | ${latest.created_at}`
          : configured
            ? "Telegram configured. No delivery attempts yet."
            : "Bot token or chat id missing.";
        el("alerts-kpi-detail").textContent = latest
          ? latest.message
          : configured
            ? "Alert transport is configured and waiting for the next event."
            : "Configure Telegram before relying on automated notifications.";
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
          el("pipeline-kpi-primary").textContent = "0 fills";
          el("pipeline-kpi-primary").className = "metric-primary warn";
          el("pipeline-kpi-secondary").textContent = "No completed runtime cycles yet.";
          el("pipeline-kpi-detail").textContent = "Run a pipeline manually or wait for the scheduler to complete a cycle.";
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
          el("pipeline-kpi-primary").textContent = `${latestPipelineRun.filled_execution_count ?? 0} fills`;
          el("pipeline-kpi-primary").className = `metric-primary ${statusClass(latestPipelineRun.status)}`;
          el("pipeline-kpi-secondary").textContent =
            `${latestPipelineRun.generated_signal_count ?? 0} signals | ` +
            `${latestPipelineRun.approved_risk_count ?? 0} approved | ` +
            `${latestPipelineRun.rejected_risk_count ?? 0} rejected`;
          el("pipeline-kpi-detail").textContent =
            `${displayStatus} on ${symbolLabel} via ${executionBackend} for ${strategyLabel}.`;
          return;
        }

        const displayStatus = String(latestCompleted.status || "unknown").toUpperCase();
        el("pipeline-status").textContent = displayStatus;
        el("pipeline-status").className = `value ${statusClass(latestCompleted.status)}`;
        el("pipeline-detail").textContent = `${latestCompleted.created_at} | ${latestCompleted.message}`;
        el("pipeline-symbols").textContent = "Symbols: unavailable";
        el("pipeline-counts").textContent = "Counts: unavailable";
        el("pipeline-kpi-primary").textContent = displayStatus;
        el("pipeline-kpi-primary").className = `metric-primary ${statusClass(latestCompleted.status)}`;
        el("pipeline-kpi-secondary").textContent = latestCompleted.created_at || "Completed pipeline event found.";
        el("pipeline-kpi-detail").textContent = latestCompleted.message || "Latest pipeline event did not include summary counts.";
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

      function updateSoakValidation(currentReport, history, summary) {
        el("soak-json").textContent = formatJson({
          current_report: currentReport,
          summary,
          recent_history: history,
        });
        const soakStatus = String(currentReport?.status || summary?.status || "unknown").toUpperCase();
        el("soak-kpi-primary").textContent = `${summary?.accumulated_ok_hours ?? 0}h`;
        el("soak-kpi-primary").className = `metric-primary ${statusClass(currentReport?.status || "degraded")}`;
        el("soak-kpi-secondary").textContent =
          `${soakStatus} | remaining ${summary?.remaining_accumulated_hours ?? "n/a"}h`;
        el("soak-kpi-detail").textContent =
          `continuous=${summary?.continuous_span_hours ?? "n/a"}h | ok_rate=${summary?.ok_rate ?? "n/a"} | longest_ok=${summary?.longest_ok_streak_hours ?? "n/a"}h`;
      }

      function updateQueueMetrics(queueSummary) {
        const counts = queueSummary?.counts || {};
        const queued = counts.queued ?? 0;
        const leased = counts.leased ?? 0;
        const failed = counts.failed ?? 0;
        const recentBatch = queueSummary?.recent_batches?.[0] || null;
        const primaryLabel = failed > 0 ? `${failed} failed` : `${queued} queued`;
        const primaryClass = failed > 0 ? "bad" : queued > 0 || leased > 0 ? "warn" : "ok";
        el("queue-kpi-primary").textContent = primaryLabel;
        el("queue-kpi-primary").className = `metric-primary ${primaryClass}`;
        el("queue-kpi-secondary").textContent =
          `queued=${queued} | leased=${leased} | completed=${counts.completed ?? 0}`;
        el("queue-kpi-detail").textContent = recentBatch
          ? `latest batch: ${recentBatch.batch_id || "n/a"} | ${recentBatch.orchestration || "n/a"} | ${recentBatch.status || "unknown"}`
          : "No recent queue batch recorded.";
      }

      function updateRiskConfig(data) {
        const board = el("risk-config-board");
        if (!board) return;
        const global = data.global_defaults || {};
        const overrides = data.overrides || [];
        const fields = ["order_qty", "max_position_qty", "cooldown_seconds", "max_daily_loss"];
        const fieldLabels = { order_qty: "Order Qty", max_position_qty: "Max Pos Qty", cooldown_seconds: "Cooldown (s)", max_daily_loss: "Max Daily Loss" };

        const rows = overrides.map((cfg) => {
          const cells = fields.map((f) => {
            const val = cfg[f];
            const def = global[f];
            const changed = val !== def;
            return `<td class="num" style="${changed ? "font-weight:bold;color:var(--accent);" : ""}">${val ?? "—"}</td>`;
          }).join("");
          return `<tr><td><strong>${cfg.strategy_name}</strong></td>${cells}<td class="table-note">${cfg.updated_at || ""}</td></tr>`;
        });

        const defaultRow = `<tr style="color:var(--muted);font-style:italic"><td>defaults</td>${fields.map((f) => `<td class="num">${global[f] ?? "—"}</td>`).join("")}<td></td></tr>`;

        board.innerHTML = overrides.length === 0
          ? `<div class="ops-card"><div class="ops-card-title">No strategy overrides configured.</div><div class="ops-card-note">All strategies currently inherit the global defaults below.</div><details class="collapsible"><summary>View global defaults</summary><div class="collapsible-body"><pre>${formatJson(global)}</pre></div></details></div>`
          : `<div class="ops-card">
              <div class="ops-card-header">
                <div class="ops-card-title">Strategy Risk Overrides</div>
                <div class="chip">${overrides.length} override${overrides.length === 1 ? "" : "s"}</div>
              </div>
              <div class="ops-card-note">Values highlighted in accent differ from the global defaults row.</div>
              <div class="data-table-wrap">
                <table class="data-table">
                  <thead><tr><th>Strategy</th>${fields.map((f) => `<th class="num">${fieldLabels[f]}</th>`).join("")}<th>Updated</th></tr></thead>
                  <tbody>${defaultRow}${rows.join("")}</tbody>
                </table>
              </div>
            </div>`;
      }

      function updatePortfolio(data) {
        const board = el("portfolio-board");
        if (!board) return;
        const cfg = data.config || {};
        const positions = data.open_positions || [];
        const perStrategy = Array.isArray(data.per_strategy) ? data.per_strategy : [];
        const violations = data.violations || [];
        const withinLimits = data.within_limits !== false;

        const enforced = cfg.enforcement_active ? "ENFORCED" : "INACTIVE (total_capital=0)";
        const enforcedClass = cfg.enforcement_active ? (withinLimits ? "ok" : "bad") : "warn";

        const posRows = positions.map((p) => `<tr>
          <td><strong>${p.symbol}</strong></td>
          <td class="num">${Number(p.qty || 0).toFixed(4)}</td>
          <td class="num">${p.notional != null ? Number(p.notional).toFixed(2) : "—"}</td>
          <td class="num" style="color:${Number(p.unrealized_pnl || 0) >= 0 ? "var(--ok)" : "var(--bad)"}">${p.unrealized_pnl != null ? Number(p.unrealized_pnl).toFixed(4) : "—"}</td>
        </tr>`).join("");

        const stratRows = perStrategy.map((s) => {
          const symbols = Object.keys(s.open_symbols || {}).join(", ") || "—";
          const withinClass = s.within_limit ? "" : "color:var(--bad);font-weight:bold";
          const pct = cfg.max_strategy_notional ? (Number(s.total_notional || 0) / Number(cfg.max_strategy_notional) * 100).toFixed(1) + "%" : "—";
          return `<tr>
            <td>${s.strategy_name}</td>
            <td class="num">${symbols}</td>
            <td class="num">${s.total_notional != null ? Number(s.total_notional).toFixed(2) : "—"}</td>
            <td class="num">${s.limit_notional != null ? Number(s.limit_notional).toFixed(2) : "—"}</td>
            <td class="num" style="${withinClass}">${pct}</td>
          </tr>`;
        }).join("");

        const violationHtml = violations.length
          ? `<div class="ops-card" style="margin-bottom:12px;border-color:rgba(255,107,107,0.3)"><div class="ops-card-title bad">Exposure Violations</div><div class="ops-card-note">${violations.join("; ")}</div></div>`
          : "";

        board.innerHTML = `
          <div class="stats-inline">
            <div class="side-stat"><label>Enforcement</label><div class="value ${enforcedClass}">${enforced}</div></div>
            <div class="side-stat"><label>Total Capital</label><div class="value">${cfg.total_capital != null ? Number(cfg.total_capital).toLocaleString() + " USDT" : "—"}</div></div>
            <div class="side-stat"><label>Max Strategy %</label><div class="value">${cfg.max_strategy_allocation_pct != null ? (Number(cfg.max_strategy_allocation_pct) * 100).toFixed(0) + "%" : "—"}</div></div>
            <div class="side-stat"><label>Max Total %</label><div class="value">${cfg.max_total_exposure_pct != null ? (Number(cfg.max_total_exposure_pct) * 100).toFixed(0) + "%" : "—"}</div></div>
          </div>
          ${violationHtml}
          ${positions.length ? `
          <p class="table-title">Open Positions (${positions.length})</p>
          <div class="data-table-wrap">
            <table class="data-table">
              <thead><tr><th>Symbol</th><th class="num">Qty</th><th class="num">Notional (USDT)</th><th class="num">Unreal. PnL</th></tr></thead>
              <tbody>${posRows}</tbody>
            </table>
          </div>` : `<p class="table-note" style="margin-bottom:12px">No open positions.</p>`}
          ${perStrategy.length ? `
          <p class="table-title">Per-Strategy Exposure</p>
          <div class="data-table-wrap">
            <table class="data-table">
              <thead><tr><th>Strategy</th><th class="num">Symbols</th><th class="num">Notional</th><th class="num">Limit</th><th class="num">Used %</th></tr></thead>
              <tbody>${stratRows}</tbody>
            </table>
          </div>` : ""}`;
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

        board.innerHTML = sortedStrategies.map((item, index) => {
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
          const winRate = Number(item.realized_trade_count || 0) > 0
            ? `${((Number(item.winning_trade_count || 0) / Number(item.realized_trade_count || 0)) * 100).toFixed(1)}%`
            : "n/a";
          const summaryLine = [
            `signal=${latestSignal}`,
            `risk=${latestRisk}`,
            `order=${latestOrder}`,
            `fill=${latestFill}`,
          ].join(" | ");

          return `
            <div class="strategy-card clickable ${closedTradesStrategyFilter === item.strategy_name ? "selected" : ""}" data-strategy-name="${item.strategy_name}" role="button" tabindex="0" title="Filter recent closed trades for ${item.strategy_name}">
              <div class="strategy-hero">
                <div class="strategy-hero-main">
                  <div class="strategy-rank">Rank #${index + 1}</div>
                  <div class="strategy-name-row">
                    <strong>${item.strategy_name}</strong>
                    <span class="${enabledClass}">${enabledLabel}</span>
                    ${strategyEntry.enabled === false ? `<span class="chip">disabled=${disabledReason}</span>` : ""}
                    ${strategyEntry.effective === false && strategyEntry.active ? `<span class="chip">limited by scheduler</span>` : ""}
                  </div>
                  <div class="strategy-summary-line">${summaryLine}</div>
                  <div class="strategy-card-actions">
                    ${canPromote ? `<button type="button" class="secondary" data-promote-strategy="${item.strategy_name}">Promote</button>` : ""}
                    ${canDemote ? `<button type="button" class="secondary" data-demote-strategy="${item.strategy_name}">Demote</button>` : ""}
                    ${strategyEntry.enabled !== false
                      ? `<button type="button" class="secondary" data-disable-strategy="${item.strategy_name}">Disable</button>`
                      : `<button type="button" class="secondary" data-enable-strategy="${item.strategy_name}">Enable</button>`}
                  </div>
                </div>
                <div class="strategy-kpi-grid">
                  <div class="strategy-kpi"><strong>Gross PnL</strong><span class="${pnlClass}">${pnl}</span></div>
                  <div class="strategy-kpi"><strong>Win Rate</strong><span>${winRate}</span></div>
                  <div class="strategy-kpi"><strong>Filled Orders</strong><span>${item.filled_order_count}</span></div>
                  <div class="strategy-kpi"><strong>Net Qty</strong><span>${item.net_position_qty}</span></div>
                </div>
              </div>
              <div class="strategy-secondary-grid">
                <div class="strategy-metric"><strong>Disabled Reason</strong>${disabledReason}</div>
                <div class="strategy-metric"><strong>Latest Risk</strong>${latestRisk}</div>
                <div class="strategy-metric"><strong>Filled Qty</strong>${item.filled_qty_total}</div>
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
            <div class="strategy-hero">
              <div class="strategy-hero-main">
                <div class="strategy-rank">Focused Strategy View</div>
                <div class="strategy-name-row">
                  <strong>${selected.strategy_name}</strong>
                  <span class="${activityState.className}">${activityState.label}</span>
                </div>
                <div class="strategy-summary-line">
                  signal=${selected.latest_signal?.signal_type || "none"} |
                  risk=${selected.latest_risk?.decision || "none"} |
                  order=${selected.latest_order?.status || "none"} |
                  fill=${selected.latest_fill?.side || "none"}
                </div>
              </div>
              <div class="strategy-kpi-grid">
                <div class="strategy-kpi"><strong>Gross PnL</strong><span class="${pnlClass}">${Number(selected.gross_realized_pnl || 0).toFixed(6)}</span></div>
                <div class="strategy-kpi"><strong>Win Rate</strong><span>${winRate}</span></div>
                <div class="strategy-kpi"><strong>Closed Trades</strong><span>${closedTradeCount}</span></div>
                <div class="strategy-kpi"><strong>Last Closed Result</strong><span class="${lastClosedStatusClass}">${lastClosedStatus}</span></div>
              </div>
            </div>
            <div class="strategy-secondary-grid">
              <div class="strategy-metric"><strong>Latest Signal</strong>${selected.latest_signal?.signal_type || "none"}</div>
              <div class="strategy-metric"><strong>Latest Risk</strong>${selected.latest_risk?.decision || "none"}</div>
              <div class="strategy-metric"><strong>Latest Order</strong>${selected.latest_order?.status || "none"}</div>
              <div class="strategy-metric"><strong>Latest Fill</strong>${selected.latest_fill?.side || "none"}</div>
              <div class="strategy-metric"><strong>Realized Trades</strong>${selected.realized_trade_count}</div>
              <div class="strategy-metric"><strong>Latest Activity</strong>${selected.latest_activity_at || "none"}</div>
              <div class="strategy-metric"><strong>Latest Fill At</strong>${selected.latest_fill_at || "none"}</div>
              <div class="strategy-metric"><strong>Latest Closed Symbol</strong>${latestClosedTrade?.symbol || "none"}</div>
              <div class="strategy-metric"><strong>Latest Closed PnL</strong>${latestClosedTrade ? Number(latestClosedTrade.realized_pnl || 0).toFixed(6) : "n/a"}</div>
            </div>
            <div class="mini-trade-grid">${recentClosedTradesHtml}</div>
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
            <div class="ops-card">
              <div class="ops-card-header">
                <div class="ops-card-title">${item.strategy_name} · ${item.symbol}</div>
                <div class="chip"><span class="${pnlClass}">${item.status}</span></div>
              </div>
              <div class="ops-card-grid">
                <div><strong>Qty</strong>${item.qty}</div>
                <div><strong>Realized PnL</strong><span class="${pnlClass}">${Number(item.realized_pnl).toFixed(6)}</span></div>
                <div><strong>Entry</strong>${Number(item.entry_price).toFixed(4)}</div>
                <div><strong>Exit</strong>${Number(item.exit_price).toFixed(4)}</div>
              </div>
              <div class="ops-card-note">${item.closed_at}</div>
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
            <div class="ops-card">
              <div class="ops-card-header">
                <div class="ops-card-title">Action: ${action}${index === 0 ? ' <span class="ok">LATEST</span>' : ""}</div>
                <div class="chip"><span class="${statusClassName}">${String(event.status || "unknown").toUpperCase()}</span></div>
              </div>
              <div class="ops-card-meta">
                <span class="chip">${event.created_at}</span>
                <span class="chip">${event.source}</span>
                ${preset ? `<span class="chip">preset=${preset}</span>` : ""}
              </div>
              <div class="ops-card-grid">
                <div><strong>Message</strong>${event.message}</div>
                <div><strong>Details</strong>${detailBits.join(" | ") || "no extra detail"}</div>
              </div>
              <div class="ops-card-note">
                <button type="button" class="secondary" data-copy-scheduler-action="${action}" data-copy-scheduler-preset="${preset}">Copy Action</button> ${replayButton}
              </div>
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
        const typeBits = ["market_data", "strategy", "risk", "execution"].map((jobType) => {
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
            <div class="ops-card">
              <div class="ops-card-header">
                <div class="ops-card-title">Job #${job.id} · ${job.job_type}</div>
                <div class="chip"><span class="${jobStatusClass}">${String(job.status).toUpperCase()}</span></div>
              </div>
              <div class="ops-card-meta">
                <span class="chip">attempts=${job.attempt_count}</span>
                <span class="chip">created=${job.created_at}</span>
                <span class="chip">backend=${job.payload?.execution_backend || "unknown"}</span>
              </div>
              <div class="ops-card-note"><strong>Payload</strong> ${payloadText}${errorText}</div>
            </div>
          `;
        }).join("") + `
          <div class="ops-card">
            <div class="ops-card-header">
              <div class="ops-card-title">Queue Debug</div>
              <div class="chip">summary</div>
            </div>
            <div class="ops-card-grid">
              <div><strong>Health</strong>${summaryBits.join(" | ")}</div>
              <div><strong>By Type</strong>${typeBits.join(" | ")}</div>
              <div><strong>Latest Failed</strong>${latestFailedBit}</div>
              <div><strong>Latest Retry</strong>${latestRetryBit}</div>
              <div><strong>Incomplete Batch</strong>${incompleteBatchBit}</div>
              <div><strong>Completed Batch</strong>${completedBatchBit}</div>
            </div>
            <div class="ops-card-note">${batchBits}</div>
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
        const [health, positions, orders, strategySummary, closedTrades, pnl, logs, auditEvents, alertStatus, soakReport, soakHistory, soakSummary, strategies, schedulerStrategy, schedulerSymbols, queueSummary, riskConfig, portfolio] = await Promise.all([
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
          api("/validation/soak/history/summary"),
          api("/strategies"),
          api("/scheduler/strategy"),
          api("/scheduler/symbols"),
          api("/queue/summary"),
          api("/risk-config").catch(() => ({ global_defaults: {}, overrides: [] })),
          api("/portfolio").catch(() => ({ config: {}, open_positions: [], per_strategy: {}, violations: [], within_limits: true })),
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
        updateSoakValidation(soakReport, soakHistory, soakSummary);
        updateQueueMetrics(queueSummary);
        updateRiskConfig(riskConfig);
        updatePortfolio(portfolio);
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
          "queue-drain-risk": "queue-message",
          "queue-drain-execution": "queue-message",
          "queue-retry-strategy": "queue-message",
          "queue-retry-risk": "queue-message",
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
          } else if (type === "queue-drain-risk") {
            result = await api("/queue/jobs/run-next", {
              method: "POST",
              body: JSON.stringify({ job_type: "risk" }),
            });
          } else if (type === "queue-drain-execution") {
            result = await api("/queue/jobs/run-next", {
              method: "POST",
              body: JSON.stringify({ job_type: "execution" }),
            });
          } else if (type === "queue-retry-strategy" || type === "queue-retry-risk" || type === "queue-retry-execution") {
            const jobType = type === "queue-retry-strategy"
              ? "strategy"
              : type === "queue-retry-risk"
                ? "risk"
                : "execution";
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

      // ---- Tab switching ----
      document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          const tabId = btn.dataset.tab;
          document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
          document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
          btn.classList.add("active");
          document.getElementById("tab-" + tabId).classList.add("active");
        });
      });

      // ---- ML / AI helpers ----
      function mlTrainSymbol() { return el("ml-train-symbol-input")?.value.trim() || "BTCUSDT"; }
      function mlTrainTimeframe() { return el("ml-train-timeframe-input")?.value.trim() || "1m"; }
      function mlRegistrySymbol() { return el("ml-registry-symbol-input")?.value.trim() || "BTCUSDT"; }
      function mlRegistryTimeframe() { return el("ml-registry-timeframe-input")?.value.trim() || "1m"; }
      function mlInferSymbol() { return el("ml-infer-symbol-input")?.value.trim() || "BTCUSDT"; }
      function mlInferTimeframe() { return el("ml-infer-timeframe-input")?.value.trim() || "1m"; }
      function mlRlSymbol() { return el("ml-rl-symbol-input")?.value.trim() || "BTCUSDT"; }
      function mlRlTimeframe() { return el("ml-rl-timeframe-input")?.value.trim() || "1m"; }

      function showMlJson(preId, msgId, data, msgText) {
        const pre = el(preId);
        const msg = el(msgId);
        if (pre) { pre.textContent = JSON.stringify(data, null, 2); pre.style.display = "block"; }
        if (msg) { msg.textContent = msgText; msg.className = "message ok"; }
      }

      function showMlError(msgId, err) {
        const msg = el(msgId);
        if (msg) { msg.textContent = String(err); msg.className = "message bad"; }
      }

      document.addEventListener("click", async (event) => {
        const action = event.target.dataset?.action;
        if (!action?.startsWith("ml-")) return;

        if (action === "ml-train") {
          try {
            const epochs = parseInt(el("ml-train-epochs-input")?.value || "50");
            const r = await api(`/training/jobs`, { method: "POST", body: JSON.stringify({ symbol: mlTrainSymbol(), timeframe: mlTrainTimeframe(), n_epochs: epochs }) });
            showMlJson("ml-train-json", "ml-train-message", r, `Training job ${r.id} — status: ${r.status}`);
          } catch (e) { showMlError("ml-train-message", e); }
        }

        if (action === "ml-train-list") {
          try {
            const r = await api(`/training/jobs`);
            showMlJson("ml-train-json", "ml-train-message", r, `${r.total} training jobs found.`);
          } catch (e) { showMlError("ml-train-message", e); }
        }

        if (action === "ml-champion") {
          try {
            const r = await api(`/registry/champion/${mlRegistrySymbol()}?timeframe=${mlRegistryTimeframe()}`);
            showMlJson("ml-registry-json", "ml-registry-message", r, r.champion ? `Champion: ${r.champion.version}` : "No champion.");
          } catch (e) { showMlError("ml-registry-message", e); }
        }

        if (action === "ml-registry-list") {
          try {
            const r = await api(`/registry/models?symbol=${mlRegistrySymbol()}`);
            showMlJson("ml-registry-json", "ml-registry-message", r, `${r.total} models found.`);
          } catch (e) { showMlError("ml-registry-message", e); }
        }

        if (action === "ml-promote") {
          const modelId = el("ml-registry-model-id-input")?.value;
          if (!modelId) { showMlError("ml-registry-message", "Enter a Model ID first."); return; }
          try {
            const r = await api(`/registry/models/${modelId}/promote`, { method: "POST" });
            showMlJson("ml-registry-json", "ml-registry-message", r, `Model ${modelId} promoted to champion.`);
          } catch (e) { showMlError("ml-registry-message", e); }
        }

        if (action === "ml-archive") {
          const modelId = el("ml-registry-model-id-input")?.value;
          if (!modelId) { showMlError("ml-registry-message", "Enter a Model ID first."); return; }
          try {
            const r = await api(`/registry/models/${modelId}/archive`, { method: "POST" });
            showMlJson("ml-registry-json", "ml-registry-message", r, `Model ${modelId} archived.`);
          } catch (e) { showMlError("ml-registry-message", e); }
        }

        if (action === "ml-infer-status") {
          try {
            const r = await api(`/inference/status/${mlInferSymbol()}?timeframe=${mlInferTimeframe()}`);
            showMlJson("ml-infer-json", "ml-infer-message", r, r.ready ? "Inference ready." : "Not ready — no champion or no features.");
          } catch (e) { showMlError("ml-infer-message", e); }
        }

        if (action === "ml-infer-predict") {
          try {
            const r = await api(`/inference/predict/${mlInferSymbol()}?timeframe=${mlInferTimeframe()}`);
            showMlJson("ml-infer-json", "ml-infer-message", r, `Signal: ${r.signal}, probability: ${r.probability}`);
          } catch (e) { showMlError("ml-infer-message", e); }
        }

        if (action === "ml-rl-train") {
          try {
            const episodes = parseInt(el("ml-rl-episodes-input")?.value || "100");
            const autoPromote = el("ml-rl-auto-promote")?.checked || false;
            const r = await api(`/training/rl-jobs`, { method: "POST", body: JSON.stringify({ symbol: mlRlSymbol(), timeframe: mlRlTimeframe(), n_episodes: episodes, auto_promote: autoPromote }) });
            showMlJson("ml-rl-json", "ml-rl-message", r, `RL job done — verdict: ${r.metrics?.verdict ?? r.status}, registry: ${r.registry_status ?? "n/a"}`);
          } catch (e) { showMlError("ml-rl-message", e); }
        }
      });

      // ---- Market Data actions ----
      document.addEventListener("click", async (event) => {
        const action = event.target.dataset?.action;
        if (!action?.startsWith("market-")) return;

        if (action === "market-status-refresh") {
          try {
            const rows = await api("/candles/status");
            const board = el("market-status-board");
            if (!board) return;
            if (!rows || rows.length === 0) {
              board.innerHTML = '<span style="color:var(--muted);font-size:13px;">No candle data found.</span>';
              return;
            }
            board.innerHTML = rows.map((r) => {
              const staleMin = Math.round(r.stale_seconds / 60);
              const staleLabel = staleMin < 5 ? `<span style="color:var(--ok)">${staleMin}m ago</span>`
                : staleMin < 30 ? `<span style="color:var(--warn)">${staleMin}m ago</span>`
                : `<span style="color:var(--bad)">${staleMin}m ago</span>`;
              const gapLabel = r.has_gaps
                ? `<span style="color:var(--warn)">⚠ ~${r.gap_count_estimate} gaps</span>`
                : `<span style="color:var(--ok)">✓ no gaps</span>`;
              return `<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--line);">
                <div><strong>${r.symbol}</strong> <span style="color:var(--muted);font-size:12px">${r.timeframe}</span></div>
                <div style="font-size:13px;display:flex;gap:16px;align-items:center">
                  <span style="color:var(--muted)">${r.count} candles</span>
                  ${staleLabel}
                  ${gapLabel}
                </div>
              </div>`;
            }).join("");
          } catch (e) { if (el("market-status-board")) el("market-status-board").innerHTML = `<span style="color:var(--bad)">${e}</span>`; }
        }

        if (action === "market-fetch") {
          const raw = el("market-fetch-symbols-input")?.value.trim();
          const symbols = raw ? raw.split(",").map((s) => s.trim()).filter(Boolean) : null;
          try {
            const r = await api("/market-data/fetch", { method: "POST", body: JSON.stringify(symbols) });
            const pre = el("market-fetch-json");
            const msg = el("market-fetch-message");
            if (pre) { pre.textContent = JSON.stringify(r, null, 2); pre.style.display = "block"; }
            if (msg) { msg.textContent = `Fetched ${r.saved_klines ?? 0} new candles.`; msg.className = "message ok"; }
          } catch (e) { const msg = el("market-fetch-message"); if (msg) { msg.textContent = String(e); msg.className = "message bad"; } }
        }

        if (action === "market-fs-materialize") {
          const sym = el("market-fs-symbol-input")?.value.trim() || "BTCUSDT";
          const tf = el("market-fs-timeframe-input")?.value.trim() || "1m";
          try {
            const r = await api("/features/materialize", { method: "POST", body: JSON.stringify({ symbol: sym, timeframe: tf, days: 30 }) });
            const pre = el("market-fs-json");
            const msg = el("market-fs-message");
            if (pre) { pre.textContent = JSON.stringify(r, null, 2); pre.style.display = "block"; }
            if (msg) { msg.textContent = `Materialized ${r.upserted ?? r.count ?? ""} feature vectors.`; msg.className = "message ok"; }
          } catch (e) { const msg = el("market-fs-message"); if (msg) { msg.textContent = String(e); msg.className = "message bad"; } }
        }

        if (action === "market-fs-latest") {
          const sym = el("market-fs-symbol-input")?.value.trim() || "BTCUSDT";
          const tf = el("market-fs-timeframe-input")?.value.trim() || "1m";
          try {
            const r = await api(`/features/${sym}/latest?timeframe=${tf}`);
            const pre = el("market-fs-json");
            const msg = el("market-fs-message");
            if (pre) { pre.textContent = JSON.stringify(r, null, 2); pre.style.display = "block"; }
            if (msg) { msg.textContent = "Latest feature vector loaded."; msg.className = "message ok"; }
          } catch (e) { const msg = el("market-fs-message"); if (msg) { msg.textContent = String(e); msg.className = "message bad"; } }
        }
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
