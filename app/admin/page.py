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
      .detail-pill {
        display: inline-block;
        border-radius: 999px;
        padding: 1px 8px;
        margin-left: 2px;
        font-size: 11px;
        border: 1px solid var(--line);
      }
      .detail-pill-expected {
        color: var(--muted);
        background: rgba(255, 255, 255, 0.02);
        opacity: 0.82;
      }
      .detail-pill-anomalous {
        color: var(--warn);
        background: rgba(255, 184, 77, 0.08);
        border-color: rgba(255, 184, 77, 0.28);
      }
      .broker-context-list {
        margin-top: 8px;
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .broker-context-line {
        color: var(--muted);
        font-size: 12px;
        line-height: 1.45;
      }
      .broker-context-line strong {
        color: var(--text);
        font-weight: 600;
      }

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
        grid-template-columns: 1fr 1fr;
        grid-template-rows: auto auto;
        gap: 20px;
        margin-bottom: 20px;
      }

      .controls .scheduler-card {
        grid-column: 2;
        grid-row: 1 / span 2;
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

      .checkbox-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .checkbox-item {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 5px 12px;
        border: 1px solid var(--line);
        border-radius: 6px;
        cursor: pointer;
        font-size: 0.85rem;
        background: var(--surface);
        transition: border-color 0.15s, background 0.15s;
        user-select: none;
      }

      .checkbox-item:hover {
        border-color: var(--accent);
      }

      .checkbox-item input[type=checkbox] {
        accent-color: var(--accent);
        width: 14px;
        height: 14px;
        cursor: pointer;
        margin: 0;
      }

      .result-tabs {
        display: flex;
        gap: 4px;
        margin-bottom: 8px;
      }

      .result-tab {
        padding: 4px 12px;
        font-size: 12px;
        border: 1px solid var(--line);
        border-radius: 6px;
        background: transparent;
        color: var(--muted);
        cursor: pointer;
        transition: all 0.15s;
      }

      .result-tab:hover {
        color: var(--fg);
        border-color: var(--accent);
      }

      .result-tab.active {
        background: var(--accent);
        color: #fff;
        border-color: var(--accent);
      }

      /* ---- Fetch Panel ---- */
      .fetch-panel-desc {
        margin: 2px 0 0;
        color: var(--muted);
        font-size: 13px;
      }

      .fetch-field {
        margin-bottom: 12px;
      }

      .fetch-field-label {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--muted);
        margin-bottom: 8px;
      }

      .fetch-field-hint {
        font-weight: 400;
        text-transform: none;
        letter-spacing: 0;
        color: var(--muted);
        opacity: 0.7;
      }

      .toggle-pill-group {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .toggle-pill {
        padding: 6px 14px;
        border-radius: 20px;
        border: 1px solid var(--line);
        background: transparent;
        color: var(--fg);
        font-size: 13px;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.15s;
        user-select: none;
      }

      .toggle-pill:hover {
        border-color: var(--accent);
        color: var(--accent);
      }

      .toggle-pill.selected {
        background: var(--accent);
        border-color: var(--accent);
        color: #fff;
      }

      .ctrl-section {
        margin-bottom: 18px;
      }

      .ctrl-divider {
        border: none;
        border-top: 1px solid var(--line);
        margin: 18px 0;
      }

      .limit-row {
        display: flex;
        align-items: center;
        gap: 10px;
      }

      .limit-input {
        width: 90px;
        padding: 6px 10px;
        border: 1px solid var(--line);
        border-radius: 6px;
        background: var(--surface);
        color: var(--fg);
        font-size: 14px;
      }

      .limit-presets {
        display: flex;
        gap: 6px;
      }

      .limit-preset-btn {
        padding: 5px 10px;
        font-size: 12px;
        border: 1px solid var(--line);
        border-radius: 6px;
        background: transparent;
        color: var(--muted);
        cursor: pointer;
        transition: all 0.15s;
      }

      .limit-preset-btn:hover {
        border-color: var(--accent);
        color: var(--accent);
      }

      .fetch-actions {
        display: flex;
        gap: 10px;
        margin-bottom: 12px;
      }

      .fetch-btn-primary {
        padding: 8px 20px;
        border-radius: 8px;
        border: none;
        background: var(--accent);
        color: #fff;
        font-size: 14px;
        font-weight: 600;
        cursor: pointer;
        transition: opacity 0.15s;
      }

      .fetch-btn-primary:hover { opacity: 0.85; }

      .fetch-btn-secondary {
        padding: 8px 16px;
        border-radius: 8px;
        border: 1px solid var(--line);
        background: transparent;
        color: var(--fg);
        font-size: 14px;
        cursor: pointer;
        transition: border-color 0.15s;
      }

      .fetch-btn-secondary:hover { border-color: var(--accent); }

      .fetch-result-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 8px;
        margin-top: 8px;
      }

      .fetch-result-summary {
        font-size: 13px;
        font-weight: 600;
        color: var(--good);
      }

      .fetch-result-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 8px 12px;
        border-radius: 6px;
        margin-bottom: 4px;
        background: var(--surface);
        font-size: 13px;
      }

      .fetch-result-row .symbol-name {
        font-weight: 600;
      }

      .fetch-result-count {
        font-weight: 600;
      }

      .fetch-result-count.new { color: var(--good); }
      .fetch-result-count.none { color: var(--muted); }

      /* ---- Candles Table ---- */
      .candles-table-wrap {
        margin-bottom: 24px;
        border: 1px solid var(--line);
        border-radius: 16px;
        overflow: hidden;
      }

      .candles-table-header {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 12px 16px;
        background: rgba(119, 208, 255, 0.05);
        border-bottom: 1px solid var(--line);
      }

      .candles-symbol-badge {
        font-size: 13px;
        font-weight: 700;
        color: var(--accent);
        letter-spacing: 0.04em;
      }

      .candles-header-meta {
        font-size: 11px;
        color: var(--muted);
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }

      .candles-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        font-family: "SFMono-Regular", "Menlo", monospace;
      }

      .candles-table th {
        text-align: right;
        color: var(--muted);
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        padding: 8px 12px;
        background: rgba(0,0,0,0.2);
        border-bottom: 1px solid var(--line);
        white-space: nowrap;
      }

      .candles-table th:first-child { text-align: left; }

      .candles-table td {
        text-align: right;
        padding: 6px 12px;
        border-bottom: 1px solid rgba(255,255,255,0.04);
        color: var(--fg);
        white-space: nowrap;
      }

      .candles-table td:first-child { text-align: left; color: var(--muted); font-size: 11px; }

      .candles-table tbody tr:last-child td { border-bottom: none; }

      .candles-table tbody tr:hover { background: rgba(119, 208, 255, 0.04); }

      .candles-table tbody tr.bull-row { background: rgba(74, 222, 128, 0.03); }
      .candles-table tbody tr.bear-row { background: rgba(248, 113, 113, 0.03); }

      .candles-table td.cell-bull { color: #4ade80; font-weight: 600; }
      .candles-table td.cell-bear { color: #f87171; font-weight: 600; }

      .candles-dir { font-size: 10px; margin-left: 3px; opacity: 0.8; }

      /* ---- JSON Syntax Highlighting ---- */
      .json-key  { color: #7dd3fc; }
      .json-str  { color: #86efac; }
      .json-num  { color: #fb923c; }
      .json-bool { color: #c084fc; }
      .json-null { color: #64748b; }

      /* ---- Pipeline Result ---- */
      .pipeline-result-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.06em;
        margin-bottom: 14px;
      }

      .pipeline-kv-row {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        padding: 7px 0;
        border-bottom: 1px solid var(--line);
        font-size: 13px;
      }

      .pipeline-kv-row:last-child { border-bottom: none; }

      .pipeline-kv-label { color: var(--muted); }

      .pipeline-kv-value {
        color: var(--fg);
        font-weight: 500;
        text-align: right;
        word-break: break-all;
        max-width: 60%;
      }

      /* ---- Data Status Board ---- */
      .status-symbol-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 12px 14px;
        border-radius: 8px;
        background: var(--panel-2);
        margin-bottom: 6px;
      }

      .status-symbol-left {
        display: flex;
        align-items: center;
        gap: 10px;
      }

      .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        flex-shrink: 0;
        box-shadow: 0 0 6px currentColor;
      }

      .status-symbol-name {
        font-weight: 600;
        font-size: 14px;
        margin-right: 6px;
      }

      .status-badge {
        display: inline-block;
        padding: 1px 6px;
        border-radius: 4px;
        background: var(--line);
        color: var(--muted);
        font-size: 11px;
        font-weight: 500;
      }

      .status-symbol-stats {
        display: flex;
        gap: 28px;
        align-items: center;
      }

      .status-stat {
        text-align: right;
      }

      .status-stat-label {
        font-size: 10px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--muted);
        margin-bottom: 2px;
      }

      .status-stat-value {
        font-size: 13px;
        font-weight: 600;
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
        align-items: start;
      }

      .data-card {
        padding: 18px 22px;
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
        border: 1px solid rgba(255, 255, 255, 0.07);
        background: #0b1219;
        border-radius: 18px;
        padding: 20px;
        position: relative;
        overflow: hidden;
      }
      .strategy-card::before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent);
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
        overflow: hidden;
      }
      .ops-card-grid > div {
        min-width: 0;
        overflow-wrap: break-word;
        word-break: break-all;
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
        overflow-wrap: break-word;
        word-break: break-all;
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
        transition: border-color 150ms ease, background 150ms ease;
      }
      .strategy-card.clickable:hover {
        border-color: rgba(119, 208, 255, 0.3);
        background: #0d1520;
      }
      .strategy-card.selected {
        border-color: rgba(119, 208, 255, 0.65);
      }

      /* ── Header ── */
      .strategy-card-top {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 18px;
      }
      .strategy-card-identity { display: flex; flex-direction: column; gap: 4px; }
      .strategy-rank {
        font-size: 10px; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.14em;
        color: var(--muted);
      }
      .strategy-name-row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
      .strategy-name-row strong { font-size: 21px; font-weight: 700; letter-spacing: -0.02em; }
      .strategy-current-price { font-size: 12px; color: var(--muted); margin-top: 2px; }
      .strategy-current-price span { font-size: 15px; font-weight: 600; color: var(--fg); font-variant-numeric: tabular-nums; }
      .strategy-price-book { display: flex; flex-direction: column; gap: 4px; }
      .strategy-price-book-main { display: flex; gap: 10px; flex-wrap: wrap; align-items: baseline; }
      .strategy-price-book-line { font-variant-numeric: tabular-nums; }
      .strategy-price-book-line strong { font-size: 11px; color: var(--muted); margin-right: 4px; }
      .strategy-price-book-mid { font-size: 11px; color: var(--muted); font-variant-numeric: tabular-nums; }
      .strategy-price-book-spread { font-size: 11px; color: var(--muted); font-variant-numeric: tabular-nums; }
      .strategy-card-action-group { display: flex; gap: 6px; flex-shrink: 0; }
      .strategy-card-action-group button {
        font-size: 11px; padding: 4px 10px; border-radius: 7px;
        opacity: 0.55; transition: opacity 120ms;
      }
      .strategy-card-action-group button:hover { opacity: 1; }

      /* ── Prob row ── */
      .strategy-prob-row {
        display: flex; gap: 6px; margin-top: -10px; margin-bottom: 14px;
      }
      .strategy-prob-item {
        font-size: 11px; font-weight: 600;
        padding: 3px 9px; border-radius: 20px;
        background: rgba(255,255,255,0.05);
        color: var(--muted);
      }
      .strategy-prob-item.sig-buy  { background: rgba(52,211,153,0.08); color: #34d399; }
      .strategy-prob-item.sig-sell { background: rgba(248,113,113,0.08); color: #f87171; }

      /* ── Reject reason ── */
      .strategy-reject-reason {
        font-size: 11px; color: var(--bad);
        margin-top: -10px; margin-bottom: 14px;
        padding: 6px 10px; border-radius: 7px;
        background: rgba(248,113,113,0.07);
        border: 1px solid rgba(248,113,113,0.15);
      }

      /* ── Signal row ── */
      .strategy-signal-row {
        display: flex; align-items: center; gap: 8px;
        margin-bottom: 16px; flex-wrap: wrap;
      }
      .strategy-signal-label {
        font-size: 10px; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.1em; color: var(--muted);
      }
      .strategy-signal-value {
        font-size: 11px; font-weight: 700;
        padding: 2px 9px; border-radius: 20px;
        letter-spacing: 0.05em;
      }
      .strategy-signal-value.sig-buy { background: rgba(52,211,153,0.12); color: #34d399; }
      .strategy-signal-value.sig-sell { background: rgba(248,113,113,0.12); color: #f87171; }
      .strategy-signal-value.sig-hold { background: rgba(255,255,255,0.06); color: var(--muted); }
      .strategy-signal-divider { color: rgba(255,255,255,0.15); font-size: 12px; }

      /* ── KPI grid ── */
      .strategy-kpi-row {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px; margin-bottom: 12px;
      }
      .strategy-kpi {
        padding: 11px 13px; border-radius: 11px;
        background: rgba(255,255,255,0.025);
        border: 1px solid rgba(255,255,255,0.05);
      }
      .strategy-kpi strong {
        display: block; margin-bottom: 5px;
        color: var(--muted); font-size: 10px; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.08em;
      }
      .strategy-kpi span { font-size: 16px; font-weight: 700; letter-spacing: -0.01em; }

      /* ── Info rows ── */
      .strategy-info-row {
        display: grid; grid-template-columns: 1fr 1fr;
        gap: 8px; margin-bottom: 8px;
      }
      .strategy-info-cell {
        padding: 9px 13px; border-radius: 10px;
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.04);
        display: flex; flex-direction: column; gap: 3px;
      }
      .strategy-info-cell.full { grid-column: 1 / -1; }
      .strategy-info-label {
        font-size: 10px; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.08em; color: var(--muted);
      }
      .strategy-info-value { font-size: 13px; font-weight: 600; color: var(--text); }
      .strategy-info-value.muted { color: var(--muted); font-weight: 400; }
      .strategy-info-value.ok { color: var(--ok); }
      .strategy-info-value.bad { color: var(--bad); }

      /* ── Footer ── */
      .strategy-card-footer {
        display: flex; align-items: center; justify-content: space-between;
        gap: 12px; padding-top: 12px; margin-top: 4px;
        border-top: 1px solid rgba(255,255,255,0.05);
        font-size: 11px; color: var(--muted); flex-wrap: wrap;
      }
      .strategy-footer-left { display: flex; gap: 16px; align-items: center; }
      .strategy-footer-item { display: flex; gap: 5px; align-items: center; }
      .strategy-footer-item span:first-child {
        font-size: 10px; text-transform: uppercase; letter-spacing: 0.07em;
      }
      .strategy-footer-item span:last-child { font-weight: 600; color: var(--text); }

      /* legacy */
      .strategy-hero { display: none; }
      .strategy-secondary-grid { display: none; }
      .strategy-summary-line { display: none; }
      .strategy-card-header { display: none; }
      .strategy-card-actions { display: none; }
      .strategy-card-grid { display: none; }
      .strategy-metric { display: none; }
      .strategy-kpi-grid { display: none; }
      .strategy-status-line { display: none; }
      .strategy-position-row { display: none; }

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
        .strategy-kpi-row,
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

      .confirm-overlay {
        display: none;
        position: fixed; inset: 0;
        background: rgba(0,0,0,0.6);
        z-index: 9999;
        align-items: center;
        justify-content: center;
      }
      .confirm-overlay.active { display: flex; }
      .confirm-box {
        background: var(--panel-1);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 14px;
        padding: 28px 32px;
        min-width: 320px;
        max-width: 420px;
        text-align: center;
      }
      .confirm-box h3 { margin: 0 0 8px; font-size: 16px; }
      .confirm-box p  { margin: 0 0 20px; color: var(--muted); font-size: 13px; }
      .confirm-box .button-row { justify-content: center; gap: 12px; }

      .training-form-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
        margin: 14px 0 10px;
      }

      .training-field {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }

      .training-field label {
        color: var(--muted);
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .training-kpi-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        margin-top: 14px;
      }

      .training-kpi-card {
        padding: 14px;
        border-radius: 14px;
        background: #0b1219;
        border: 1px solid rgba(255, 255, 255, 0.05);
      }

      .training-kpi-card label {
        display: block;
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
      }

      .training-kpi-card .value {
        font-size: 18px;
        font-weight: 700;
      }

      .training-job-list {
        display: grid;
        gap: 12px;
      }

      .training-job-row {
        cursor: pointer;
        transition: border-color 0.15s ease, transform 0.15s ease;
      }

      .training-job-row:hover {
        border-color: rgba(119, 208, 255, 0.3);
        transform: translateY(-1px);
      }

      .training-job-row.selected {
        border-color: rgba(119, 208, 255, 0.6);
      }

      .training-job-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 12px;
      }

      .training-inline-checks {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin: 12px 0 4px;
      }

      .training-inline-checks label {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        color: var(--muted);
        font-size: 13px;
      }
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <span class="topbar-title">Crypto Admin</span>
        <nav class="topbar-nav">
          <button class="tab-btn active" data-tab="overview">Overview</button>
          <button class="tab-btn" data-tab="controls">Controls</button>
          <button class="tab-btn" data-tab="market">Market Data</button>
          <button class="tab-btn" data-tab="features">Features</button>
          <button class="tab-btn" data-tab="monitor">Monitor</button>
          <button class="tab-btn" data-tab="ml">ML / AI</button>
          <button class="tab-btn" data-tab="training">Training</button>
          <button class="tab-btn" data-tab="reports">Reports</button>
          <button class="tab-btn" data-tab="diagnostics">Diagnostics</button>
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
            Monitor runtime state, inspect paper-trading records, and control the scheduler without dropping into curl commands.
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
            <div class="hero-stat-card wide">
              <label>Broker Context</label>
              <div class="value" id="broker-context-status">Loading</div>
              <div class="broker-context-list" id="broker-context-detail">
                <div class="broker-context-line">Inspecting latest fill, current position, and recent rejected reasons...</div>
              </div>
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

        <!-- Pipeline -->
        <div class="panel control-card">
          <h2>Pipeline</h2>
          <p>Run one full trading cycle and inspect the returned execution summary.</p>
          <div class="ctrl-section">
            <div class="fetch-field-label">Strategy</div>
            <select id="pipeline-strategy-select" style="margin-top:6px">
__STRATEGY_OPTIONS__
            </select>
          </div>
          <div class="ctrl-section">
            <div class="fetch-field-label">Symbols <span class="fetch-field-hint">leave empty to use all active symbols</span></div>
            <div id="pipeline-symbol-pills" class="toggle-pill-group" style="margin-top:6px"></div>
          </div>
          <div class="ctrl-section">
            <div class="fetch-field-label">Orchestration</div>
            <select id="pipeline-orchestration-select" style="margin-top:6px">
__PIPELINE_ORCHESTRATION_OPTIONS__
            </select>
          </div>
          <div class="fetch-actions" style="margin-top:4px">
            <button class="fetch-btn-primary" data-action="pipeline">Run Pipeline</button>
            <button class="fetch-btn-secondary" data-refresh="all">Refresh Data</button>
          </div>
          <div class="auto-refresh" style="margin-top:12px">
            <button class="secondary" data-action="auto-refresh-toggle">Pause Auto Refresh</button>
            <span id="auto-refresh-status">Auto refresh every 10 seconds.</span>
          </div>
          <div class="message" id="pipeline-message" style="display:none"></div>
          <div id="pipeline-result" style="display:none; margin-top:14px">
            <span id="pipeline-result-badge" class="pipeline-result-badge"></span>
            <details style="margin-top:14px">
              <summary style="cursor:pointer; color:var(--muted); font-size:12px; user-select:none">Raw JSON</summary>
              <pre id="pipeline-result-json" style="font-size:11px; overflow:auto; margin-top:8px; white-space:pre-wrap; background:var(--surface); padding:10px; border-radius:6px"></pre>
            </details>
          </div>
        </div>

        <!-- Scheduler -->
        <div class="panel control-card scheduler-card">
          <h2>Scheduler</h2>
          <p>Pause or resume automatic execution without touching launchd state directly.</p>

          <div class="ctrl-section">
            <div class="fetch-field-label">Active Strategies</div>
            <div id="scheduler-strategy-pills" class="toggle-pill-group" style="margin-top:6px"></div>
            <div id="scheduler-disabled-strategy-pills" class="toggle-pill-group" style="margin-top:4px"></div>
          </div>
          <div class="ctrl-section">
            <div class="fetch-field-label">Active Symbols</div>
            <div id="scheduler-symbol-pills" class="toggle-pill-group" style="margin-top:6px"></div>
          </div>
          <div style="margin-bottom:18px">
            <button class="fetch-btn-primary" data-action="scheduler-strategy-save">Apply Strategy State</button>
          </div>

          <hr class="ctrl-divider" />

          <div class="ctrl-section">
            <div class="fetch-field-label">Execution Backend</div>
            <div style="display:flex; gap:8px; align-items:center; margin-top:6px">
              <select id="execution-backend-select">
                <option value="paper">paper</option>
                <option value="noop">noop</option>
                <option value="simulated_live">simulated_live</option>
                <option value="binance">binance</option>
              </select>
              <button class="fetch-btn-secondary" data-action="execution-backend-save">Apply</button>
            </div>
          </div>

          <hr class="ctrl-divider" />

          <div class="ctrl-section">
            <div class="fetch-field-label">Strategy Limit <span class="fetch-field-hint">how many enabled strategies run</span></div>
            <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:6px">
              <input id="scheduler-effective-limit-input" class="limit-input" type="number" step="1" min="1" placeholder="all" style="width:72px" />
              <button class="secondary" type="button" data-action="scheduler-preset-top1">top-1</button>
              <button class="secondary" type="button" data-action="scheduler-preset-top2">top-2</button>
              <button class="secondary" type="button" data-action="scheduler-preset-all">all</button>
            </div>
          </div>
          <div class="ctrl-section">
            <div class="fetch-field-label">Execution Priority <span class="fetch-field-hint">lower number runs first</span></div>
            <div id="scheduler-priority-controls" style="display:flex; flex-wrap:wrap; gap:8px 16px; margin-top:6px; align-items:center"></div>
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:10px">
              <button class="secondary" type="button" data-action="scheduler-priority-sequential">Sequential</button>
              <button class="secondary" type="button" data-action="scheduler-priority-reverse">Reverse</button>
              <button class="secondary" type="button" data-action="scheduler-priority-active-first">Active first</button>
              <button class="secondary" type="button" data-action="scheduler-reset-priorities">Reset</button>
            </div>
          </div>
          <div class="ctrl-section">
            <div class="fetch-field-label">Disabled Notes <span class="fetch-field-hint">explain why a strategy is paused</span></div>
            <div id="scheduler-disabled-note-controls" style="margin-top:6px"></div>
            <button class="secondary" type="button" data-action="scheduler-clear-notes" style="margin-top:8px">Clear Notes</button>
          </div>
          <div class="inline-note" id="scheduler-preset-detail" style="margin-bottom:16px">
            Limit presets change how many enabled strategies run. Priority presets reorder the scheduler execution sequence.
          </div>

          <hr class="ctrl-divider" />

          <div style="display:flex; gap:8px">
            <button class="fetch-btn-primary" data-action="scheduler-start">Start</button>
            <button class="danger" data-action="scheduler-stop">Stop</button>
          </div>
          <div class="message" id="scheduler-message"></div>
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
              <label>Market Data</label>
              <div class="value" id="data-worker-status">Loading</div>
              <div class="inline-note" id="data-worker-detail">Checking market data heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Pipeline</label>
              <div class="value" id="strategy-worker-status">Loading</div>
              <div class="inline-note" id="strategy-worker-detail">Checking pipeline heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Scheduler</label>
              <div class="value" id="risk-worker-status">Loading</div>
              <div class="inline-note" id="risk-worker-detail">Checking scheduler heartbeat...</div>
            </div>
            <div class="side-stat">
              <label>Alerting</label>
              <div class="value" id="execution-worker-status">Loading</div>
              <div class="inline-note" id="execution-worker-detail">Checking alerting heartbeat...</div>
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
          <h2>Recent Fills</h2>
          <p>Latest exchange fills including commission, quote quantity, and exchange transact time when available.</p>
          <div class="trade-list" id="fills-board">
            <div class="strategy-card">Loading...</div>
          </div>
          <details class="collapsible">
            <summary>View raw fills payload</summary>
            <div class="collapsible-body">
              <pre id="fills-json">Loading...</pre>
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
            <h2>Market Data</h2>
          </div>
          <p>Manage candle fetching, freshness, and storage status.</p>
        </div>
      <section class="grid">

        <article class="panel data-card">
          <h2>Data Status</h2>
          <p>Candle count, freshness, and gap estimate per symbol.</p>
          <div class="button-row" style="gap:8px">
            <button class="secondary" data-action="market-status-refresh">Refresh Status</button>
            <button class="secondary" id="market-status-view-toggle" data-view="cards">Coverage Matrix</button>
          </div>
          <div id="market-status-board" style="margin-top:12px;">
            <span style="color:var(--muted);font-size:13px;">Loading...</span>
          </div>
          <div id="market-coverage-matrix" style="display:none;margin-top:12px;overflow-x:auto"></div>
        </article>

        <article class="panel data-card fetch-panel">
          <div class="fetch-panel-header">
            <div>
              <h2>Fetch Market Data</h2>
              <p class="fetch-panel-desc">Trigger market data fetch independently without running the full pipeline. Binance limit: 1000 candles per request.</p>
            </div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Symbols <span class="fetch-field-hint">leave empty to use active symbols</span></div>
            <div id="market-fetch-symbol-checkboxes" class="toggle-pill-group"></div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Timeframes <span class="fetch-field-hint">leave empty to use active timeframes</span></div>
            <div id="market-fetch-timeframe-pills" class="toggle-pill-group"></div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Start Date <span class="fetch-field-hint">leave empty to fetch latest candles only</span></div>
            <div class="limit-row">
              <input id="market-fetch-start-date" class="limit-input" type="date" style="width:160px" />
              <div class="limit-presets">
                <button class="date-preset-btn" data-days="7">7d</button>
                <button class="date-preset-btn" data-days="30">30d</button>
                <button class="date-preset-btn" data-days="90">90d</button>
              </div>
            </div>
          </div>

          <div class="fetch-actions">
            <button class="fetch-btn-primary" data-action="market-fetch">Fetch Now</button>
          </div>
          <div id="market-fetch-result" style="display:none">
            <div class="fetch-result-header">
              <span id="market-fetch-summary" class="fetch-result-summary"></span>
              <div class="result-tabs">
                <button class="result-tab active" data-target="market-fetch-pretty">Pretty</button>
                <button class="result-tab" data-target="market-fetch-raw">Raw JSON</button>
              </div>
            </div>
            <div id="market-fetch-pretty"></div>
            <pre id="market-fetch-raw" style="display:none"></pre>
          </div>
          <div class="message" id="market-fetch-message" style="display:none"></div>
        </article>

      </section>

      <article class="panel data-card" id="market-candles-panel" style="display:none; margin-top:20px">
        <h2 id="market-candles-title">Latest Candles</h2>
        <p>Last 10 candles per symbol from the most recent fetch.</p>
        <div id="market-fetch-candles" style="overflow-x:auto"></div>
      </article>

      <article class="panel data-card fetch-panel" id="candles-quality-panel" style="margin-top:20px">
        <div class="fetch-panel-header">
          <div>
            <h2>Data Quality</h2>
            <p class="fetch-panel-desc">Validate candle data integrity: duplicates, OHLCV violations, gaps, and price spikes.</p>
          </div>
          <button class="fetch-btn-primary" data-action="candles-quality">Run Check</button>
        </div>
        <div id="candles-quality-result" style="display:none; margin-top:16px">
          <div id="candles-quality-status-row" style="display:flex; align-items:center; gap:12px; margin-bottom:16px">
            <span id="candles-quality-badge" class="status-badge"></span>
            <span id="candles-quality-duration" style="font-size:12px; color:var(--muted)"></span>
          </div>
          <div id="candles-quality-messages" style="margin-bottom:16px"></div>
          <div id="candles-quality-sections"></div>
        </div>
        <div class="message" id="candles-quality-message" style="display:none"></div>
      </article>

      <article class="panel data-card" id="fetch-history-panel" style="margin-top:20px">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <h2>Fetch History</h2>
            <p>Recent market data fetch operations.</p>
          </div>
          <button class="secondary" data-action="market-fetch-history-refresh">Refresh</button>
        </div>
        <div id="fetch-history-board" style="margin-top:12px">
          <span style="color:var(--muted);font-size:13px">Loading...</span>
        </div>
      </article>

      </section>
      </div>

      <div class="tab-panel" id="tab-features">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Feature Engineering</div>
            <h2>Features</h2>
          </div>
          <p>Materialize stored feature vectors, inspect the latest values, and use this space as the starting point for feature iteration.</p>
        </div>
      <section class="grid">

        <article class="panel data-card fetch-panel">
          <div class="fetch-panel-header">
            <div>
              <h2>Feature Store</h2>
              <p class="fetch-panel-desc">Compute and update feature vectors from stored candles.</p>
            </div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Symbol</div>
            <div id="market-fs-symbol-pills" class="toggle-pill-group"></div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Timeframe</div>
            <input id="market-fs-timeframe-input" class="limit-input" type="text" placeholder="1m" value="1m" style="width:80px" />
          </div>
          <div class="fetch-actions">
            <button class="fetch-btn-primary" data-action="market-fs-materialize">Materialize Features</button>
            <button class="fetch-btn-secondary" data-action="market-fs-latest">Latest Feature Vector</button>
          </div>
          <div class="message" id="market-fs-message" style="display:none"></div>
          <div id="market-fs-result" style="display:none">
            <div class="fetch-result-header">
              <span id="market-fs-summary" class="fetch-result-summary"></span>
              <div class="result-tabs">
                <button class="result-tab active" data-target="market-fs-pretty">Pretty</button>
                <button class="result-tab" data-target="market-fs-raw">Raw JSON</button>
              </div>
            </div>
            <div id="market-fs-pretty"></div>
            <pre id="market-fs-raw" style="display:none"></pre>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Feature Workflow</h2>
          <p>Use this tab as the handoff between raw candles and model training.</p>
          <div class="feature-points">
            <span class="chip">1. Materialize by symbol and timeframe</span>
            <span class="chip">2. Inspect latest vector values</span>
            <span class="chip">3. Compare feature changes after code updates</span>
            <span class="chip">4. Hand off stable sets to ML / AI</span>
          </div>
          <div class="inline-note" style="margin-top:16px">
            Next useful additions here are coverage, freshness, row-count, null-rate, and feature-set version views.
          </div>
        </article>

        <article class="panel data-card" style="grid-column: 1 / -1;">
          <h2>Active Feature Set — V2 (19 features)</h2>
          <p>Model input features used by PPO and LightGBM. All computed by <code>build_crypto_features()</code> in <code>app/features/crypto_features.py</code>. V2 adds trend, momentum, and K-bar pattern features — PPO walk-forward avg return improved from +40.97% → +80.16%.</p>
          <table class="data-table" style="margin-top:14px">
            <thead>
              <tr>
                <th>Feature</th>
                <th>Category</th>
                <th>Normalization</th>
                <th>Description</th>
                <th class="num">IC t-stat</th>
              </tr>
            </thead>
            <tbody>
              <tr><td><code>log_ret_1</code></td><td>Returns</td><td>clip ±0.20</td><td>1-bar log return</td><td class="num" style="color:#4ade80">−5.82 ✓</td></tr>
              <tr><td><code>log_ret_3</code></td><td>Returns</td><td>clip ±0.30</td><td>3-bar log return</td><td class="num" style="color:#4ade80">−2.35 ✓</td></tr>
              <tr><td><code>log_ret_5</code></td><td>Returns</td><td>clip ±0.40</td><td>5-bar log return</td><td class="num" style="color:#4ade80">−3.29 ✓</td></tr>
              <tr><td><code>log_ret_10</code></td><td>Returns</td><td>clip ±0.60</td><td>10-bar log return</td><td class="num">−1.89</td></tr>
              <tr><td><code>log_ret_20</code></td><td>Returns</td><td>clip ±0.80</td><td>20-bar log return</td><td class="num" style="color:#4ade80">−2.60 ✓</td></tr>
              <tr><td><code>flow_imbalance</code></td><td>Order Flow</td><td>bounded [−1, 1]</td><td>Taker buy imbalance: 2×taker_buy/volume − 1</td><td class="num">−0.31</td></tr>
              <tr><td><code>hl_spread</code></td><td>Volatility</td><td>clip [0, 0.5]</td><td>(high − low) / close</td><td class="num">+0.00</td></tr>
              <tr><td><code>dist_sma_60</code></td><td>Trend</td><td>clip ±0.20</td><td>(close − SMA60) / close</td><td class="num" style="color:#4ade80">−3.07 ✓</td></tr>
              <tr><td><code>rsi_14</code></td><td>Momentum</td><td>bounded [−1, 1]</td><td>RSI(14) normalised: RSI/50 − 1</td><td class="num">−1.38</td></tr>
              <tr><td><code>close_location</code></td><td>K-bar Pattern</td><td>bounded [0, 1]</td><td>(close − low) / (high − low) — 棒內收盤位置</td><td class="num" style="color:#4ade80">+2.61 ✓</td></tr>
              <tr><td><code>upper_wick_ratio</code></td><td>K-bar Pattern</td><td>bounded [0, 1]</td><td>上影線 / (high − low) — 賣壓拒絕</td><td class="num" style="color:#4ade80">−2.95 ✓</td></tr>
              <tr><td><code>lower_wick_ratio</code></td><td>K-bar Pattern</td><td>bounded [0, 1]</td><td>下影線 / (high − low) — 買盤支撐</td><td class="num" style="color:#4ade80">+2.68 ✓</td></tr>
              <tr><td><code>atr_14_norm_z</code></td><td>Volatility</td><td>rolling z-score w=50</td><td>ATR(14) / close z-scored</td><td class="num">−1.01</td></tr>
              <tr><td><code>rv_20_z</code></td><td>Volatility</td><td>rolling z-score w=50</td><td>20-bar realized volatility z-scored</td><td class="num">−0.62</td></tr>
              <tr><td><code>hl_spread_z</code></td><td>Volatility</td><td>rolling z-score w=50</td><td>hl_spread z-scored</td><td class="num" style="color:#4ade80">−2.13 ✓</td></tr>
              <tr><td><code>log_vol_z</code></td><td>Volume</td><td>log1p → z-score w=50</td><td>log(volume+1) z-scored</td><td class="num" style="color:#4ade80">−2.04 ✓</td></tr>
              <tr><td><code>log_trades_z</code></td><td>Volume</td><td>log1p → z-score w=50</td><td>log(trades+1) z-scored</td><td class="num">−1.33</td></tr>
              <tr><td><code>avg_quote_per_trade_z</code></td><td>Volume</td><td>log1p → z-score w=50</td><td>quote_asset_volume / trades z-scored</td><td class="num">−1.02</td></tr>
              <tr><td><code>liquidity_proxy_z</code></td><td>Liquidity</td><td>log1p → robust z-score w=100</td><td>volume / hl_spread z-scored (IQR-based)</td><td class="num">−1.04</td></tr>
            </tbody>
          </table>
          <div class="inline-note" style="margin-top:10px">✓ = |t-stat| &gt; 2（統計顯著，95% 信心水準）。IC 為負 = 均值回歸；IC 為正 = 動能延續。已移除：<code>dist_sma_20</code>（r=+0.92 與 log_ret_10 重複）、<code>body_ratio</code>（t=+0.23 無訊號）、<code>taker_ratio</code>（r=1.0 與 flow_imbalance 完全重複）。</div>
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
          <p>Training jobs, model registry, inference, and RL experiments.</p>
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

      <div class="tab-panel" id="tab-training">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Training</div>
            <h2>PPO Training Workspace</h2>
          </div>
          <p>Train the PPO (Stable Baselines3) strategy model directly from this console. The trained candidate model can be deployed to replace the active model used in live trading.</p>
        </div>
      <section class="grid">

        <article class="panel data-card">
          <h2>Run PPO Training</h2>
          <p>Trains the <strong>PPO (Stable Baselines3)</strong> model — the same model used in live trading. Training runs in the background; refresh the job list to check progress.</p>
          <div class="training-form-grid">
            <div class="training-field">
              <label for="ppo-symbol-input">Symbol</label>
              <input id="ppo-symbol-input" type="text" placeholder="BTCUSDT" value="BTCUSDT" />
            </div>
            <div class="training-field">
              <label for="ppo-timeframe-input">Timeframe</label>
              <input id="ppo-timeframe-input" type="text" placeholder="1m" value="1m" />
            </div>
            <div class="training-field">
              <label for="ppo-steps-input">Total Steps</label>
              <input id="ppo-steps-input" type="number" value="1000000" min="10000" max="10000000" step="100000" />
            </div>
            <div class="training-field">
              <label for="ppo-eval-windows-input">Eval Windows</label>
              <input id="ppo-eval-windows-input" type="number" value="8" min="1" max="20" />
            </div>
            <div class="training-field">
              <label for="ppo-fee-rate-input">Fee Rate (per side)</label>
              <input id="ppo-fee-rate-input" type="number" value="0.001" min="0" max="0.05" step="0.0001" />
            </div>
            <div class="training-field">
              <label for="ppo-seed-input">Seed</label>
              <input id="ppo-seed-input" type="number" value="42" min="0" max="999999" />
            </div>
          </div>
          <details style="margin:12px 0 4px">
            <summary style="cursor:pointer;color:var(--muted);font-size:12px;letter-spacing:0.08em;text-transform:uppercase">Advanced Hyperparameters</summary>
            <div class="training-form-grid" style="margin-top:10px">
              <div class="training-field">
                <label for="ppo-lr-input">Learning Rate</label>
                <input id="ppo-lr-input" type="number" value="0.0003" min="0.000001" max="0.1" step="0.0001" />
              </div>
              <div class="training-field">
                <label for="ppo-n-steps-input">N Steps</label>
                <input id="ppo-n-steps-input" type="number" value="2048" min="64" max="8192" step="64" />
              </div>
              <div class="training-field">
                <label for="ppo-batch-size-input">Batch Size</label>
                <input id="ppo-batch-size-input" type="number" value="256" min="16" max="2048" step="16" />
              </div>
              <div class="training-field">
                <label for="ppo-n-epochs-input">N Epochs</label>
                <input id="ppo-n-epochs-input" type="number" value="10" min="1" max="50" />
              </div>
              <div class="training-field">
                <label for="ppo-gamma-input">Gamma</label>
                <input id="ppo-gamma-input" type="number" value="0.99" min="0" max="1" step="0.01" />
              </div>
            </div>
          </details>
          <div class="button-row">
            <button data-action="ppo-train-run">Start PPO Training</button>
            <button class="secondary" data-action="ppo-jobs-refresh">Refresh Jobs</button>
          </div>
          <div class="message" id="ppo-train-message">PPO training workspace ready.</div>
          <div style="margin-top:10px;font-size:12px;color:var(--muted)">
            TensorBoard:
            <a id="ppo-tb-link" href="http://localhost:6006" target="_blank" style="color:var(--accent)">http://localhost:6006</a>
            &nbsp;—&nbsp;start with:
            <code style="font-size:11px;background:var(--panel-2);padding:2px 6px;border-radius:4px">.venv/bin/tensorboard --logdir runtime/tb_logs</code>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Selected Job</h2>
          <p>Click a job in the history list to inspect its result and deploy.</p>
          <div class="training-kpi-grid" id="ppo-summary-kpis">
            <div class="training-kpi-card"><label>Status</label><div class="value" id="ppo-kpi-status">—</div></div>
            <div class="training-kpi-card"><label>Verdict</label><div class="value" id="ppo-kpi-verdict">—</div></div>
            <div class="training-kpi-card"><label>PPO avg return</label><div class="value" id="ppo-kpi-ppo-ret">—</div></div>
            <div class="training-kpi-card"><label>B&H avg return</label><div class="value" id="ppo-kpi-bnh-ret">—</div></div>
          </div>
          <div id="ppo-progress-wrap" style="display:none;margin-top:14px">
            <div style="display:flex;justify-content:space-between;margin-bottom:4px">
              <span style="font-size:12px;color:var(--muted)">Training progress</span>
              <span style="font-size:12px;color:var(--muted)" id="ppo-progress-label">0%</span>
            </div>
            <div style="background:var(--panel-2);border-radius:4px;height:8px;overflow:hidden">
              <div id="ppo-progress-bar" style="background:var(--accent);height:100%;width:0%;transition:width 0.5s ease"></div>
            </div>
          </div>
          <div class="ops-card" style="margin-top:14px">
            <div class="ops-card-header">
              <div class="ops-card-title" id="ppo-selected-job-title">No job selected</div>
              <div class="chip" id="ppo-selected-job-chip">—</div>
            </div>
            <div class="ops-card-grid" id="ppo-summary-grid">
              <div><strong>Dataset</strong>Select or run a training job.</div>
              <div><strong>Walk-forward</strong>Win rate and per-window results appear here.</div>
              <div><strong>Hyperparameters</strong>Steps, fee rate, and SB3 params appear here.</div>
              <div><strong>Model</strong>Candidate path and deploy status appear here.</div>
            </div>
          </div>
          <div id="ppo-wf-table-wrap" style="display:none;margin-top:14px">
            <div style="font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px">Walk-Forward Results</div>
            <div class="data-table-wrap">
              <table class="data-table" id="ppo-wf-table">
                <thead><tr><th>Win</th><th class="num">PPO ret</th><th class="num">B&H ret</th><th class="num">Edge</th><th class="num">Trades</th><th>Result</th></tr></thead>
                <tbody></tbody>
              </table>
            </div>
          </div>
          <div id="ppo-deploy-wrap" style="display:none;margin-top:14px">
            <div class="button-row">
              <button data-action="ppo-deploy" id="ppo-deploy-btn">Deploy as Active Model</button>
            </div>
            <div class="message" id="ppo-deploy-message"></div>
          </div>
          <details style="margin-top:12px">
            <summary style="cursor:pointer;color:var(--muted);font-size:12px">View raw payload</summary>
            <pre id="ppo-job-json" style="margin-top:8px;display:none"></pre>
          </details>
        </article>

        <article class="panel data-card" style="grid-column: 1 / -1;">
          <div class="section-header" style="margin-bottom:12px">
            <div>
              <div class="section-kicker">History</div>
              <h2 style="margin:4px 0 0;font-size:20px">PPO Training Jobs</h2>
            </div>
            <p style="margin:0">Newest jobs first. Click any job to inspect its metrics and deploy.</p>
          </div>
          <div class="training-job-list" id="ppo-jobs-board">
            <div class="ops-card"><div class="ops-card-title">Loading...</div></div>
          </div>
        </article>

      </section>
      </section>
      </div>

      <div class="tab-panel" id="tab-reports">
      <section class="section-block">
        <div class="section-header">
          <div>
            <div class="section-kicker">Reports</div>
            <h2>Testnet Execution Report</h2>
          </div>
          <p>Review recent Binance testnet trading results with gross PnL, fees, net performance, failed executions, and latest fills in one place.</p>
        </div>
      <section class="grid">
        <article class="panel data-card">
          <h2>Report Filters</h2>
          <p>Query the latest execution report for a symbol and optional strategy window.</p>
          <div class="training-form-grid">
            <div class="training-field">
              <label for="report-symbol-input">Symbol</label>
              <input id="report-symbol-input" type="text" value="BTCUSDT" placeholder="BTCUSDT" />
            </div>
            <div class="training-field">
              <label for="report-strategy-input">Strategy</label>
              <input id="report-strategy-input" type="text" value="ppo" placeholder="ppo or leave blank" />
            </div>
            <div class="training-field">
              <label for="report-days-input">Days</label>
              <input id="report-days-input" type="number" value="7" min="1" max="30" />
            </div>
          </div>
          <div class="button-row">
            <button data-action="report-refresh">Refresh Report</button>
          </div>
          <div class="message" id="report-message">Loading execution report...</div>
        </article>

        <article class="panel data-card">
          <h2>Summary</h2>
          <p>Top-line result after including recorded or estimated commissions.</p>
          <div class="training-kpi-grid" id="report-summary-kpis">
            <div class="training-kpi-card"><label>Gross PnL</label><div class="value">n/a</div></div>
            <div class="training-kpi-card"><label>Fees</label><div class="value">n/a</div></div>
            <div class="training-kpi-card"><label>Net PnL</label><div class="value">n/a</div></div>
            <div class="training-kpi-card"><label>Win Rate</label><div class="value">n/a</div></div>
          </div>
          <div class="ops-card" style="margin-top:14px">
            <div class="ops-card-grid" id="report-summary-grid">
              <div><strong>Window</strong>Loading...</div>
              <div><strong>Activity</strong>Loading...</div>
              <div><strong>Holding</strong>Loading...</div>
              <div><strong>Current Position</strong>Loading...</div>
            </div>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Daily Breakdown</h2>
          <p>Daily fills, notional, fees, and gross/net PnL.</p>
          <div class="data-table-wrap">
            <table class="data-table" id="report-daily-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th class="num">Fills</th>
                  <th class="num">Notional</th>
                  <th class="num">Fees</th>
                  <th class="num">Gross</th>
                  <th class="num">Net</th>
                </tr>
              </thead>
              <tbody>
                <tr><td colspan="6" class="table-note">Loading...</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Failed Executions</h2>
          <p>Recent execution job failures with broker error detail.</p>
          <div class="trade-list" id="report-failed-board">
            <div class="strategy-card">Loading...</div>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Recent Closed Trades</h2>
          <p>Latest closed trade outcomes for the selected report scope.</p>
          <div class="trade-list" id="report-closed-trades-board">
            <div class="strategy-card">Loading...</div>
          </div>
        </article>

        <article class="panel data-card">
          <h2>Recent Fills</h2>
          <p>Latest fills contributing to this report.</p>
          <div class="trade-list" id="report-fills-board">
            <div class="strategy-card">Loading...</div>
          </div>
          <details class="collapsible">
            <summary>View raw report payload</summary>
            <div class="collapsible-body">
              <pre id="report-json">Loading...</pre>
            </div>
          </details>
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
          <p>Recent structured events for pipeline, risk, and scheduler actions.</p>
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
          <h2>Data Retention</h2>
          <p>Purge old records to keep the database lean. Completed job queue rows and old audit events are safe to remove.</p>
          <div class="fetch-field">
            <div class="fetch-field-label">Audit Events <span class="fetch-field-hint">delete records older than N days</span></div>
            <div class="limit-row">
              <input id="retention-audit-days" class="limit-input" type="number" value="90" min="1" />
              <div class="limit-presets">
                <button class="retention-preset-btn" data-field="retention-audit-days" data-days="30">30d</button>
                <button class="retention-preset-btn" data-field="retention-audit-days" data-days="90">90d</button>
                <button class="retention-preset-btn" data-field="retention-audit-days" data-days="180">180d</button>
              </div>
            </div>
          </div>
          <div class="fetch-field">
            <div class="fetch-field-label">Job Queue <span class="fetch-field-hint">delete done/failed rows older than N days</span></div>
            <div class="limit-row">
              <input id="retention-job-days" class="limit-input" type="number" value="30" min="1" />
              <div class="limit-presets">
                <button class="retention-preset-btn" data-field="retention-job-days" data-days="7">7d</button>
                <button class="retention-preset-btn" data-field="retention-job-days" data-days="30">30d</button>
                <button class="retention-preset-btn" data-field="retention-job-days" data-days="60">60d</button>
              </div>
            </div>
          </div>
          <div class="fetch-actions">
            <button class="fetch-btn-primary" data-action="retention-run">Run Retention</button>
          </div>
          <div class="message" id="retention-message" style="display:none"></div>
          <div id="retention-result" style="display:none">
            <div class="fetch-result-header">
              <span id="retention-summary" class="fetch-result-summary"></span>
            </div>
            <div id="retention-pretty"></div>
          </div>
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
      let _schedulerPillsDirty = false;
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

      function syntaxHighlightJson(value) {
        const raw = typeof value === "string" ? value : JSON.stringify(value, null, 2);
        // Escape HTML entities first
        const escaped = raw
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;");
        return escaped.replace(
          /("(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
          (match) => {
            let cls = "json-num";
            if (/^"/.test(match)) {
              cls = /:$/.test(match) ? "json-key" : "json-str";
            } else if (/true|false/.test(match)) {
              cls = "json-bool";
            } else if (/null/.test(match)) {
              cls = "json-null";
            }
            return `<span class="${cls}">${match}</span>`;
          }
        );
      }

      function statusClass(status) {
        if (status === "ok") return "ok";
        if (status === "degraded") return "warn";
        if (status === "error" || status === "blocked") return "bad";
        return "";
      }

      function renderInlineDetails(parts) {
        return parts.map((part) => {
          if (typeof part === "string") return part;
          const cls = part.className ? `detail-pill ${part.className}` : "detail-pill";
          return `<span class="${cls}">${part.text}</span>`;
        }).join(" | ");
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
        if (brokerProtection.expected_rejected_risk_streak !== undefined) {
          executionDetail.push({ text: `expected streak=${brokerProtection.expected_rejected_risk_streak}`, className: "detail-pill-expected" });
        }
        if (brokerProtection.anomalous_rejected_risk_streak !== undefined) {
          executionDetail.push({ text: `anomalous streak=${brokerProtection.anomalous_rejected_risk_streak}`, className: "detail-pill-anomalous" });
        }
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
        el("execution-backend-detail").innerHTML = renderInlineDetails(executionDetail);
        const brokerContextStatus = brokerProtection.status === "degraded"
          ? "ANOMALOUS"
          : (brokerProtection.expected_rejected_risk_streak !== undefined ? "EXPECTED" : "CLEAR");
        el("broker-context-status").textContent = brokerContextStatus;
        el("broker-context-status").className = `value ${brokerProtection.status === "degraded" ? statusClass("degraded") : (brokerProtection.expected_rejected_risk_streak !== undefined ? "" : statusClass("ok"))}`;
        const brokerContextLines = [];
        const latestFill = brokerProtection.latest_fill || null;
        if (latestFill) {
          brokerContextLines.push(
            `<div class="broker-context-line"><strong>Latest fill</strong> ${latestFill.symbol || "n/a"} ${latestFill.side || "n/a"} qty=${latestFill.qty ?? "n/a"} price=${latestFill.price ?? "n/a"} age=${latestFill.age_seconds ?? "n/a"}s</div>`
          );
        } else {
          brokerContextLines.push('<div class="broker-context-line"><strong>Latest fill</strong> none</div>');
        }
        const currentPosition = brokerProtection.current_position || null;
        if (currentPosition) {
          const updatedSuffix = currentPosition.updated_at ? ` updated=${currentPosition.updated_at}` : "";
          brokerContextLines.push(
            `<div class="broker-context-line"><strong>Current position</strong> ${currentPosition.symbol || "n/a"} qty=${currentPosition.qty ?? "n/a"} avg=${currentPosition.avg_price ?? "n/a"} realized_pnl=${currentPosition.realized_pnl ?? "n/a"}${updatedSuffix}</div>`
          );
        } else {
          brokerContextLines.push('<div class="broker-context-line"><strong>Current position</strong> unavailable</div>');
        }
        const recentRejects = Array.isArray(brokerProtection.recent_rejection_reasons)
          ? brokerProtection.recent_rejection_reasons
          : [];
        if (recentRejects.length) {
          const rejectSummary = recentRejects
            .map((item) => `${item.signal_type || "n/a"}: ${item.reason || "n/a"}`)
            .join(" | ");
          brokerContextLines.push(
            `<div class="broker-context-line"><strong>Recent rejects</strong> ${rejectSummary}</div>`
          );
        } else {
          brokerContextLines.push('<div class="broker-context-line"><strong>Recent rejects</strong> none</div>');
        }
        el("broker-context-detail").innerHTML = brokerContextLines.join("");

        el("last-refresh").textContent = new Date().toLocaleTimeString();

        const strip = el("status-strip");
        strip.innerHTML = "";
        const chips = [
          ["health", health.status],
          ["scheduler", scheduler.stopped ? "stopped" : scheduler.status],
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
            if (brokerCheck.expected_rejected_risk_streak !== undefined) detailBits.push(`<span class="detail-pill detail-pill-expected">expected streak=${brokerCheck.expected_rejected_risk_streak}</span>`);
            if (brokerCheck.anomalous_rejected_risk_streak !== undefined) detailBits.push(`<span class="detail-pill detail-pill-anomalous">anomalous streak=${brokerCheck.anomalous_rejected_risk_streak}</span>`);
            if (brokerCheck.latest_order?.status) detailBits.push(`latest_order=${brokerCheck.latest_order.status}`);
            if (brokerCheck.latest_order?.age_seconds !== undefined) detailBits.push(`latest_order_age=${brokerCheck.latest_order.age_seconds}s`);
            if (brokerCheck.recommended_action === "switch_to_paper_backend") {
              issueActionButton = ' <button type="button" class="secondary" data-action="broker-switch-paper">Switch to paper</button>';
            } else if (brokerCheck.recommended_action === "pause_scheduler") {
              issueActionButton = ' <button type="button" class="secondary" data-action="broker-pause-scheduler">Pause scheduler</button>';
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

        const dataWorker = heartbeatMap.market_data;
        el("data-worker-status").textContent = dataWorker
          ? String(dataWorker.status).toUpperCase()
          : "NONE";
        el("data-worker-status").className = `value ${statusClass(dataWorker ? dataWorker.status : "degraded")}`;
        el("data-worker-detail").textContent = dataWorker
          ? `${dataWorker.last_seen_at} | ${dataWorker.message}`
          : "No market data heartbeat recorded yet.";

        const strategyWorker = heartbeatMap.pipeline;
        el("strategy-worker-status").textContent = strategyWorker
          ? String(strategyWorker.status).toUpperCase()
          : "NONE";
        el("strategy-worker-status").className = `value ${statusClass(strategyWorker ? strategyWorker.status : "degraded")}`;
        el("strategy-worker-detail").textContent = strategyWorker
          ? `${strategyWorker.last_seen_at} | ${strategyWorker.message}`
          : "No pipeline heartbeat recorded yet.";

        const riskWorker = heartbeatMap.scheduler;
        el("risk-worker-status").textContent = riskWorker
          ? String(riskWorker.status).toUpperCase()
          : "NONE";
        el("risk-worker-status").className = `value ${statusClass(riskWorker ? riskWorker.status : "degraded")}`;
        el("risk-worker-detail").textContent = riskWorker
          ? `${riskWorker.last_seen_at} | ${riskWorker.message}`
          : "No scheduler heartbeat recorded yet.";

        const executionWorker = heartbeatMap.alerting;
        el("execution-worker-status").textContent = executionWorker
          ? String(executionWorker.status).toUpperCase()
          : "NONE";
        el("execution-worker-status").className = `value ${statusClass(executionWorker ? executionWorker.status : "degraded")}`;
        el("execution-worker-detail").textContent = executionWorker
          ? `${executionWorker.last_seen_at} | ${executionWorker.message}`
          : "No alerting heartbeat recorded yet.";

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
          const latestRiskReason = item.latest_risk?.reason || null;
          const probBuy  = item.latest_signal?.short_ma != null ? (Number(item.latest_signal.short_ma) * 100).toFixed(1) + "%" : null;
          const probSell = item.latest_signal?.long_ma  != null ? (Number(item.latest_signal.long_ma)  * 100).toFixed(1) + "%" : null;
          const probHold = (probBuy != null && probSell != null)
            ? ((1 - Number(item.latest_signal.short_ma) - Number(item.latest_signal.long_ma)) * 100).toFixed(1) + "%"
            : null;
          const latestOrder = item.latest_order?.status || "none";
          const latestFill = item.latest_fill?.side || "none";
          const latestActivityAt = item.latest_activity_at || "none";
          const latestOrderAt = item.latest_order_at || "none";
          const latestFillAt = item.latest_fill_at || "none";
          const openEntryPrice = item.open_entry_price != null
            ? Number(item.open_entry_price).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            : null;
          const latestClosedTrade = item.latest_closed_trade || null;
          const latestClosedSymbol = latestClosedTrade?.symbol || "none";
          const latestClosedStatus = latestClosedTrade?.status || "none";
          const latestClosedAt = latestClosedTrade?.closed_at || "none";
          const latestClosedPnl = latestClosedTrade
            ? parseFloat(Number(latestClosedTrade.realized_pnl || 0).toFixed(6)).toString()
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
          const commission = Number(item.total_commission || 0).toFixed(4);
          const netPnl = Number(item.net_realized_pnl || 0).toFixed(6);
          const netPnlClass = Number(item.net_realized_pnl || 0) > 0
            ? "ok"
            : Number(item.net_realized_pnl || 0) < 0
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

          const netQty = Number(item.net_position_qty || 0);
          const isLong = netQty > 0;
          const sigClass = latestSignal === "BUY" ? "sig-buy" : latestSignal === "SELL" ? "sig-sell" : "sig-hold";
          const netQtyStr = netQty === 0 ? "0" : parseFloat(netQty.toFixed(8)).toString();
          const positionLabel = isLong
            ? `${netQtyStr} BTC${openEntryPrice != null ? " @ " + openEntryPrice : ""}`
            : "—";
          const currentPrice = item.current_price != null
            ? Number(item.current_price).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            : null;
          const bidPrice = item.bid_price != null
            ? Number(item.bid_price).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            : null;
          const askPrice = item.ask_price != null
            ? Number(item.ask_price).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            : null;
          const spreadValue = (item.bid_price != null && item.ask_price != null)
            ? Number(item.ask_price) - Number(item.bid_price)
            : null;
          const spreadBps = (spreadValue != null && item.current_price != null && Number(item.current_price) > 0)
            ? (spreadValue / Number(item.current_price)) * 10000
            : null;
          const priceSymbol = item.price_symbol || null;
          const unrealizedPnl = (isLong && item.open_entry_price != null && item.current_price != null)
            ? (Number(item.current_price) - Number(item.open_entry_price)) * netQty
            : null;
          const unrealizedPnlStr = unrealizedPnl != null
            ? (unrealizedPnl >= 0 ? "+" : "") + parseFloat(unrealizedPnl.toFixed(4)).toString() + " USDT"
            : null;
          const unrealizedPnlClass = unrealizedPnl == null ? "muted" : unrealizedPnl > 0 ? "ok" : unrealizedPnl < 0 ? "bad" : "muted";

          return `
            <div class="strategy-card clickable ${closedTradesStrategyFilter === item.strategy_name ? "selected" : ""}" data-strategy-name="${item.strategy_name}" role="button" tabindex="0">

              <div class="strategy-card-top">
                <div class="strategy-card-identity">
                  <div class="strategy-rank"># ${index + 1}</div>
                  <div class="strategy-name-row">
                    <strong>${item.strategy_name}</strong>
                    <span class="${enabledClass}">${enabledLabel}</span>
                  </div>
                </div>
                <div class="strategy-card-action-group">
                  ${canPromote ? `<button type="button" class="secondary" data-promote-strategy="${item.strategy_name}" title="Promote priority">↑</button>` : ""}
                  ${canDemote ? `<button type="button" class="secondary" data-demote-strategy="${item.strategy_name}" title="Demote priority">↓</button>` : ""}
                  ${strategyEntry.enabled !== false
                    ? `<button type="button" class="secondary" data-disable-strategy="${item.strategy_name}">Disable</button>`
                    : `<button type="button" class="secondary" data-enable-strategy="${item.strategy_name}">Enable</button>`}
                </div>
              </div>

              <div class="strategy-signal-row">
                <span class="strategy-signal-label">Signal</span>
                <span class="strategy-signal-value ${sigClass}">${latestSignal}</span>
                <span class="strategy-signal-divider">·</span>
                <span class="strategy-signal-label">Risk</span>
                <span class="strategy-signal-value sig-hold">${latestRisk}</span>
                <span class="strategy-signal-divider">·</span>
                <span class="strategy-signal-label">Last Fill</span>
                <span class="strategy-signal-value sig-hold">${latestFill}</span>
              </div>
              ${(latestRiskReason || probBuy != null) ? `
              <div class="strategy-reject-reason" style="${latestRisk !== "REJECTED" ? "background:rgba(255,255,255,0.04);color:var(--muted);border-color:rgba(255,255,255,0.08);" : ""}">
                ${latestRiskReason ? `<span>${latestRiskReason}</span>` : ""}
                ${probBuy != null ? `<span class="strategy-prob-row" style="margin-top:${latestRiskReason ? "6px" : "0"};display:flex;gap:6px;">
                  <span class="strategy-prob-item">HOLD ${probHold}</span>
                  <span class="strategy-prob-item sig-buy">BUY ${probBuy}</span>
                  <span class="strategy-prob-item sig-sell">SELL ${probSell}</span>
                </span>` : ""}
              </div>` : ""}

              <div class="strategy-kpi-row">
                <div class="strategy-kpi">
                  <strong>Gross PnL</strong>
                  <span class="${pnlClass}">${pnl}</span>
                </div>
                <div class="strategy-kpi">
                  <strong>Commission</strong>
                  <span class="bad">-${commission}</span>
                </div>
                <div class="strategy-kpi">
                  <strong>Net PnL</strong>
                  <span class="${netPnlClass}">${netPnl}</span>
                </div>
                <div class="strategy-kpi">
                  <strong>Win Rate</strong>
                  <span>${winRate}</span>
                </div>
                <div class="strategy-kpi">
                  <strong>W / L</strong>
                  <span>${item.winning_trade_count} <span style="color:var(--muted);font-weight:400;font-size:13px;">/</span> ${item.losing_trade_count}</span>
                </div>
                <div class="strategy-kpi">
                  <strong>Trades</strong>
                  <span>${item.filled_order_count}</span>
                </div>
              </div>

              <div class="strategy-info-row">
                <div class="strategy-info-cell">
                  <span class="strategy-info-label">Position</span>
                  <span class="strategy-info-value ${isLong ? "ok" : "muted"}">${isLong ? "LONG  " + positionLabel : "Flat"}</span>
                </div>
                <div class="strategy-info-cell">
                  <span class="strategy-info-label">Bid / Ask</span>
                  <span class="strategy-info-value strategy-price-book">
                    ${bidPrice != null || askPrice != null ? `
                      <span class="strategy-price-book-main">
                        <span class="strategy-price-book-line"><strong>Bid</strong>${bidPrice ?? "—"}</span>
                        <span class="strategy-price-book-line"><strong>Ask</strong>${askPrice ?? "—"}</span>
                      </span>
                      <span class="strategy-price-book-spread">Spread ${spreadValue != null ? spreadValue.toFixed(2) : "—"}${spreadBps != null ? ` (${spreadBps.toFixed(2)} bps)` : ""}</span>
                      <span class="strategy-price-book-mid">Mid ${currentPrice != null ? currentPrice + " USDT" : "—"}${priceSymbol ? ` · ${priceSymbol}` : ""}</span>
                    ` : `${currentPrice != null ? currentPrice + " USDT" : "—"}`}
                  </span>
                </div>
                <div class="strategy-info-cell">
                  <span class="strategy-info-label">Unrealized PnL</span>
                  <span class="strategy-info-value ${unrealizedPnlClass}">${unrealizedPnlStr ?? "—"}</span>
                </div>
              </div>
              <div class="strategy-info-row">
                <div class="strategy-info-cell">
                  <span class="strategy-info-label">Last Trade</span>
                  <span class="strategy-info-value ${latestClosedTrade ? latestClosedPnlClass : "muted"}">
                    ${latestClosedTrade ? latestClosedPnl + " USDT  (" + latestClosedStatus + ")" : "—"}
                  </span>
                </div>
              </div>

              <div class="strategy-card-footer">
                <div class="strategy-footer-left">
                  <div class="strategy-footer-item">
                    <span>Volume</span>
                    <span>${Number(item.filled_qty_total).toFixed(4)} BTC</span>
                  </div>
                  <div class="strategy-footer-item">
                    <span>Last Active</span>
                    <span>${latestActivityAt}</span>
                  </div>
                  ${latestClosedTrade ? `
                  <div class="strategy-footer-item">
                    <span>Closed At</span>
                    <span>${latestClosedAt}</span>
                  </div>` : ""}
                </div>
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
          .filter((event) => event.event_type === "scheduler_control" || event.event_type === "execution_control")
          .filter((event) => {
            if (event.event_type === "execution_control") {
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
          el("scheduler-strategy-pills")?.querySelectorAll(".toggle-pill.selected") || []
        ).map((p) => p.dataset.strategy);
        const selectedSchedulerSymbols = Array.from(
          el("scheduler-symbol-pills")?.querySelectorAll(".toggle-pill.selected") || []
        ).map((p) => p.dataset.symbol);
        const selectedDisabledStrategies = [];
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
        const latestFailedDetail = latestFailedJob?.result?.error_detail || null;
        const latestFailedBit = latestFailedJob
          ? `Latest failed: #${latestFailedJob.id} ${latestFailedJob.job_type} attempts=${latestFailedJob.attempt_count}${latestFailedJob.error_message ? ` error=${latestFailedJob.error_message}` : ""}${latestFailedDetail?.status_code ? ` status=${latestFailedDetail.status_code}` : ""}${latestFailedDetail?.binance_code != null ? ` binance_code=${latestFailedDetail.binance_code}` : ""}${latestFailedDetail?.binance_msg ? ` binance_msg=${latestFailedDetail.binance_msg}` : ""}`
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
              <div class="ops-card-note"><strong>Payload</strong> ${payloadText}${errorText}${job.result?.error_detail ? `<br><strong>Broker Error</strong> ${formatJson(job.result.error_detail)}` : ""}</div>
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

      function renderRecentFills(fills) {
        const board = el("fills-board");
        if (!board) return;
        if (!Array.isArray(fills) || fills.length === 0) {
          board.innerHTML = '<div class="strategy-card">No fills recorded yet.</div>';
          return;
        }
        board.innerHTML = fills.map((fill) => {
          const commission = fill.commission != null
            ? `${Number(fill.commission).toFixed(8)} ${fill.commission_asset || ""}`.trim()
            : "n/a";
          const quoteQty = fill.quote_qty != null ? Number(fill.quote_qty).toFixed(6) : "n/a";
          const transactTime = fill.transact_time
            ? new Date(Number(fill.transact_time)).toISOString().replace("T", " ").slice(0, 19)
            : "n/a";
          const sideClass = fill.side === "BUY" ? "ok" : fill.side === "SELL" ? "bad" : "";
          return `
            <div class="ops-card">
              <div class="ops-card-header">
                <div class="ops-card-title">${fill.symbol} · <span class="${sideClass}">${fill.side}</span> · ${fill.qty}</div>
                <div class="chip">fill #${fill.id}</div>
              </div>
              <div class="ops-card-grid">
                <div><strong>Price</strong>${fill.price}</div>
                <div><strong>Commission</strong>${commission}</div>
                <div><strong>Quote Qty</strong>${quoteQty}</div>
                <div><strong>Exchange Time</strong>${transactTime}</div>
              </div>
              <div class="ops-card-note">order_id=${fill.order_id} | recorded_at=${fill.created_at}</div>
            </div>
          `;
        }).join("");
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
        const [health, positions, orders, fills, strategySummary, closedTrades, pnl, logs, auditEvents, alertStatus, soakReport, soakHistory, soakSummary, strategies, schedulerStrategy, schedulerSymbols, queueSummary, riskConfig, portfolio] = await Promise.all([
          api("/health"),
          api("/positions?limit=10"),
          api("/orders?limit=10"),
          api("/fills?limit=10"),
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
        const schedulerStrategyPills = el("scheduler-strategy-pills");
        if (schedulerStrategyPills && schedulerStrategy?.available_strategies && !_schedulerPillsDirty) {
          const activeSet = new Set(schedulerStrategy.strategy_names || []);
          schedulerStrategyPills.innerHTML = schedulerStrategy.available_strategies
            .map((s) => `<button type="button" class="toggle-pill${activeSet.has(s) ? " selected" : ""}" data-strategy="${s}">${s}</button>`)
            .join("");
          schedulerStrategyPills.querySelectorAll(".toggle-pill").forEach((pill) => {
            pill.addEventListener("click", () => { pill.classList.toggle("selected"); _schedulerPillsDirty = true; });
          });
        }
        const schedulerSymbolPills = el("scheduler-symbol-pills");
        if (schedulerSymbolPills && schedulerSymbols?.available_symbols && !_schedulerPillsDirty) {
          const activeSymbolSet = new Set(schedulerSymbols.symbol_names || []);
          schedulerSymbolPills.innerHTML = schedulerSymbols.available_symbols
            .map((s) => `<button type="button" class="toggle-pill${activeSymbolSet.has(s) ? " selected" : ""}" data-symbol="${s}">${s}</button>`)
            .join("");
          schedulerSymbolPills.querySelectorAll(".toggle-pill").forEach((pill) => {
            pill.addEventListener("click", () => { pill.classList.toggle("selected"); _schedulerPillsDirty = true; });
          });
        }
        const schedulerEffectiveLimitInput = el("scheduler-effective-limit-input");
        if (schedulerEffectiveLimitInput) {
          schedulerEffectiveLimitInput.value = schedulerStrategy?.effective_strategy_limit || "";
        }
        const pipelineSymbolPills = el("pipeline-symbol-pills");
        if (pipelineSymbolPills && schedulerSymbols?.available_symbols) {
          const prevSel = new Set(
            Array.from(pipelineSymbolPills.querySelectorAll(".toggle-pill.selected")).map((p) => p.dataset.symbol)
          );
          const activeSymbols = new Set(schedulerSymbols.symbol_names || []);
          pipelineSymbolPills.innerHTML = schedulerSymbols.available_symbols
            .map((s) => {
              const selected = prevSel.size > 0 ? prevSel.has(s) : activeSymbols.has(s);
              return `<button type="button" class="toggle-pill${selected ? " selected" : ""}" data-symbol="${s}">${s}</button>`;
            })
            .join("");
          pipelineSymbolPills.querySelectorAll(".toggle-pill").forEach((pill) => {
            pill.addEventListener("click", () => pill.classList.toggle("selected"));
          });
        }
        const marketFetchPills = el("market-fetch-symbol-checkboxes");
        if (marketFetchPills && schedulerSymbols?.available_symbols) {
          const prevSelected = new Set(
            Array.from(marketFetchPills.querySelectorAll(".toggle-pill.selected")).map((p) => p.dataset.symbol)
          );
          marketFetchPills.innerHTML = schedulerSymbols.available_symbols
            .map((symbol) => `<button type="button" class="toggle-pill${prevSelected.has(symbol) ? " selected" : ""}" data-symbol="${symbol}">${symbol}</button>`)
            .join("");
          marketFetchPills.querySelectorAll(".toggle-pill").forEach((pill) => {
            pill.addEventListener("click", () => pill.classList.toggle("selected"));
          });
        }
        const marketFetchTimeframePills = el("market-fetch-timeframe-pills");
        if (marketFetchTimeframePills) {
          try {
            const tfData = await api("/scheduler/timeframes");
            const tfOrder = {"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440};
            const supportedTf = (tfData?.supported_timeframes || ["1m","3m","5m","15m","30m","1h","4h","1d"]).slice().sort((a,b) => (tfOrder[a]||0)-(tfOrder[b]||0));
            const activeTf = new Set(tfData?.timeframe_names || ["1m"]);
            const prevTf = new Set(
              Array.from(marketFetchTimeframePills.querySelectorAll(".toggle-pill.selected")).map((p) => p.dataset.tf)
            );
            const selectedTf = prevTf.size > 0 ? prevTf : activeTf;
            marketFetchTimeframePills.innerHTML = supportedTf
              .map((tf) => `<button type="button" class="toggle-pill${selectedTf.has(tf) ? " selected" : ""}" data-tf="${tf}">${tf}</button>`)
              .join("");
            marketFetchTimeframePills.querySelectorAll(".toggle-pill").forEach((pill) => {
              pill.addEventListener("click", () => pill.classList.toggle("selected"));
            });
          } catch (_) {}
        }
        const fsPills = el("market-fs-symbol-pills");
        if (fsPills && schedulerSymbols?.available_symbols) {
          const prevSel = fsPills.querySelector(".toggle-pill.selected")?.dataset.symbol;
          fsPills.innerHTML = schedulerSymbols.available_symbols
            .map((symbol, i) => {
              const sel = prevSel ? symbol === prevSel : i === 0;
              return `<button type="button" class="toggle-pill${sel ? " selected" : ""}" data-symbol="${symbol}">${symbol}</button>`;
            })
            .join("");
          fsPills.querySelectorAll(".toggle-pill").forEach((pill) => {
            pill.addEventListener("click", () => {
              fsPills.querySelectorAll(".toggle-pill").forEach((p) => p.classList.remove("selected"));
              pill.classList.add("selected");
            });
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
        renderRecentFills(fills);
        updateHeartbeats(health);
        el("health-json").textContent = formatJson(health);
        el("positions-json").textContent = formatJson(positions);
        el("orders-json").textContent = formatJson(orders);
        el("fills-json").textContent = formatJson(fills);
        el("pnl-json").textContent = formatJson(pnl);
        el("logs-json").textContent = formatJson(logs);
        el("audit-json").textContent = formatJson(auditEvents);
        el("queue-json").textContent = formatJson(queueSummary);
      }

      function renderPipelineResult(result) {
        const badge = el("pipeline-result-badge");
        const rowsEl = el("pipeline-result-rows");
        const jsonEl = el("pipeline-result-json");
        const resultEl = el("pipeline-result");
        const msgEl = el("pipeline-message");

        // Normalise status — queue_batch returns job-level "completed"
        const status = (result.status || "unknown").toLowerCase();
        const isOk = ["ok", "queued", "partial", "completed"].includes(status);

        if (badge) {
          badge.textContent = status.toUpperCase();
          badge.style.background = isOk ? "var(--ok)" : "var(--danger)";
          badge.style.color = "#fff";
        }

        if (rowsEl) rowsEl.innerHTML = "";

        if (jsonEl) jsonEl.innerHTML = syntaxHighlightJson(result);
        if (resultEl) resultEl.style.display = "block";
        if (msgEl) { msgEl.style.display = "none"; msgEl.textContent = ""; }
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
            const msgEl = el("pipeline-message");
            const resultEl = el("pipeline-result");
            if (resultEl) resultEl.style.display = "none";
            if (msgEl) { msgEl.textContent = "Running…"; msgEl.className = "message"; msgEl.style.display = "block"; }
            try {
              const selectedSymbols = Array.from(el("pipeline-symbol-pills")?.querySelectorAll(".toggle-pill.selected") || []).map((p) => p.dataset.symbol);
              result = await api("/pipeline/run", {
                method: "POST",
                body: JSON.stringify({
                  strategy_name: el("pipeline-strategy-select")?.value || "__DEFAULT_STRATEGY_NAME__",
                  symbol_names: selectedSymbols,
                  orchestration: el("pipeline-orchestration-select")?.value || "direct",
                }),
              });
              el("pipeline-json").textContent = formatJson(result);
              renderPipelineResult(result);
            } catch (err) {
              if (msgEl) { msgEl.textContent = `Error: ${err.message}`; msgEl.className = "message bad"; msgEl.style.display = "block"; }
              if (resultEl) resultEl.style.display = "none";
            }
            await refreshAll();
            return;
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
            _schedulerPillsDirty = false;
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
        if (button.dataset.action.startsWith("ml-")) return;
        if (button.dataset.action.startsWith("training-")) return;
        if (button.dataset.action.startsWith("report-")) return;
        if (button.dataset.action.startsWith("market-")) return;
        if (button.dataset.action.startsWith("retention-")) return;
        if (button.dataset.action.startsWith("candles-")) return;
        if (button.dataset.action === "market-fetch-history-refresh") return;
        button.addEventListener("click", () => runAction(button.dataset.action));
      });

      async function refreshFetchHistory() {
        const board = el("fetch-history-board");
        if (!board) return;
        try {
          const entries = await api("/market-data/fetch/history?limit=20");
          if (!entries || !entries.length) {
            board.innerHTML = '<span style="color:var(--muted);font-size:13px">No fetch history yet.</span>';
            return;
          }
          const modeColor = { incremental: "var(--ok)", seed: "var(--accent)", backfill: "var(--warn)" };
          board.innerHTML = entries.map((e) => {
            const dt = new Date(e.fetched_at);
            const timeStr = dt.toISOString().replace("T", " ").slice(0, 19);
            const symbols = (e.symbol_names || []).join(", ");
            const tfs = (e.timeframes || []).join(", ");
            const modes = [...new Set((e.symbol_results || []).map((s) => s.mode).filter(Boolean))];
            const modeChips = modes.map((m) => `<span class="chip" style="font-size:11px;background:${modeColor[m] || "var(--muted)"}20;color:${modeColor[m] || "var(--muted)"}">${m}</span>`).join(" ");
            const isNew = e.saved_klines > 0;
            return `<div class="fetch-result-row" style="align-items:flex-start;gap:8px">
              <div style="flex:1;min-width:0">
                <span style="font-size:12px;color:var(--muted)">${timeStr}</span>
                <span style="margin-left:8px;font-size:12px">${symbols}</span>
                <span style="margin-left:6px;font-size:11px;color:var(--muted)">${tfs}</span>
                <span style="margin-left:6px">${modeChips}</span>
              </div>
              <span class="fetch-result-count ${isNew ? "new" : "none"}" style="white-space:nowrap">${isNew ? "+" + e.saved_klines : "Up to date"}</span>
            </div>`;
          }).join("");
        } catch (e) {
          if (board) board.innerHTML = `<span style="color:var(--bad)">${e}</span>`;
        }
      }

      document.addEventListener("click", (e) => {
        if (e.target.dataset?.action === "market-fetch-history-refresh") refreshFetchHistory();
      });

      el("issue-strip")?.addEventListener("click", (event) => {
        const button = event.target.closest("[data-action]");
        if (!button) return;
        runAction(button.dataset.action);
      });

      document.querySelectorAll("[data-refresh]").forEach((button) => {
        button.addEventListener("click", async () => {
          const orig = button.textContent;
          button.disabled = true;
          button.textContent = "Refreshing...";
          try {
            await refreshAll();
          } finally {
            button.disabled = false;
            button.textContent = orig;
          }
        });
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
      refreshMarketStatus();
      refreshFetchHistory();
      refreshPPOJobs().catch((error) => {
        const msg = el("ppo-train-message");
        if (msg) {
          msg.textContent = `Failed to load PPO jobs: ${error.message}`;
          msg.className = "message bad";
        }
      });
      refreshExecutionReport().then((report) => {
        const msg = el("report-message");
        if (msg) {
          msg.textContent = `Loaded ${report.summary?.symbol ?? "n/a"} report for the last ${report.summary?.days ?? "n/a"} days.`;
          msg.className = "message ok";
        }
      }).catch((error) => {
        const msg = el("report-message");
        if (msg) {
          msg.textContent = `Failed to load report: ${error.message}`;
          msg.className = "message bad";
        }
      });
      (function() {
        const d = new Date();
        d.setDate(d.getDate() - 7);
        const input = el("market-fetch-start-date");
        if (input) input.value = d.toISOString().slice(0, 10);
      })();

      // ---- Tab switching ----
      document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          const tabId = btn.dataset.tab;
          document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
          document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
          btn.classList.add("active");
          document.getElementById("tab-" + tabId).classList.add("active");
          if (tabId === "training") {
            refreshPPOJobs(ppoSelectedJobId).catch((error) => {
              const msg = el("ppo-train-message");
              if (msg) {
                msg.textContent = `Failed to load PPO jobs: ${error.message}`;
                msg.className = "message bad";
              }
            });
          }
          if (tabId === "reports") {
            refreshExecutionReport().then((report) => {
              const msg = el("report-message");
              if (msg) {
                msg.textContent = `Loaded ${report.summary?.symbol ?? "n/a"} report for the last ${report.summary?.days ?? "n/a"} days.`;
                msg.className = "message ok";
              }
            }).catch((error) => {
              const msg = el("report-message");
              if (msg) {
                msg.textContent = `Failed to load report: ${error.message}`;
                msg.className = "message bad";
              }
            });
          }
        });
      });

      document.addEventListener("click", (e) => {
        const btn = e.target.closest(".result-tab");
        if (!btn) return;
        const targetId = btn.dataset.target;
        const container = btn.closest("[id$='-result']");
        if (!container) return;
        container.querySelectorAll(".result-tab").forEach((b) => b.classList.remove("active"));
        container.querySelectorAll("[id]").forEach((panel) => {
          if (panel.classList.contains("result-tab") || panel.closest(".result-tabs")) return;
          panel.style.display = panel.id === targetId ? "" : "none";
        });
        btn.classList.add("active");
      });


      document.addEventListener("click", (e) => {
        const btn = e.target.closest(".date-preset-btn");
        if (!btn) return;
        const days = parseInt(btn.dataset.days);
        const d = new Date();
        d.setDate(d.getDate() - days);
        const iso = d.toISOString().slice(0, 10);
        const input = el("market-fetch-start-date");
        if (input) input.value = iso;
      });

      // ---- Retention handlers ----
      document.addEventListener("click", (e) => {
        const btn = e.target.closest(".retention-preset-btn");
        if (!btn) return;
        const input = el(btn.dataset.field);
        if (input) input.value = btn.dataset.days;
      });

      document.addEventListener("click", async (e) => {
        const btn = e.target.closest("[data-action='retention-run']");
        if (!btn) return;
        const auditDays = parseInt(el("retention-audit-days")?.value || "90");
        const jobDays = parseInt(el("retention-job-days")?.value || "30");
        const msg = el("retention-message");
        const result = el("retention-result");
        btn.disabled = true; btn.textContent = "Running...";
        if (msg) { msg.style.display = "none"; }
        try {
          const r = await api("/maintenance/retention", {
            method: "POST",
            body: JSON.stringify({ audit_days: auditDays, job_queue_days: jobDays })
          });
          const summary = el("retention-summary");
          if (summary) summary.textContent = `Deleted ${r.audit_events_deleted} audit events, ${r.job_queue_deleted} job queue rows`;
          el("retention-pretty").innerHTML = `
            <div class="fetch-result-row"><span class="symbol-name">Audit Events deleted</span><span class="fetch-result-count new">${r.audit_events_deleted}</span></div>
            <div class="fetch-result-row"><span class="symbol-name">Job Queue deleted</span><span class="fetch-result-count new">${r.job_queue_deleted}</span></div>
            <div class="fetch-result-row"><span class="symbol-name">Audit retention</span><span class="fetch-result-count none">${r.audit_retention_days}d</span></div>
            <div class="fetch-result-row"><span class="symbol-name">Job Queue retention</span><span class="fetch-result-count none">${r.job_queue_retention_days}d</span></div>`;
          if (result) result.style.display = "block";
        } catch (err) {
          if (msg) { msg.textContent = String(err); msg.className = "message bad"; msg.style.display = "block"; }
        } finally {
          btn.disabled = false; btn.textContent = "Run Retention";
        }
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
      function trainingRlSymbol() { return el("training-rl-symbol-input")?.value.trim() || "BTCUSDT"; }
      function trainingRlTimeframe() { return el("training-rl-timeframe-input")?.value.trim() || "1m"; }
      function reportSymbol() { return el("report-symbol-input")?.value.trim() || "BTCUSDT"; }
      function reportStrategy() { return el("report-strategy-input")?.value.trim() || ""; }

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

      function renderExecutionReport(report) {
        const summary = report?.summary || {};
        const kpis = el("report-summary-kpis");
        const grid = el("report-summary-grid");
        const dailyTableBody = document.querySelector("#report-daily-table tbody");
        const failedBoard = el("report-failed-board");
        const closedBoard = el("report-closed-trades-board");
        const fillsBoard = el("report-fills-board");
        const raw = el("report-json");

        if (kpis) {
          const gross = Number(summary.gross_pnl || 0);
          const fees = Number(summary.fees || 0);
          const net = Number(summary.net_pnl || 0);
          const winRate = summary.win_rate == null ? "n/a" : `${(Number(summary.win_rate) * 100).toFixed(1)}%`;
          kpis.innerHTML = `
            <div class="training-kpi-card"><label>Gross PnL</label><div class="value ${gross > 0 ? "ok" : gross < 0 ? "bad" : ""}">${gross.toFixed(6)}</div></div>
            <div class="training-kpi-card"><label>Fees</label><div class="value bad">-${fees.toFixed(6)}</div></div>
            <div class="training-kpi-card"><label>Net PnL</label><div class="value ${net > 0 ? "ok" : net < 0 ? "bad" : ""}">${net.toFixed(6)}</div></div>
            <div class="training-kpi-card"><label>Win Rate</label><div class="value">${winRate}</div></div>`;
        }

        if (grid) {
          const pos = summary.current_position || {};
          grid.innerHTML = `
            <div><strong>Window</strong>${summary.symbol || "n/a"} · strategy=${summary.strategy_name || "all"} · ${summary.days || 0}d</div>
            <div><strong>Activity</strong>${summary.fills || 0} fills · ${summary.closed_trades || 0} closed trades · notional=${Number(summary.notional || 0).toFixed(4)}</div>
            <div><strong>Holding</strong>avg=${summary.avg_hold_minutes == null ? "n/a" : `${Number(summary.avg_hold_minutes).toFixed(2)} min`} · best=${summary.best_trade == null ? "n/a" : Number(summary.best_trade).toFixed(6)} · worst=${summary.worst_trade == null ? "n/a" : Number(summary.worst_trade).toFixed(6)}</div>
            <div><strong>Current Position</strong>${pos.qty != null ? `${pos.qty} @ ${pos.avg_price || 0}` : "n/a"}</div>`;
        }

        if (dailyTableBody) {
          const daily = Array.isArray(report?.daily) ? report.daily : [];
          dailyTableBody.innerHTML = daily.length
            ? daily.map((row) => `<tr>
                <td>${row.trade_date}</td>
                <td class="num">${row.fills}</td>
                <td class="num">${Number(row.notional || 0).toFixed(4)}</td>
                <td class="num">${Number(row.fees || 0).toFixed(4)}</td>
                <td class="num" style="color:${Number(row.gross_pnl || 0) >= 0 ? 'var(--ok)' : 'var(--bad)'}">${Number(row.gross_pnl || 0).toFixed(6)}</td>
                <td class="num" style="color:${Number(row.net_pnl || 0) >= 0 ? 'var(--ok)' : 'var(--bad)'}">${Number(row.net_pnl || 0).toFixed(6)}</td>
              </tr>`).join("")
            : `<tr><td colspan="6" class="table-note">No report data in the selected window.</td></tr>`;
        }

        if (failedBoard) {
          const jobs = Array.isArray(report?.recent_failed_execution_jobs) ? report.recent_failed_execution_jobs : [];
          failedBoard.innerHTML = jobs.length
            ? jobs.map((job) => {
                const detail = job.result?.error_detail || {};
                return `<div class="ops-card">
                  <div class="ops-card-header">
                    <div class="ops-card-title">Execution Job #${job.id}</div>
                    <div class="chip"><span class="bad">${String(job.status).toUpperCase()}</span></div>
                  </div>
                  <div class="ops-card-grid">
                    <div><strong>Created</strong>${job.created_at}</div>
                    <div><strong>Error</strong>${job.error_message || "n/a"}</div>
                    <div><strong>HTTP Status</strong>${detail.status_code ?? "n/a"}</div>
                    <div><strong>Binance</strong>${detail.binance_code ?? "n/a"} ${detail.binance_msg || ""}</div>
                  </div>
                </div>`;
              }).join("")
            : '<div class="strategy-card">No failed execution jobs in the selected window.</div>';
        }

        if (closedBoard) {
          const trades = Array.isArray(report?.recent_closed_trades) ? report.recent_closed_trades : [];
          closedBoard.innerHTML = trades.length
            ? trades.map((trade) => `<div class="ops-card">
                <div class="ops-card-header">
                  <div class="ops-card-title">${trade.strategy_name} · ${trade.symbol}</div>
                  <div class="chip"><span class="${Number(trade.realized_pnl || 0) >= 0 ? "ok" : "bad"}">${Number(trade.realized_pnl || 0).toFixed(6)}</span></div>
                </div>
                <div class="ops-card-grid">
                  <div><strong>Closed At</strong>${trade.closed_at}</div>
                  <div><strong>Hold</strong>${trade.hold_minutes == null ? "n/a" : `${trade.hold_minutes} min`}</div>
                  <div><strong>Entry</strong>${Number(trade.entry_price || 0).toFixed(2)}</div>
                  <div><strong>Exit</strong>${Number(trade.exit_price || 0).toFixed(2)}</div>
                </div>
              </div>`).join("")
            : '<div class="strategy-card">No closed trades in the selected window.</div>';
        }

        if (fillsBoard) {
          const fills = Array.isArray(report?.recent_fills) ? report.recent_fills : [];
          fillsBoard.innerHTML = fills.length
            ? fills.map((fill) => `<div class="ops-card">
                <div class="ops-card-header">
                  <div class="ops-card-title">${fill.symbol} · ${fill.side}</div>
                  <div class="chip">fill #${fill.id}</div>
                </div>
                <div class="ops-card-grid">
                  <div><strong>Recorded</strong>${fill.created_at}</div>
                  <div><strong>Price</strong>${Number(fill.price || 0).toFixed(2)}</div>
                  <div><strong>Commission</strong>${fill.commission == null ? "n/a" : `${Number(fill.commission).toFixed(8)} ${fill.commission_asset || ""}`}</div>
                  <div><strong>Quote Qty</strong>${fill.quote_qty == null ? "n/a" : Number(fill.quote_qty).toFixed(6)}</div>
                </div>
              </div>`).join("")
            : '<div class="strategy-card">No fills in the selected window.</div>';
        }

        if (raw) raw.textContent = formatJson(report);
      }

      async function refreshExecutionReport() {
        const params = new URLSearchParams({
          symbol: reportSymbol(),
          days: String(parseInt(el("report-days-input")?.value || "7")),
          limit: "10",
        });
        const strategyName = reportStrategy();
        if (strategyName) params.set("strategy_name", strategyName);
        const report = await api(`/reports/testnet-execution?${params.toString()}`);
        renderExecutionReport(report);
        return report;
      }

      // ---- PPO Training ----
      let ppoJobsState = [];
      let ppoSelectedJobId = null;
      let ppoPollingTimer = null;

      function ppoStatusTone(status) {
        if (status === "done") return "ok";
        if (status === "failed") return "bad";
        if (status === "running" || status === "pending") return "warn";
        return "";
      }

      function ppoFmt(value, digits = 4) {
        if (value == null || value === "") return "n/a";
        const n = Number(value);
        return Number.isFinite(n) ? (n >= 0 ? "+" : "") + n.toFixed(digits) : String(value);
      }

      function renderPPOSummary(job) {
        if (!job) {
          el("ppo-kpi-status").textContent = "\u2014";
          el("ppo-kpi-verdict").textContent = "\u2014";
          el("ppo-kpi-ppo-ret").textContent = "\u2014";
          el("ppo-kpi-bnh-ret").textContent = "\u2014";
          el("ppo-selected-job-title").textContent = "No job selected";
          el("ppo-selected-job-chip").textContent = "\u2014";
          el("ppo-summary-grid").innerHTML = `
            <div><strong>Dataset</strong>Select or run a training job.</div>
            <div><strong>Walk-forward</strong>Win rate and per-window results appear here.</div>
            <div><strong>Hyperparameters</strong>Steps, fee rate, and SB3 params appear here.</div>
            <div><strong>Model</strong>Candidate path and deploy status appear here.</div>`;
          el("ppo-wf-table-wrap").style.display = "none";
          el("ppo-deploy-wrap").style.display = "none";
          el("ppo-progress-wrap").style.display = "none";
          el("ppo-job-json").style.display = "none";
          return;
        }
        const metrics = job.metrics || {};
        const dataset = job.dataset || {};
        const params  = job.params  || {};
        const model   = job.model   || {};
        const tone    = ppoStatusTone(job.status);
        el("ppo-kpi-status").className  = "value " + tone;
        el("ppo-kpi-status").textContent = String(job.status || "unknown").toUpperCase();
        el("ppo-kpi-verdict").textContent = metrics.verdict || "n/a";
        el("ppo-kpi-ppo-ret").textContent = ppoFmt(metrics.avg_ppo_pct);
        el("ppo-kpi-bnh-ret").textContent = ppoFmt(metrics.avg_bnh_pct);
        el("ppo-selected-job-title").textContent = `Job #${job.id} \u00b7 ${job.symbol}/${job.timeframe}`;
        el("ppo-selected-job-chip").textContent   = String(job.status || "\u2014").toUpperCase();
        el("ppo-summary-grid").innerHTML = `
          <div><strong>Dataset</strong>${dataset.n_train ?? "n/a"} train / ${dataset.n_total ?? "n/a"} total \u00b7 fee=${dataset.fee_rate ?? params.fee_rate ?? "n/a"}</div>
          <div><strong>Walk-forward</strong>Win rate ${metrics.win_rate != null ? (metrics.win_rate * 100).toFixed(0) + "%" : "n/a"} \u00b7 avg edge ${ppoFmt(metrics.avg_edge)}</div>
          <div><strong>Hyperparameters</strong>steps=${params.total_steps ?? "n/a"}, lr=${params.learning_rate ?? "n/a"}, batch=${params.batch_size ?? "n/a"}, gamma=${params.gamma ?? "n/a"}</div>
          <div><strong>Model</strong>${model.model_path ? model.model_path.split("/").pop() : "n/a"} \u00b7 finished=${job.finished_at || "running..."}</div>`;
        const prog = job.progress_json;
        if (job.status === "running" && prog) {
          el("ppo-progress-wrap").style.display = "block";
          el("ppo-progress-bar").style.width    = (prog.pct || 0) + "%";
          el("ppo-progress-label").textContent  = `${prog.pct ?? 0}% (${(prog.step ?? 0).toLocaleString()} / ${(prog.total ?? 0).toLocaleString()} steps)`;
        } else {
          el("ppo-progress-wrap").style.display = "none";
        }
        const wf = Array.isArray(metrics.walk_forward) ? metrics.walk_forward : [];
        if (wf.length > 0) {
          el("ppo-wf-table-wrap").style.display = "block";
          el("ppo-wf-table").querySelector("tbody").innerHTML = wf.map((r) => {
            const flag = r.beats_bnh ? '<span class="ok">\u2713</span>' : '<span class="bad">\u2717</span>';
            const edge = ((r.ppo?.log_ret ?? 0) - (r.bnh?.log_ret ?? 0)).toFixed(5);
            return `<tr>
              <td>${r.window}</td>
              <td class="num">${ppoFmt(r.ppo?.pct_ret)}</td>
              <td class="num">${ppoFmt(r.bnh?.pct_ret)}</td>
              <td class="num">${edge}</td>
              <td class="num">${r.ppo?.n_trades ?? "\u2014"}</td>
              <td>${flag}</td>
            </tr>`;
          }).join("");
        } else {
          el("ppo-wf-table-wrap").style.display = "none";
        }
        el("ppo-deploy-wrap").style.display = job.status === "done" ? "block" : "none";
        const deployBtn = el("ppo-deploy-btn");
        if (deployBtn) deployBtn.dataset.jobId = job.id;
        const rawEl = el("ppo-job-json");
        if (rawEl) { rawEl.textContent = JSON.stringify(job, null, 2); rawEl.style.display = "block"; }
        if (job.error) {
          const msg = el("ppo-train-message");
          if (msg) { msg.textContent = job.error; msg.className = "message bad"; }
        }
      }

      function renderPPOJobs(payload, selectedId = null) {
        const board = el("ppo-jobs-board");
        if (!board) return;
        const jobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
        ppoJobsState = jobs;
        if (!jobs.length) {
          board.innerHTML = `<div class="ops-card"><div class="ops-card-title">No PPO jobs yet.</div><div class="ops-card-note">Click "Start PPO Training" to create the first job.</div></div>`;
          renderPPOSummary(null);
          return;
        }
        const resolvedId = selectedId ?? ppoSelectedJobId ?? jobs[0].id;
        ppoSelectedJobId = resolvedId;
        board.innerHTML = jobs.map((job) => {
          const metrics = job.metrics || {};
          const params  = job.params  || {};
          const tone    = ppoStatusTone(job.status);
          const sel     = Number(job.id) === Number(resolvedId);
          const verdictTone = metrics.verdict === "PASS" ? "ok" : metrics.verdict === "FAIL" ? "bad" : "warn";
          return `
            <div class="ops-card training-job-row${sel ? " selected" : ""}" data-ppo-job-id="${job.id}">
              <div class="ops-card-header">
                <div class="ops-card-title">Job #${job.id} \u00b7 ${job.symbol}/${job.timeframe} \u00b7 PPO</div>
                <div style="display:flex;gap:8px;align-items:center">
                  <div class="chip"><span class="${tone}">${String(job.status).toUpperCase()}</span></div>
                  <button class="secondary" style="padding:2px 10px;font-size:11px" data-action="ppo-job-delete" data-job-id="${job.id}">Delete</button>
                </div>
              </div>
              <div class="training-job-meta">
                <span class="chip">steps=${(params.total_steps ?? "?").toLocaleString()}</span>
                <span class="chip">fee=${params.fee_rate ?? "?"}</span>
                <span class="chip"><span class="${verdictTone}">verdict=${metrics.verdict || "\u2014"}</span></span>
                <span class="chip">win=${metrics.win_rate != null ? (metrics.win_rate * 100).toFixed(0) + "%" : "\u2014"}</span>
              </div>
              <div class="ops-card-grid">
                <div><strong>PPO avg ret</strong>${ppoFmt(metrics.avg_ppo_pct)}</div>
                <div><strong>B&H avg ret</strong>${ppoFmt(metrics.avg_bnh_pct)}</div>
                <div><strong>Avg edge</strong>${ppoFmt(metrics.avg_edge)}</div>
                <div><strong>Finished</strong>${job.finished_at || job.created_at || "\u2014"}</div>
              </div>
              ${job.error ? `<div class="ops-card-note" style="color:var(--bad)">${job.error}</div>` : ""}
            </div>`;
        }).join("");
        renderPPOSummary(jobs.find((j) => Number(j.id) === Number(resolvedId)) || jobs[0]);
      }

      async function refreshPPOJobs(selectedId = null) {
        const payload = await api(`/training/jobs?limit=20`);
        const ppoOnly = { ...payload, jobs: (payload?.jobs || []).filter((j) => j.params?.job_type === "ppo" || j.feature_set === "ppo") };
        renderPPOJobs(ppoOnly, selectedId);
        return ppoOnly;
      }

      function startPPOPolling(jobId) {
        if (ppoPollingTimer) clearInterval(ppoPollingTimer);
        ppoPollingTimer = setInterval(async () => {
          try {
            const job = await api(`/training/jobs/${jobId}`);
            renderPPOSummary(job);
            const idx = ppoJobsState.findIndex((j) => j.id === job.id);
            if (idx >= 0) ppoJobsState[idx] = job;
            renderPPOJobs({ jobs: ppoJobsState }, ppoSelectedJobId);
            if (job.status !== "running" && job.status !== "pending") {
              clearInterval(ppoPollingTimer);
              ppoPollingTimer = null;
            }
          } catch (_) {}
        }, 5000);
      }

      document.addEventListener("click", async (event) => {
        const action = event.target.dataset?.action;
        if (!action?.startsWith("ppo-")) return;

        if (action === "ppo-jobs-refresh") {
          try {
            const payload = await refreshPPOJobs(ppoSelectedJobId);
            const msg = el("ppo-train-message");
            if (msg) { msg.textContent = `Loaded ${payload.jobs?.length ?? 0} PPO jobs.`; msg.className = "message ok"; }
          } catch (e) {
            const msg = el("ppo-train-message");
            if (msg) { msg.textContent = String(e); msg.className = "message bad"; }
          }
          return;
        }

        if (action === "ppo-train-run") {
          const btn = event.target.closest("[data-action='ppo-train-run']");
          const msg = el("ppo-train-message");
          if (btn) { btn.disabled = true; btn.textContent = "Submitting..."; }
          if (msg) { msg.textContent = "Starting PPO training job in background..."; msg.className = "message"; }
          try {
            const body = {
              symbol:        el("ppo-symbol-input")?.value.trim() || "BTCUSDT",
              timeframe:     el("ppo-timeframe-input")?.value.trim() || "1m",
              total_steps:   parseInt(el("ppo-steps-input")?.value || "1000000"),
              eval_windows:  parseInt(el("ppo-eval-windows-input")?.value || "8"),
              fee_rate:      parseFloat(el("ppo-fee-rate-input")?.value || "0.001"),
              seed:          parseInt(el("ppo-seed-input")?.value || "42"),
              learning_rate: parseFloat(el("ppo-lr-input")?.value || "0.0003"),
              n_steps:       parseInt(el("ppo-n-steps-input")?.value || "2048"),
              batch_size:    parseInt(el("ppo-batch-size-input")?.value || "256"),
              n_epochs:      parseInt(el("ppo-n-epochs-input")?.value || "10"),
              gamma:         parseFloat(el("ppo-gamma-input")?.value || "0.99"),
            };
            const result = await api("/training/ppo-jobs", { method: "POST", body: JSON.stringify(body) });
            ppoSelectedJobId = result.id;
            if (msg) {
              msg.innerHTML = `PPO job #${result.id} started \u2014 training in background, auto-refreshes every 5s. `
                + `TensorBoard: <a href="${result.tensorboard_url || 'http://localhost:6006'}" target="_blank" style="color:var(--accent)">${result.tensorboard_url || 'http://localhost:6006'}</a>`;
              msg.className = "message ok";
            }
            await refreshPPOJobs(result.id);
            startPPOPolling(result.id);
          } catch (e) {
            if (msg) { msg.textContent = String(e); msg.className = "message bad"; }
          } finally {
            if (btn) { btn.disabled = false; btn.textContent = "Start PPO Training"; }
          }
          return;
        }

        if (action === "ppo-job-delete") {
          const jobId = event.target.dataset?.jobId;
          if (!jobId) return;
          if (!await showConfirm("Delete Training Job", `Delete Job #${jobId}? This cannot be undone.`)) return;
          try {
            await api(`/training/jobs/${jobId}`, { method: "DELETE" });
            if (Number(jobId) === ppoSelectedJobId) { ppoSelectedJobId = null; renderPPOSummary(null); }
            await refreshPPOJobs(ppoSelectedJobId);
          } catch (e) {
            const msg = el("ppo-train-message");
            if (msg) { msg.textContent = String(e); msg.className = "message bad"; }
          }
          return;
        }

        if (action === "ppo-deploy") {
          const jobId = event.target.dataset?.jobId || ppoSelectedJobId;
          const deployMsg = el("ppo-deploy-message");
          const btn = event.target;
          if (!jobId) return;
          btn.disabled = true;
          if (deployMsg) { deployMsg.textContent = "Deploying..."; deployMsg.className = "message"; }
          try {
            const result = await api(`/training/ppo-jobs/${jobId}/deploy`, { method: "POST" });
            if (deployMsg) {
              deployMsg.textContent = `Deployed \u2192 ${result.active_path?.split("/").pop() ?? "active model updated"}`;
              deployMsg.className = "message ok";
            }
          } catch (e) {
            if (deployMsg) { deployMsg.textContent = String(e); deployMsg.className = "message bad"; }
          } finally {
            btn.disabled = false;
          }
          return;
        }
      });

      el("ppo-jobs-board")?.addEventListener("click", (event) => {
        if (event.target.closest("[data-action='ppo-job-delete']")) return;
        const card = event.target.closest("[data-ppo-job-id]");
        if (!card) return;
        ppoSelectedJobId = Number(card.dataset.ppoJobId);
        renderPPOJobs({ jobs: ppoJobsState }, ppoSelectedJobId);
      });

      // ---- Market Data actions ----
      async function refreshMarketStatus() {
        try {
          const rows = await api("/candles/status");
          const board = el("market-status-board");
          if (!board) return;
          if (!rows || rows.length === 0) {
            board.innerHTML = '<span style="color:var(--muted);font-size:13px;">No candle data found.</span>';
            return;
          }

          // Card view
          board.innerHTML = rows.map((r) => {
            const staleMin = Math.round(r.stale_seconds / 60);
            const isFresh = staleMin < 5;
            const isWarn = staleMin < 30;
            const dotColor = isFresh ? "var(--ok)" : isWarn ? "var(--warn)" : "var(--bad)";
            const staleText = staleMin < 60 ? `${staleMin}m ago` : `${Math.round(staleMin/60)}h ago`;
            const staleColor = isFresh ? "var(--ok)" : isWarn ? "var(--warn)" : "var(--bad)";
            const hasGaps = r.has_gaps;
            const cov = r.coverage_pct ?? 100;
            const covColor = cov >= 99 ? "var(--ok)" : cov >= 90 ? "var(--warn)" : "var(--bad)";
            return `
              <div class="status-symbol-row">
                <div class="status-symbol-left">
                  <span class="status-dot" style="background:${dotColor}"></span>
                  <div>
                    <span class="status-symbol-name">${r.symbol}</span>
                    <span class="status-badge">${r.timeframe}</span>
                  </div>
                </div>
                <div class="status-symbol-stats">
                  <div class="status-stat">
                    <div class="status-stat-label">Candles</div>
                    <div class="status-stat-value">${r.count.toLocaleString()}</div>
                  </div>
                  <div class="status-stat">
                    <div class="status-stat-label">Coverage</div>
                    <div class="status-stat-value" style="color:${covColor}">${cov}%</div>
                  </div>
                  <div class="status-stat">
                    <div class="status-stat-label">Last Update</div>
                    <div class="status-stat-value" style="color:${staleColor}">${staleText}</div>
                  </div>
                  <div class="status-stat">
                    <div class="status-stat-label">Gaps</div>
                    <div class="status-stat-value" style="color:${hasGaps ? "var(--warn)" : "var(--ok)"}">
                      ${hasGaps ? `~${r.gap_count_estimate}` : "None"}
                    </div>
                  </div>
                </div>
              </div>`;
          }).join("");

          // Coverage matrix
          const matrix = el("market-coverage-matrix");
          if (matrix) {
            const symbols = [...new Set(rows.map((r) => r.symbol))];
            const tfOrder = {"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440};
            const timeframes = [...new Set(rows.map((r) => r.timeframe))].sort((a,b) => (tfOrder[a]||0)-(tfOrder[b]||0));
            const lookup = {};
            rows.forEach((r) => { lookup[`${r.symbol}|${r.timeframe}`] = r; });
            const covColor = (pct) => pct == null ? "#333" : pct >= 99 ? "var(--ok)" : pct >= 90 ? "var(--warn)" : "var(--bad)";
            const header = `<tr><th style="padding:6px 10px;text-align:left;font-size:12px;color:var(--muted)">Symbol</th>${timeframes.map((tf) => `<th style="padding:6px 10px;font-size:12px;color:var(--muted);text-align:center">${tf}</th>`).join("")}</tr>`;
            const bodyRows = symbols.map((sym) => {
              const cells = timeframes.map((tf) => {
                const d = lookup[`${sym}|${tf}`];
                if (!d) return `<td style="padding:6px 10px;text-align:center;color:#444;font-size:12px">—</td>`;
                const pct = d.coverage_pct ?? 100;
                const staleMin = Math.round(d.stale_seconds / 60);
                const dotColor = staleMin < 5 ? "var(--ok)" : staleMin < 30 ? "var(--warn)" : "var(--bad)";
                return `<td style="padding:6px 10px;text-align:center;font-size:12px">
                  <span style="color:${covColor(pct)};font-weight:600">${pct}%</span>
                  <span style="display:block;font-size:10px;color:${dotColor}">${staleMin < 60 ? staleMin + "m" : Math.round(staleMin/60) + "h"} ago</span>
                </td>`;
              }).join("");
              return `<tr><td style="padding:6px 10px;font-size:13px;font-weight:600;white-space:nowrap">${sym}</td>${cells}</tr>`;
            }).join("");
            matrix.innerHTML = `<table style="border-collapse:collapse;width:100%"><thead>${header}</thead><tbody>${bodyRows}</tbody></table>`;
          }

          // Toggle button
          const toggleBtn = el("market-status-view-toggle");
          if (toggleBtn) {
            toggleBtn.onclick = () => {
              const isCards = toggleBtn.dataset.view === "cards";
              toggleBtn.dataset.view = isCards ? "matrix" : "cards";
              toggleBtn.textContent = isCards ? "Card View" : "Coverage Matrix";
              if (board) board.style.display = isCards ? "none" : "";
              if (matrix) matrix.style.display = isCards ? "" : "none";
            };
          }
        } catch (e) { if (el("market-status-board")) el("market-status-board").innerHTML = `<span style="color:var(--bad)">${e}</span>`; }
      }

      document.addEventListener("click", async (event) => {
        const action = event.target.dataset?.action;
        if (!action?.startsWith("market-")) return;

        if (action === "market-status-refresh") {
          await refreshMarketStatus();
        }

        if (action === "market-fetch") {
          const selected = Array.from(el("market-fetch-symbol-checkboxes")?.querySelectorAll(".toggle-pill.selected") || []).map((p) => p.dataset.symbol);
          const symbols = selected.length > 0 ? selected : null;
          const selectedTf = Array.from(el("market-fetch-timeframe-pills")?.querySelectorAll(".toggle-pill.selected") || []).map((p) => p.dataset.tf);
          const timeframes = selectedTf.length > 0 ? selectedTf : null;
          const startDate = el("market-fetch-start-date")?.value || null;
          const body = startDate ? { symbols, timeframes, start_date: startDate } : { symbols, timeframes, limit: 100 };
          try {
            const r = await api("/market-data/fetch", { method: "POST", body: JSON.stringify(body) });
            const result = el("market-fetch-result");
            const msg = el("market-fetch-message");
            if (msg) { msg.textContent = ""; }
            if (result) {
              const summary = el("market-fetch-summary");
              if (summary) summary.textContent = `${r.saved_klines ?? 0} new candles saved`;
              const modeLabel = { incremental: "incremental", seed: "seed", backfill: "backfill" };
              const rows = (r.symbol_results || []).map((s) => {
                const isNew = s.saved_klines > 0;
                const mode = s.mode ? `<span class="chip" style="margin-left:6px;font-size:11px">${modeLabel[s.mode] || s.mode}</span>` : "";
                const tf = s.timeframe ? `<span style="color:var(--muted);font-size:12px;margin-left:4px">${s.timeframe}</span>` : "";
                return `<div class="fetch-result-row">
                  <span class="symbol-name">${s.symbol}${tf}${mode}</span>
                  <span class="fetch-result-count ${isNew ? "new" : "none"}">${isNew ? "+" + s.saved_klines : "Up to date"}</span>
                </div>`;
              }).join("");
              el("market-fetch-pretty").innerHTML = rows;
              el("market-fetch-raw").textContent = JSON.stringify(r, null, 2);
              result.style.display = "block";

              // Fetch last 10 candles per symbol and render as full-width table
              const fetchedSymbols = [...new Set((r.symbol_results || []).map((s) => s.symbol))];
              const activeTimeframeFilter = new Set(timeframes || []);
              const candlesContainer = el("market-fetch-candles");
              const candlesPanel = el("market-candles-panel");
              if (candlesContainer && fetchedSymbols.length) {
                candlesContainer.innerHTML = "";
                const cols = ["open_time", "timeframe", "open", "high", "low", "close", "volume", "quote_asset_volume", "number_of_trades", "taker_buy_base_volume", "taker_buy_quote_volume"];
                const colLabels = { quote_asset_volume: "quote_vol", number_of_trades: "trades", taker_buy_base_volume: "taker_base", taker_buy_quote_volume: "taker_quote" };
                for (const sym of fetchedSymbols) {
                  const tfQueryStr = [...activeTimeframeFilter].map((tf) => `timeframe=${encodeURIComponent(tf)}`).join("&");
                  const tfSuffix = tfQueryStr ? `&${tfQueryStr}` : "";
                  const candles = await api(`/candles?symbol=${encodeURIComponent(sym)}&limit=10${tfSuffix}`);
                  if (!candles || !candles.length) continue;
                  const headers = cols.map((c) => `<th>${colLabels[c] || c}</th>`).join("");
                  const dataRows = candles.map((row) => {
                    const openVal = parseFloat(row["open"]);
                    const closeVal = parseFloat(row["close"]);
                    const isBull = closeVal > openVal;
                    const isBear = closeVal < openVal;
                    const rowClass = isBull ? "bull-row" : isBear ? "bear-row" : "";
                    const cells = cols.map((c) => {
                      let v = row[c];
                      if (c === "open_time" || c === "close_time") {
                        const d = new Date(typeof v === "number" ? v : parseInt(v));
                        v = isNaN(d) ? v : d.toISOString().replace("T", " ").slice(0, 19);
                      } else if (typeof v === "number") {
                        v = v % 1 === 0 ? v : parseFloat(v.toFixed(6));
                      }
                      if (c === "close") {
                        const cls = isBull ? "cell-bull" : isBear ? "cell-bear" : "";
                        const arrow = isBull ? `<span class="candles-dir">▲</span>` : isBear ? `<span class="candles-dir">▼</span>` : "";
                        return `<td class="${cls}">${v ?? "—"}${arrow}</td>`;
                      }
                      return `<td>${v ?? "—"}</td>`;
                    }).join("");
                    return `<tr class="${rowClass}">${cells}</tr>`;
                  }).join("");
                  candlesContainer.innerHTML += `
                    <div class="candles-table-wrap">
                      <div class="candles-table-header">
                        <span class="candles-symbol-badge">${sym}</span>
                        <span class="candles-header-meta">Last ${candles.length} candles</span>
                      </div>
                      <table class="candles-table">
                        <thead><tr>${headers}</tr></thead>
                        <tbody>${dataRows}</tbody>
                      </table>
                    </div>`;
                }
                if (candlesPanel) candlesPanel.style.display = candlesContainer.innerHTML ? "block" : "none";
              }
            }
            refreshMarketStatus();
          } catch (e) { const msg = el("market-fetch-message"); if (msg) { msg.textContent = String(e); msg.className = "message bad"; msg.style.display = "block"; } }
        }

        if (action === "market-fs-materialize") {
          const sym = el("market-fs-symbol-pills")?.querySelector(".toggle-pill.selected")?.dataset.symbol || "BTCUSDT";
          const tf = el("market-fs-timeframe-input")?.value.trim() || "1m";
          const btn = event.target.closest("[data-action]");
          const msg = el("market-fs-message");
          if (btn) { btn.disabled = true; btn.textContent = "Computing..."; }
          if (msg) { msg.style.display = "none"; msg.textContent = ""; }
          try {
            const r = await api("/features/materialize", { method: "POST", body: JSON.stringify({ symbol: sym, timeframe: tf, days: 30 }) });
            const count = r.vectors_upserted ?? r.upserted ?? r.count ?? 0;
            const result = el("market-fs-result");
            const summary = el("market-fs-summary");
            if (summary) summary.textContent = `${count} feature vectors upserted`;
            el("market-fs-pretty").innerHTML = `
              <div class="fetch-result-row"><span class="symbol-name">Symbol</span><span class="fetch-result-count new">${r.symbol ?? sym}</span></div>
              <div class="fetch-result-row"><span class="symbol-name">Timeframe</span><span class="fetch-result-count new">${r.timeframe ?? tf}</span></div>
              <div class="fetch-result-row"><span class="symbol-name">Candles processed</span><span class="fetch-result-count new">${r.candle_count ?? "—"}</span></div>
              <div class="fetch-result-row"><span class="symbol-name">Vectors upserted</span><span class="fetch-result-count new">${count}</span></div>
              <div class="fetch-result-row"><span class="symbol-name">Feature set</span><span class="fetch-result-count none">${r.feature_set ?? "—"}</span></div>`;
            el("market-fs-raw").textContent = JSON.stringify(r, null, 2);
            if (result) result.style.display = "block";
          } catch (e) {
            if (msg) { msg.textContent = String(e); msg.className = "message bad"; msg.style.display = "block"; }
          } finally {
            if (btn) { btn.disabled = false; btn.textContent = "Materialize Features"; }
          }
        }

        if (action === "market-fs-latest") {
          const sym = el("market-fs-symbol-pills")?.querySelector(".toggle-pill.selected")?.dataset.symbol || "BTCUSDT";
          const tf = el("market-fs-timeframe-input")?.value.trim() || "1m";
          const msg = el("market-fs-message");
          if (msg) { msg.style.display = "none"; msg.textContent = ""; }
          try {
            const r = await api(`/features/${sym}/latest?timeframe=${tf}`);
            const result = el("market-fs-result");
            const summary = el("market-fs-summary");
            if (summary) summary.textContent = `Latest feature vector — ${sym} ${tf}`;
            const fields = Object.entries(r)
              .filter(([k]) => !["id","created_at","feature_set","symbol","timeframe","open_time"].includes(k))
              .map(([k, v]) => `<div class="fetch-result-row"><span class="symbol-name">${k}</span><span class="fetch-result-count none">${typeof v === "number" ? v.toFixed(6) : v}</span></div>`)
              .join("");
            el("market-fs-pretty").innerHTML = fields;
            el("market-fs-raw").textContent = JSON.stringify(r, null, 2);
            if (result) result.style.display = "block";
          } catch (e) { if (msg) { msg.textContent = String(e); msg.className = "message bad"; msg.style.display = "block"; } }
        }
      });

      document.addEventListener("click", async (event) => {
        const action = event.target.dataset?.action;
        if (action !== "candles-quality") return;
        const btn = event.target;
        const result = el("candles-quality-result");
        const msg = el("candles-quality-message");
        btn.disabled = true;
        btn.textContent = "Running…";
        if (msg) { msg.style.display = "none"; }
        try {
          const r = await api("/validation/candles/quality");
          const statusColors = { ok: "var(--ok)", warning: "var(--warn)", error: "var(--bad)" };
          const badge = el("candles-quality-badge");
          if (badge) {
            badge.textContent = r.status.toUpperCase();
            badge.style.background = statusColors[r.status] || "var(--muted)";
            badge.style.color = "#000";
            badge.style.padding = "2px 10px";
            badge.style.borderRadius = "4px";
            badge.style.fontWeight = "bold";
            badge.style.fontSize = "12px";
          }
          const dur = el("candles-quality-duration");
          if (dur) dur.textContent = `checked in ${r.duration_seconds}s`;

          const msgs = el("candles-quality-messages");
          if (msgs) {
            const items = [
              ...(r.errors || []).map((e) => `<div style="color:var(--bad)">✗ ${e}</div>`),
              ...(r.warnings || []).map((w) => `<div style="color:var(--warn)">⚠ ${w}</div>`),
            ];
            msgs.innerHTML = items.length ? items.join("") : `<div style="color:var(--ok)">✓ All checks passed</div>`;
          }

          const sections = el("candles-quality-sections");
          if (sections) {
            const renderSection = (title, items, renderRow) => {
              if (!items || !items.length) return "";
              return `<div style="margin-bottom:16px">
                <div style="font-size:12px;font-weight:700;text-transform:uppercase;color:var(--muted);margin-bottom:8px">${title}</div>
                <table style="width:100%;border-collapse:collapse;font-size:13px">
                  ${items.map(renderRow).join("")}
                </table>
              </div>`;
            };
            const td = (v, style="") => `<td style="padding:4px 8px;border-bottom:1px solid var(--border);${style}">${v ?? "—"}</td>`;
            const tr = (cells) => `<tr>${cells}</tr>`;

            const dupSection = r.duplicates?.count > 0
              ? renderSection("Duplicates", r.duplicates.examples, (d) =>
                  tr(td(d.symbol) + td(d.timeframe) + td(d.open_time) + td(`×${d.count}`, "color:var(--bad)")))
              : "";

            const intSection = r.integrity?.count > 0
              ? renderSection("OHLCV Violations", r.integrity.examples, (d) =>
                  tr(td(d.symbol) + td(d.timeframe) + td(d.open_time) + td(`O:${d.open} H:${d.high} L:${d.low} C:${d.close}`)))
              : "";

            const gapSection = r.gaps?.details?.length > 0
              ? renderSection("Gaps", r.gaps.details, (g) =>
                  tr(td(g.symbol) + td(g.timeframe) + td(`${g.gap_count} gap(s)`, "color:var(--warn)") + td(`${g.missing_candles} missing`) + td(`${g.total_candles} total`)))
              : "";

            const spikeSection = r.price_spikes?.details?.length > 0
              ? renderSection("Price Spikes (>" + r.price_spikes.threshold_pct + "%)", r.price_spikes.details, (s) =>
                  tr(td(s.symbol) + td(s.timeframe) + td(`${s.spike_count} spike(s)`, "color:var(--warn)") +
                     td(s.examples.map((x) => `${x.change_pct}%`).join(", "))))
              : "";

            sections.innerHTML = dupSection + intSection + gapSection + spikeSection
              || `<div style="color:var(--muted);font-size:13px">No issues found.</div>`;
          }

          if (result) result.style.display = "block";
        } catch (e) {
          if (msg) { msg.textContent = String(e); msg.className = "message bad"; msg.style.display = "block"; }
        } finally {
          btn.disabled = false;
          btn.textContent = "Run Check";
        }
      });
      // Custom confirm dialog
      let _confirmResolve = null;
      function showConfirm(title, message) {
        return new Promise((resolve) => {
          _confirmResolve = resolve;
          el("confirm-title").textContent   = title;
          el("confirm-message").textContent = message;
          el("confirm-overlay").classList.add("active");
        });
      }
      el("confirm-ok")?.addEventListener("click", () => {
        el("confirm-overlay").classList.remove("active");
        if (_confirmResolve) { _confirmResolve(true); _confirmResolve = null; }
      });
      el("confirm-cancel")?.addEventListener("click", () => {
        el("confirm-overlay").classList.remove("active");
        if (_confirmResolve) { _confirmResolve(false); _confirmResolve = null; }
      });
    </script>

    <div class="confirm-overlay" id="confirm-overlay">
      <div class="confirm-box">
        <h3 id="confirm-title">Confirm</h3>
        <p id="confirm-message"></p>
        <div class="button-row">
          <button id="confirm-cancel" class="secondary">Cancel</button>
          <button id="confirm-ok">OK</button>
        </div>
      </div>
    </div>
  </body>
</html>
"""
    return (
        html.replace("__STRATEGY_OPTIONS__", strategy_options)
        .replace("__CLOSED_TRADE_STRATEGY_OPTIONS__", closed_trade_strategy_options)
        .replace("__PIPELINE_ORCHESTRATION_OPTIONS__", pipeline_orchestration_options)
        .replace("__DEFAULT_STRATEGY_NAME__", DEFAULT_STRATEGY_NAME)
    )
