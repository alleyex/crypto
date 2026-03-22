# 加密交易系統開發工作表 Checklist

這份工作表用來追蹤從 MVP 到 Growth、Production、AI/RL 的建置進度。

## Stage 1 MVP

- [x] 定義系統範圍
  目標：鎖定為單交易所、單帳戶、單策略。
  交付物：MVP 範圍文件與 README 說明。
  備註：先不要做 RL。

- [x] 建立專案骨架
  目標：讓 API、worker、core 模組可以啟動。
  交付物：初版專案目錄結構。
  備註：建議使用 Python + FastAPI。

- [x] 設計資料庫 schema
  目標：可儲存 candles、signals、orders、fills、positions。
  交付物：SQLite MVP schema。
  備註：PostgreSQL migration 尚未開始。

- [x] 接入市場資料
  目標：穩定抓取 OHLCV。
  交付物：`candles` 成功寫入資料庫。
  備註：先從一個 symbol 開始。

- [x] 建立特徵計算流程
  目標：產生基本特徵。
  交付物：MA 特徵已可用。
  備註：returns、volatility 尚未加入。

- [x] 實作策略引擎
  目標：產生 buy/sell/hold 訊號。
  交付物：signals 成功寫入資料庫。
  備註：先從 MA Cross 開始。

- [~] 實作風控引擎
  目標：阻擋危險交易。
  交付物：初版風控規則。
  備註：已完成重複訊號、持倉限制、最大倉位、冷卻時間、正式 daily ledger 與自動 kill switch；`daily_realized_pnl` 已在現有 `fills` 資料上完成 2026-03-17 到 2026-03-20 對帳驗證，剩餘缺口主要是更長時間的實跑驗收。

- [x] 建立模擬下單執行
  目標：完成 paper trading。
  交付物：paper broker。
  備註：手續費與滑點先簡化即可。

- [x] 同步 order / fill / position 狀態
  目標：讓狀態可一致追蹤。
  交付物：orders 與 positions 對得上。
  備註：這是 MVP 核心。

- [x] 計算損益
  目標：可以查看 realized / unrealized PnL。
  交付物：PnL service。
  備註：每日快照一開始可選做。

- [x] 建立基本 API
  目標：提供查詢與控制能力。
  交付物：`/health`、`/orders`、`/positions` 等 API。
  備註：pause/stop 已完成，自動 kill switch 尚未完成。

- [x] 加入 logging 與 audit
  目標：每次決策都可回查。
  交付物：scheduler log、health、structured audit events 已可查。
  備註：目前已涵蓋 pipeline、risk、scheduler、kill switch、alert delivery。

- [x] 建立 Docker Compose
  目標：本機可一鍵啟動。
  交付物：`docker-compose.yml`、`Dockerfile`。
  備註：`api`、`scheduler`、`postgres` Compose runtime 與 PostgreSQL validation 已實跑驗證。

- [ ] 完成 paper trading 驗證
  目標：連續運行至少一週。
  交付物：驗證紀錄。
  備註：soak validation 摘要腳本已完成，`build_soak_validation_report()` 已含 signal quality（BUY/SELL/HOLD 分佈、approval rate、execution rate、per-strategy breakdown）；尚未累積完整 72h 連跑紀錄。

## Stage 2 Growth

- [x] 分離 API 與 worker
  目標：降低彼此干擾。
  交付物：可獨立部署的 API 與 worker。
  備註：已拆出 `data-worker`、`strategy-worker`、`risk-worker`、`execution-worker` Compose services；split-workers double execution bug 已修（`--queue-drain` + `queue_dispatch`）；`depends_on_job_id` 與 payload propagation 確保 market_data → strategy → risk → execution 依序執行；stale lease 自動回收；`depends_on: api` false coupling 已移除。（2026-03-21）

- [x] 建立市場資料 worker
  目標：將行情接入獨立處理。
  交付物：data worker。
  備註：`market-data-only` mode 與 `data-worker` 可獨立執行，支援 runtime active symbols，queue_drain 模式下依賴鏈保證在 strategy job 前完成。backpressure / 更廣商品集為 Stage 3 議題。（2026-03-20）

- [x] 建立策略 worker
  目標：支援多策略排程。
  交付物：strategy worker。
  備註：`strategy-only` mode、strategy registry、multi-strategy、multi-symbol 可跑；per-strategy error isolation 已完成（一個 crash 不影響其他）；strategy worker 現在只負責 signals，不再內含 risk evaluation；multi-strategy double-execution 已透過 pending approved BUY qty 修復。（2026-03-21）

- [x] 建立 risk worker
  目標：將風控評估從策略產生流程中切出。
  交付物：risk worker。
  備註：`risk-only` mode、`risk-worker` heartbeat/log、queue drain/dispatch、admin drain/retry controls 均已完成；pipeline 已正式固定為 `market_data -> strategy -> risk -> execution`。（2026-03-21）

- [x] 建立 execution worker
  目標：將重試與同步邏輯獨立。
  交付物：execution worker。
  備註：`execution-only` mode 可獨立執行；queue retry / stale batch recover / clear / audit trail 已完成；orphan order 偵測（`check_orphan_orders` step）已加入每次 execution job；上游改為接收 risk step 傳下來的 `risk_event_ids`。（2026-03-21）

- [x] 引入 Redis 或簡易 queue
  目標：支援非同步任務分派。
  交付物：初版 queue。
  備註：已完成 persistent `job_queue`、enqueue/list、`run-next`、pipeline batch dispatch / drain / queue_batch、health/admin queue summary、retry / recover / clear controls、stale batch detection、queue alerting 與 queue control audit trail。

- [x] 支援多商品
  目標：從單一資產擴展。
  交付物：symbol 設定與執行能力。
  備註：runtime active symbols、targeted pipeline symbols、multi-symbol market data / strategy / risk / execution、health/admin/runtime summary 均已完成。新增交易對需修改 `app/data/symbols.py`，屬 Stage 3 動態商品管理議題。（2026-03-20）

- [x] 支援多策略
  目標：可同時運行多個策略。
  交付物：strategy registry。
  備註：strategy registry、runtime control、priority/limit/enable-disable、leaderboard、closed trade reporting 均已完成；per-strategy error isolation 已完成；同一 pipeline cycle 雙重執行 bug 已透過 pending approved BUY qty 修復；風控集中化治理為 Stage 3 議題。（2026-03-20）

- [x] 建立告警通知
  目標：異常時可即時通知。
  交付物：Telegram 或 Slack alerts。
  備註：Telegram alert、health alert dedupe、queue failure / stale batch alert、stale worker alert、execution failure alert、test endpoint 與 queue control audit 已完成；更細的 order-level / broker-level operational coverage 仍可再補。

## Stage 3 Production

- [x] 抽象化 broker adapter
  目標：支援多交易所。
  交付物：adapter interface。
  備註：`BrokerClient` Protocol、`SimulatedBrokerClient`、`BinanceBrokerClient`（HMAC-SHA256、MARKET order、weighted-avg fill price）、`BinanceLiveExecutionAdapter` 均已完成；testnet 端到端驗證通過（帳號連線 → dry-run order → 完整 pipeline 7 步）；切換 `CRYPTO_EXECUTION_BACKEND=binance` 即可上線。（2026-03-21）

- [x] 風控服務獨立化
  目標：提高風控一致性。
  交付物：risk service。
  備註：`run_risk_job()` 獨立成 `app/pipeline/risk_job.py`；pipeline 固定為 `market_data → strategy → risk → execution` 四步；`_propagate_dependent_job_payload` 自動將 `signal_ids`/`risk_event_ids` 傳遞到下游 queue job；LEFT JOIN 偵測未評估訊號，無需 schema 變更；strategy_job 不再含風控邏輯。（2026-03-21）

- [x] 建立投組服務
  目標：管理跨策略資金分配。
  交付物：portfolio service。
  備註：`app/portfolio/portfolio_service.py` 完整實作；`PortfolioConfig`（`total_capital`、`max_strategy_allocation_pct`、`max_total_exposure_pct`）、`get/set_portfolio_config()`、`get_portfolio_summary()`（per-symbol notional + per-strategy exposure）、`check_portfolio_limits()`（total exposure + per-strategy allocation，含 pending approved BUY 防止 concurrent over-allocation）；`GET /portfolio`、`GET /portfolio/config`、`POST /portfolio/config` 均已完成；`check_portfolio_limits()` 已整合至 `risk_service._evaluate_signal()` BUY 路徑；`total_capital=0` 時不強制執行（純資訊模式）。（2026-03-21）

- [x] 建立監控能力
  目標：提高可觀測性。
  交付物：metrics 與 dashboard。
  備註：`GET /metrics` endpoint 已完成（2026-03-20）；aggregates signals、risk reject rate、fill volume、realized PnL、queue latency，period_hours 可設定（1–168h）。

- [x] 建立 kill switch
  目標：異常時可強制停止交易。
  交付物：手動與自動停機控制。
  備註：`app/system/kill_switch.py` 完整 state management；`POST /kill-switch/enable|disable`、`GET /kill-switch/status`；admin UI 顯示狀態與按鈕；risk service 在 daily loss breach 時自動觸發；pipeline 與所有 scheduler modes（direct / queue_batch / queue_drain / queue_dispatch / split-workers）均檢查 kill switch；Telegram alert on enable（2026-03-20）。

- [x] 擴充 audit log
  目標：追蹤所有關鍵事件。
  交付物：audit store。
  備註：`log_event()` 已涵蓋 pipeline_run、risk_evaluation、kill_switch、scheduler_control、queue_control、execution_control、alert_delivery；本次補齊 `portfolio_config`（POST /portfolio/config）、`risk_config`（POST/DELETE /risk-config/{strategy}）、`param_sync`（POST /backtest/sweep/{strategy}/apply-best-params）共四條路徑；4 條 integration test 驗證 audit row 寫入。（2026-03-21）

- [ ] 引入 event bus
  目標：支援更大規模服務拆分。
  交付物：event bus 或 queue。
  備註：到這個階段再做。

## Stage 4 AI / RL

- [x] 建立回測與模擬器
  目標：建立接近真實的研究環境。
  交付物：simulator。
  備註：`run_backtest()` 核心引擎（in-memory SQLite、fill_on close/next_open）、`compute_metrics()`（Sharpe/drawdown/win_rate/profit_factor）、`load_candles_from_db()`、`run_parameter_sweep()`、`run_walk_forward()` 及對應 API endpoints（GET /backtest、POST /backtest/sweep、POST /backtest/walk-forward）均已完成；回測結果持久化至 `backtest_runs`（migration 020），`GET /backtest/history` 支援 symbol/strategy/run_type 篩選與分頁；`POST /backtest/sweep/{strategy}/apply-best-params` 讀取最佳 sweep 結果並自動更新 risk_configs。（2026-03-21）

- [x] 建立實驗追蹤
  目標：管理研究結果。
  交付物：tracking system。
  備註：全六組完成（migrations 021-025）；涵蓋 experiment_name 分組、tags/notes、compare、leaderboard、champion promote/get、WF fold grouping + 聚合統計、equity curve 持久化與查詢。409 tests 通過。（2026-03-21）

- [x] 建立 feature store
  目標：保持 train/live 特徵一致。
  交付物：feature layer。
  備註：`app/features/compute.py`（v1 特徵集：MA/RSI/MACD/BBands/returns/volatility）、`app/features/store.py`（upsert + 分頁查詢）、migration 026、5 個 API endpoints、55 tests 通過。（2026-03-21）

- [x] 建立訓練流程
  目標：讓訓練可重現。
  交付物：training jobs。
  備註：`app/training/dataset.py`（build\_dataset + chronological split）、`app/training/trainer.py`（pure-Python logistic regression，無外部 ML 套件）、`app/training/job_service.py`（training\_jobs 表 + CRUD）、migration 027、`POST /training/jobs`、45 tests 通過。（2026-03-21）

- [x] 建立 model registry
  目標：控管模型版本。
  交付物：registry。
  備註：`app/registry/registry_service.py`（candidate/champion/archived 狀態機、promote/archive/rollback）、migration 028、7 個 API endpoints（含 /registry/champion/{symbol}）、38 tests 通過。（2026-03-21）

- [x] 建立 inference service
  目標：線上提供已核准模型推論。
  交付物：inference service。
  備註：`app/inference/service.py`（predict\_latest/predict\_batch/get\_inference\_status，champion-only serving）、3 個 API endpoints、34 tests 通過。（2026-03-21）

- [x] 進行 RL 實驗
  目標：驗證 RL 是否值得投入。
  交付物：RL strategy report。
  備註：`app/rl/environment.py`（TradingEnv）、`app/rl/agent.py`（REINFORCE policy gradient，pure Python）、`app/rl/experiment.py`（run\_rl\_experiment，含 RL vs BuyAndHold vs Supervised 三基準比較 + verdict）、migration 029、`POST /training/rl-jobs`、37 tests 通過。（2026-03-21）

## Stage 4 實驗追蹤細項

> 基礎層（`backtest_runs` 表 + `GET /backtest/history` + `apply-best-params`）已就位。
> 以下按優先順序列出，每組可獨立實作。

### 第一組：實驗命名與分組（低成本、高價值）✅

- [x] **migration 021：`backtest_runs` 加入 `experiment_name` 欄位**
- [x] **`GET /backtest`、`POST /backtest/sweep`、`POST /backtest/walk-forward` 接受 `experiment_name` 參數**
- [x] **`GET /backtest/history` 新增 `experiment_name` 篩選**
- [x] **`GET /backtest/experiments` — 列出所有 experiment_name**
- [x] **`GET /backtest/runs/{id}` — 取得單一 run**

### 第二組：Run 標籤與備註（彈性標記）✅

- [x] **migration 022：`backtest_runs` 加入 `tags_json` 與 `notes` 欄位**
- [x] **`PATCH /backtest/runs/{id}` — 更新 notes 和 tags**

### 第三組：Run 比較 API（研究核心）✅

- [x] **`GET /backtest/compare?ids=1,2,3` — 多 run 並排比較，含 per-metric best**
- [x] **`GET /backtest/leaderboard/{strategy}` — 策略最佳 run 排行榜**

### 第四組：Champion Run 管理（與 apply-best-params 整合）✅

- [x] **migration 023：`backtest_runs` 加入 `promoted_at` 欄位**
- [x] **`POST /backtest/runs/{id}/promote` — 標記為 champion，寫入 audit log，清除舊 champion**
- [x] **`GET /backtest/champion/{strategy}` — 取得目前 champion run**

  備註：第一至四組已於 2026-03-21 全部完成，24 條測試通過（394 total）。

### 第五組：Equity Curve 持久化✅

- [x] **migration 025：`backtest_runs` 加入 `equity_curve_json` 欄位**
- [x] **`GET /backtest` 持久化 single run 的 equity curve**
- [x] **`POST /backtest/walk-forward` 每折持久化 `test_equity_curve`（walk_forward.py 同步更新）**
- [x] **`GET /backtest/runs/{id}/equity-curve` — 取回 equity curve**
  （run 不存在 → 404；curve 未存 → 空 list）

  備註：第五組已於 2026-03-21 完成，6 條測試通過（409 total）。第五組不存 sweep runs 的 curve（組合數過多）。
  交付物：endpoint + 測試。

### 第六組：Walk-forward Fold 分組（實驗完整性）✅

- [x] **migration 024：`backtest_runs` 加入 `wf_group_id` 與 `fold_index` 欄位**
- [x] **`POST /backtest/walk-forward` 生成 UUID `wf_group_id`，各 fold 攜帶 `fold_index` 持久化，response 回傳 `wf_group_id`**
- [x] **`GET /backtest/walk-forward/groups` — 列出所有 WF group 摘要**
- [x] **`GET /backtest/walk-forward/groups/{wf_group_id}` — 取得各折 + 聚合統計（avg/min/max sharpe、return、drawdown 等）**

  備註：第六組已於 2026-03-21 完成，9 條測試通過（403 total）。

---

### 實作優先順序建議

| 順序 | 組 | 理由 |
|---|---|---|
| 1 | 第一組（實驗命名） | 成本最低（1 個 migration），立即提升整理能力 |
| 2 | 第三組（比較 + 排行榜） | 核心研究工具，不依賴其他組 |
| 3 | 第四組（Champion 管理） | 讓 apply-best-params 有完整稽核鏈 |
| 4 | 第二組（標籤備註） | 方便但非關鍵，可晚做 |
| 5 | 第六組（WF 分組） | 補完 walk-forward 可用性 |
| 6 | 第五組（Equity curve） | 資料量較大，最後再做 |

## 建議補充欄位

- 負責人
- 開始日期
- 預計完成日
- 實際完成日
- 阻塞因素
- 驗收標準

## 目前建議優先開工

- [x] 補完 Stage 1 正式 `daily ledger`
  目標：讓單日損益限制不再依賴保守版邏輯。
  備註：`daily_realized_pnl` 已落地，風控已接入，且已用 2026-03-17 到 2026-03-20 的實際 `fills` 對帳驗證。

- [x] 補齊 soak report signal quality
  目標：讓 soak validation 涵蓋訊號品質分析。
  備註：`_signal_quality_check()` 已加入 `build_soak_validation_report()`，涵蓋 actionable_rate、approval_rate、duplicate_rejection_rate、execution_rate 及 per-strategy breakdown；三道 issue 偵測已上線。（2026-03-21）

- [x] 新增 RSI、Bollinger Bands、MACD 策略
  目標：擴充策略庫，提供更多 signal 來源。
  備註：三個策略已加入 STRATEGY_REGISTRY，支援所有現有 endpoints（backtest、sweep、walk-forward、pipeline/run）。（2026-03-21）

- [ ] 累積一週 paper trading / soak validation 紀錄
  目標：關閉 Stage 1 驗證缺口。
  備註：目前已有 soak validation 與摘要，但尚未累積完整 72h 連跑紀錄。

- [x] 推進 broker adapter groundwork
  目標：讓 `simulated_live` 不再只是 placeholder。
  備註：已完成 `BrokerClient` Protocol、`SimulatedBrokerClient`、`live_broker` 執行模組，`SimulatedLiveExecutionAdapter` 已路由至真實 execution path，`is_live`/`placeholder` 屬性已加入所有 adapter。（2026-03-20）

- [x] 補 broker / order-level alerting 與保護機制
  目標：讓 operational alerts 從 queue / worker 層延伸到下單與 broker 層。
  備註：已完成 `unfilled_order_count`、`latest_fill` 進入 `_pipeline_check`，`_broker_protection_check` 加入 unfilled order degraded 條件，alert message 含 unfilled_orders/latest_fill_price。（2026-03-20）

## Stage 5 自動重訓管線（Auto-Retrain Pipeline）

> 目標：將 ML 鏈從「手動實驗」升級為「持續學習」，讓模型隨新資料自動更新。

- [ ] **Step 1：Retrain Config（觸發條件設定）**
  目標：定義何時觸發重訓、用什麼指標決定是否 promote。
  交付物：`retrain_configs` 表（migration 030）、`GET/POST /retrain/config/{symbol}` API。
  欄位：`min_new_candles`（預設 100）、`promote_metric`（accuracy/f1/sharpe）、`min_improve_pct`（預設 1%）。

- [ ] **Step 2：Retrain Job 核心邏輯**
  目標：實作完整自動重訓流程。
  交付物：`app/pipeline/retrain_job.py`。
  流程：`materialize_features → build_dataset + train → evaluate vs champion → register_model → promote if better`。
  備註：新模型未達門檻時仍存入 registry as candidate（保留記錄）。

- [ ] **Step 3：整合 Job Queue**
  目標：讓 retrain 可排入 job queue，支援 retry / fail / audit。
  交付物：`job_type='retrain'` 加入 job queue、`run_retrain_job()` 可由 worker 執行。

- [ ] **Step 4：觸發端點**
  目標：提供 API 讓 scheduler 或手動呼叫觸發重訓。
  交付物：`POST /retrain/trigger`（檢查新 candle 數量，達閾值則 enqueue）、`GET /retrain/status/{symbol}`（回傳上次訓練時間、距觸發還差幾根）。

- [ ] **Step 5：Scheduler 整合**
  目標：讓重訓自動發生，不需人工介入。
  交付物：兩種模式均支援：
  - 資料驅動：market_data job 完成後串接 retrain job（`depends_on_job_id`）。
  - 時間驅動：scheduler 定期呼叫 `POST /retrain/trigger`。

- [ ] **Step 6：告警整合**
  目標：promote / 失敗時即時通知。
  交付物：promote 成功發 Telegram alert（`model_promoted`）；訓練失敗沿用現有 alert 機制；無改善僅寫 audit log。

- [ ] **Step 7：測試**
  目標：確保重訓流程正確、regression 無誤。
  交付物：`tests/test_retrain.py`，涵蓋：完整流程、新模型較差時不 promote、trigger skipped/enqueued 邏輯、scheduler 整合。

## 里程碑

- [x] M1：市場資料可穩定落庫，策略可產生 signals。
- [x] M2：paper broker 可執行，且 `orders`、`fills`、`positions` 狀態一致。
- [~] M3：風控、pause、kill switch 可用，系統可連跑數天。
  備註：pause、基本風控、正式 daily ledger、自動與手動 kill switch、告警、admin UI 已完成；multi-strategy double-execution 修復；剩餘缺口為一週連跑 soak validation 驗收。
- [~] M4：API 與監控可用，系統可進入小額實盤準備階段。
  備註：Stage 2 全部項目已完成；Binance testnet 端到端驗證通過；風控服務獨立化完成（4-step pipeline）；回測持久化與 strategy param 自動同步完成；投組服務完成；audit log 已涵蓋所有關鍵路徑；Stage 3 僅剩 event bus（可延後）。
