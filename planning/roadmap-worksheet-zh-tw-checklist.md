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

- [ ] 實作風控引擎
  目標：阻擋危險交易。
  交付物：初版風控規則。
  備註：已完成重複訊號、持倉限制、最大倉位、冷卻時間、保守版單日虧損限制；正式 daily ledger 尚未完成。

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
  備註：已提供 soak validation 摘要腳本，但尚未累積一週連跑紀錄。

## Stage 2 Growth

- [~] 分離 API 與 worker
  目標：降低彼此干擾。
  交付物：可獨立部署的 API 與 worker。
  備註：已拆出 `data-worker`、`strategy-worker`、`execution-worker` Compose services，job module / entrypoint / scheduler mode / heartbeat / log 已分離；尚未引入 queue，也還不是獨立事件驅動 worker 架構。

- [~] 建立市場資料 worker
  目標：將行情接入獨立處理。
  交付物：data worker。
  備註：`market-data-only` mode 與 `data-worker` 已可獨立執行，且已支援 runtime active symbols；尚未做 queue / backpressure / 更完整 market universe 管理。

- [~] 建立策略 worker
  目標：支援多策略排程。
  交付物：strategy worker。
  備註：`strategy-only` mode、strategy registry、multi-strategy、multi-symbol 都已可跑；尚未做真正的 worker isolation 與 queue-based dispatch。

- [~] 建立 execution worker
  目標：將重試與同步邏輯獨立。
  交付物：execution worker。
  備註：`execution-only` mode 與 `execution-worker` 已可獨立執行，並可依 active symbols 過濾 pending risks；尚未做更完整的 retry / reconciliation worker 化。

- [~] 引入 Redis 或簡易 queue
  目標：支援非同步任務分派。
  交付物：初版 queue。
  備註：已完成 persistent `job_queue`、enqueue/list、`run-next`、scheduler queue-dispatch / queue-drain、health/admin queue summary、retry controls 與 queue observability；尚未完成 full queue-native runtime 與更完整 backpressure / worker orchestration。

- [~] 支援多商品
  目標：從單一資產擴展。
  交付物：symbol 設定與執行能力。
  備註：目前已支援 runtime active symbols、targeted pipeline symbols、multi-symbol market data / strategy / risk / execution、health/admin/runtime summary；尚未完成更廣泛商品集與更完整 portfolio/risk orchestration。

- [~] 支援多策略
  目標：可同時運行多個策略。
  交付物：strategy registry。
  備註：目前已有 strategy registry、runtime strategy control、priority/limit/enable-disable、leaderboard、closed trade reporting；風控與 execution 雖已能配合多策略運作，但集中化治理仍可再加強。

- [~] 建立告警通知
  目標：異常時可即時通知。
  交付物：Telegram 或 Slack alerts。
  備註：Telegram alert、health alert dedupe、queue failure alert、stale worker alert、execution failure alert、test endpoint 已完成；更細的 order-level / broker-level operational coverage 仍可再補。

## Stage 3 Production

- [ ] 抽象化 broker adapter
  目標：支援多交易所。
  交付物：adapter interface。
  備註：避免綁死單一 broker。

- [ ] 風控服務獨立化
  目標：提高風控一致性。
  交付物：risk service。
  備註：風控必須高於策略。

- [ ] 建立投組服務
  目標：管理跨策略資金分配。
  交付物：portfolio service。
  備註：多帳戶前應先完成。

- [ ] 建立監控能力
  目標：提高可觀測性。
  交付物：metrics 與 dashboard。
  備註：至少看 latency、reject rate、PnL。

- [ ] 建立 kill switch
  目標：異常時可強制停止交易。
  交付物：手動與自動停機控制。
  備註：實盤必備。

- [ ] 擴充 audit log
  目標：追蹤所有關鍵事件。
  交付物：audit store。
  備註：支援除錯與稽核。

- [ ] 引入 event bus
  目標：支援更大規模服務拆分。
  交付物：event bus 或 queue。
  備註：到這個階段再做。

## Stage 4 AI / RL

- [ ] 建立回測與模擬器
  目標：建立接近真實的研究環境。
  交付物：simulator。
  備註：RL 前必做。

- [ ] 建立實驗追蹤
  目標：管理研究結果。
  交付物：tracking system。
  備註：概念類似 MLflow。

- [ ] 建立訓練流程
  目標：讓訓練可重現。
  交付物：training jobs。
  備註：不要進入 live path。

- [ ] 建立 feature store
  目標：保持 train/live 特徵一致。
  交付物：feature layer。
  備註：避免 training-serving skew。

- [ ] 建立 model registry
  目標：控管模型版本。
  交付物：registry。
  備註：必須支援 rollback。

- [ ] 建立 inference service
  目標：線上提供已核准模型推論。
  交付物：inference service。
  備註：只做推論，不做訓練。

- [ ] 進行 RL 實驗
  目標：驗證 RL 是否值得投入。
  交付物：RL strategy report。
  備註：最後再做。

## 建議補充欄位

- 負責人
- 開始日期
- 預計完成日
- 實際完成日
- 阻塞因素
- 驗收標準

## 里程碑

- [x] M1：市場資料可穩定落庫，策略可產生 signals。
- [x] M2：paper broker 可執行，且 `orders`、`fills`、`positions` 狀態一致。
- [ ] M3：風控、pause、kill switch 可用，系統可連跑數天。
  備註：pause、手動 kill switch、基本風控、告警、admin UI 已完成；尚缺正式 daily ledger、自動 kill switch 與多日連跑驗證。
- [~] M4：API 與監控可用，系統可進入小額實盤準備階段。
  備註：API、admin runtime view、queue observability、split worker status、multi-symbol / multi-strategy controls、PostgreSQL validation 與 CI 已完成；尚未完成 broker adapter、風控服務獨立化、投組服務與更完整 production monitoring。
