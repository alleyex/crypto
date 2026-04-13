from typing import Callable

from app.core.db import DBConnection
from app.core.db import get_backend_name
from app.core.db import get_table_column_type
from app.core.db import get_table_columns
from app.core.db import table_exists
from app.core.db import utc_now_iso

Migration = tuple[str, Callable[[DBConnection], None]]

# ── Column-type helpers ────────────────────────────────────────────────────────────────────────

def _auto_id_column_sql(backend: str) -> str:
    if backend == "postgres":
        return "id BIGSERIAL PRIMARY KEY"
    return "id INTEGER PRIMARY KEY"

def _epoch_millis_column_sql(backend: str) -> str:
    if backend == "postgres":
        return "BIGINT"
    return "INTEGER"

CREATE_SCHEMA_MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""

POSTGRES_MIGRATION_LOCK_ID = 8_455_771_239

LEGACY_UTC_TIMESTAMP_TARGETS: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("audit_events", ("id",), ("created_at",)),
    ("runtime_heartbeats", ("component",), ("last_seen_at",)),
    ("risk_events", ("id",), ("created_at",)),
    ("orders", ("id",), ("created_at",)),
    ("fills", ("id",), ("created_at",)),
    ("positions", ("symbol",), ("updated_at",)),
    ("daily_realized_pnl", ("symbol", "pnl_date"), ("updated_at",)),
    ("risk_configs", ("strategy_name",), ("updated_at",)),
    ("portfolio_config", ("id",), ("updated_at",)),
    ("signals", ("id",), ("created_at",)),
    ("pnl_snapshots", ("id",), ("created_at",)),
    ("training_jobs", ("id",), ("created_at", "started_at", "finished_at")),
    ("model_registry", ("id",), ("created_at", "promoted_at")),
    ("schema_migrations", ("version",), ("applied_at",)),
)

STARTUP_LEGACY_UTC_TIMESTAMP_TABLES: frozenset[str] = frozenset(
    {
        "runtime_heartbeats",
        "risk_events",
        "orders",
        "fills",
        "positions",
        "daily_realized_pnl",
        "risk_configs",
        "portfolio_config",
        "training_jobs",
        "model_registry",
    }
)

POSTGRES_TEXT_TIMESTAMP_TYPES: frozenset[str] = frozenset({"text", "character varying", "varchar"})

POSTGRES_TEXT_TIMESTAMP_DEFAULT_TABLES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("schema_migrations", ("applied_at",)),
    ("signals", ("created_at",)),
    ("risk_events", ("created_at",)),
    ("orders", ("created_at",)),
    ("fills", ("created_at",)),
    ("positions", ("updated_at",)),
    ("pnl_snapshots", ("created_at",)),
    ("daily_realized_pnl", ("updated_at",)),
    ("audit_events", ("created_at",)),
    ("runtime_heartbeats", ("last_seen_at",)),
    ("job_queue", ("created_at",)),
    ("risk_configs", ("updated_at",)),
    ("portfolio_config", ("updated_at",)),
    ("backtest_runs", ("created_at",)),
    ("feature_vectors", ("created_at",)),
    ("training_jobs", ("created_at",)),
    ("model_registry", ("created_at",)),
)

# ── Tables: candles / signals / risk_events / orders / fills / positions / pnl ───────────────

def _create_candles_table(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    numeric = "NUMERIC(20,8)"
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS candles (
            {_auto_id_column_sql(backend)},
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time {_epoch_millis_column_sql(backend)} NOT NULL,
            open {numeric} NOT NULL,
            high {numeric} NOT NULL,
            low {numeric} NOT NULL,
            close {numeric} NOT NULL,
            volume {numeric} NOT NULL,
            close_time {_epoch_millis_column_sql(backend)} NOT NULL,
            quote_asset_volume {numeric},
            number_of_trades INTEGER,
            taker_buy_base_volume {numeric},
            taker_buy_quote_volume {numeric},
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe, open_time)
        );
        """
    )

def _create_signals_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS signals (
            {_auto_id_column_sql(get_backend_name(connection))},
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            short_ma REAL NOT NULL,
            long_ma REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _create_risk_events_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS risk_events (
            {_auto_id_column_sql(get_backend_name(connection))},
            signal_id INTEGER,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _add_risk_events_signal_id(connection: DBConnection) -> None:
    if table_exists(connection, "risk_events") and "signal_id" not in get_table_columns(connection, "risk_events"):
        connection.execute("ALTER TABLE risk_events ADD COLUMN signal_id INTEGER;")

def _create_orders_and_fills_tables(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS orders (
            {_auto_id_column_sql(backend)},
            client_order_id TEXT NOT NULL UNIQUE,
            risk_event_id INTEGER UNIQUE,
            broker_name TEXT,
            broker_order_id TEXT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            side TEXT NOT NULL,
            qty NUMERIC(20,8) NOT NULL,
            price NUMERIC(20,8) NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS fills (
            {_auto_id_column_sql(backend)},
            order_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty NUMERIC(20,8) NOT NULL,
            price NUMERIC(20,8) NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        );
        """
    )

def _add_orders_risk_event_id(connection: DBConnection) -> None:
    if table_exists(connection, "orders") and "risk_event_id" not in get_table_columns(connection, "orders"):
        connection.execute("ALTER TABLE orders ADD COLUMN risk_event_id INTEGER;")
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_risk_event_id ON orders(risk_event_id);"
    )

def _add_orders_broker_metadata(connection: DBConnection) -> None:
    if not table_exists(connection, "orders"):
        return
    columns = get_table_columns(connection, "orders")
    if "broker_name" not in columns:
        connection.execute("ALTER TABLE orders ADD COLUMN broker_name TEXT;")
    if "broker_order_id" not in columns:
        connection.execute("ALTER TABLE orders ADD COLUMN broker_order_id TEXT;")

def _add_performance_indexes(connection: DBConnection) -> None:
    # fills(symbol) — daily PnL lookup and position reconstruction
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fills_symbol ON fills(symbol);"
    )
    # fills(order_id) — unfilled order count LEFT JOIN
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);"
    )
    # signals(symbol, timeframe, strategy_name, id) — previous signal lookup
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_lookup"
        " ON signals(symbol, timeframe, strategy_name, id);"
    )
    # risk_events(decision, id) — rejection streak scan
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_events_decision_id"
        " ON risk_events(decision, id);"
    )

def _create_positions_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY,
            qty NUMERIC(20,8) NOT NULL,
            avg_price NUMERIC(20,8) NOT NULL,
            realized_pnl NUMERIC(20,8) NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _add_positions_realized_pnl(connection: DBConnection) -> None:
    if table_exists(connection, "positions") and "realized_pnl" not in get_table_columns(connection, "positions"):
        connection.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL NOT NULL DEFAULT 0;")

def _create_pnl_snapshots_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pnl_snapshots (
            {_auto_id_column_sql(get_backend_name(connection))},
            symbol TEXT NOT NULL,
            qty REAL NOT NULL,
            avg_price REAL NOT NULL,
            market_price REAL NOT NULL,
            unrealized_pnl REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _create_daily_realized_pnl_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_realized_pnl (
            symbol TEXT NOT NULL,
            pnl_date TEXT NOT NULL,
            realized_pnl REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, pnl_date)
        );
        """
    )

# ── Tables: audit / heartbeats / job queue ────────────────────────────────────────────────────

def _create_audit_events_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS audit_events (
            {_auto_id_column_sql(get_backend_name(connection))},
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT NOT NULL,
            message TEXT NOT NULL,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _create_runtime_heartbeats_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_heartbeats (
            component TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            message TEXT NOT NULL,
            payload_json TEXT,
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _create_job_queue_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS job_queue (
            {_auto_id_column_sql(get_backend_name(connection))},
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT,
            result_json TEXT,
            error_message TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            completed_at TEXT
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_job_queue_status_created_at ON job_queue(status, created_at, id);"
    )

def _add_job_queue_depends_on(connection: DBConnection) -> None:
    if table_exists(connection, "job_queue") and "depends_on_job_id" not in get_table_columns(connection, "job_queue"):
        connection.execute("ALTER TABLE job_queue ADD COLUMN depends_on_job_id INTEGER;")

# ── Tables: risk config / portfolio config ────────────────────────────────────────────────────

def _create_risk_configs_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_configs (
            strategy_name TEXT PRIMARY KEY,
            order_qty NUMERIC(20,8) NOT NULL,
            max_position_qty NUMERIC(20,8) NOT NULL,
            cooldown_seconds INTEGER NOT NULL,
            stop_loss_pct NUMERIC(20,8) NOT NULL DEFAULT 0,
            max_daily_loss NUMERIC(20,8) NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def _add_risk_configs_stop_loss_pct(connection: DBConnection) -> None:
    if table_exists(connection, "risk_configs") and "stop_loss_pct" not in get_table_columns(connection, "risk_configs"):
        connection.execute("ALTER TABLE risk_configs ADD COLUMN stop_loss_pct NUMERIC(20,8) NOT NULL DEFAULT 0;")

def _add_signals_lookup_index(connection: DBConnection) -> None:
    """Composite index for SELECT_PREVIOUS_SIGNAL_SQL in risk_service.

    The query filters on (symbol, timeframe, strategy_name) and sorts by id DESC —
    without this index every risk evaluation is a full table scan on signals.
    """
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signals_symbol_tf_strategy_id
        ON signals (symbol, timeframe, strategy_name, id DESC);
        """
    )

def _add_fills_symbol_index(connection: DBConnection) -> None:
    """Index fills by symbol to speed up per-symbol aggregation in positions_service."""
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fills_symbol ON fills (symbol);"
    )

def _add_job_queue_batch_id_column(connection: DBConnection) -> None:
    """Promote batch_id from inside payload_json to a dedicated indexed column.

    This replaces the LIKE '%"batch_id": "..."% ' full-scan pattern used in
    fail_batch_jobs() and run_next_pipeline_batch() with a direct equality
    lookup on an indexed column.
    """
    if table_exists(connection, "job_queue") and "batch_id" not in get_table_columns(connection, "job_queue"):
        connection.execute("ALTER TABLE job_queue ADD COLUMN batch_id TEXT;")
    # Backfill existing rows from payload_json so historical jobs remain queryable.
    # Use a parameterised LIKE so the % characters are not misinterpreted as
    # psycopg format-string placeholders.
    backend = get_backend_name(connection)
    if backend == "postgres":
        connection.execute(
            """
            UPDATE job_queue
            SET batch_id = payload_json::json->>'batch_id'
            WHERE batch_id IS NULL
              AND payload_json IS NOT NULL
              AND payload_json LIKE ?;
            """,
            ("%batch_id%",),
        )
    else:
        connection.execute(
            """
            UPDATE job_queue
            SET batch_id = json_extract(payload_json, '$.batch_id')
            WHERE batch_id IS NULL AND payload_json IS NOT NULL;
            """
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_job_queue_batch_id ON job_queue (batch_id);"
    )

def _create_portfolio_config_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_config (
            id INTEGER PRIMARY KEY,
            total_capital NUMERIC(20,8) NOT NULL DEFAULT 0,
            max_strategy_allocation_pct NUMERIC(20,8) NOT NULL DEFAULT 0.5,
            max_total_exposure_pct NUMERIC(20,8) NOT NULL DEFAULT 0.8,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

# ── Tables: backtest / feature vectors / training / model registry ────────────────────────────

def _create_backtest_runs_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS backtest_runs (
            {_auto_id_column_sql(get_backend_name(connection))},
            run_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '1m',
            days INTEGER,
            candle_count INTEGER NOT NULL,
            trade_count INTEGER NOT NULL,
            fill_on TEXT NOT NULL DEFAULT 'close',
            initial_capital NUMERIC(20,8),
            final_equity NUMERIC(20,8),
            total_return_pct NUMERIC(20,8),
            max_drawdown_pct NUMERIC(20,8),
            sharpe_ratio NUMERIC(20,8),
            win_rate_pct NUMERIC(20,8),
            profit_factor NUMERIC(20,8),
            round_trips INTEGER,
            params_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_strategy "
        "ON backtest_runs(symbol, strategy_name, created_at DESC);"
    )

# ── Alterations: column type changes, index additions, data backfills ────────────────────────

def _alter_candles_epoch_columns_to_bigint(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "candles"):
        return
    connection.execute(
        "ALTER TABLE candles ALTER COLUMN open_time TYPE BIGINT;"
    )
    connection.execute(
        "ALTER TABLE candles ALTER COLUMN close_time TYPE BIGINT;"
    )

def _add_backtest_runs_experiment_name(connection: DBConnection) -> None:
    if table_exists(connection, "backtest_runs") and "experiment_name" not in get_table_columns(connection, "backtest_runs"):
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN experiment_name TEXT;")

def _add_backtest_runs_tags_notes(connection: DBConnection) -> None:
    if not table_exists(connection, "backtest_runs"):
        return
    cols = get_table_columns(connection, "backtest_runs")
    if "tags_json" not in cols:
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN tags_json TEXT;")
    if "notes" not in cols:
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN notes TEXT;")

def _add_backtest_runs_promoted_at(connection: DBConnection) -> None:
    if table_exists(connection, "backtest_runs") and "promoted_at" not in get_table_columns(connection, "backtest_runs"):
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN promoted_at TEXT;")

def _add_backtest_runs_wf_columns(connection: DBConnection) -> None:
    if not table_exists(connection, "backtest_runs"):
        return
    cols = get_table_columns(connection, "backtest_runs")
    if "wf_group_id" not in cols:
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN wf_group_id TEXT;")
    if "fold_index" not in cols:
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN fold_index INTEGER;")

def _add_backtest_runs_equity_curve(connection: DBConnection) -> None:
    if table_exists(connection, "backtest_runs") and "equity_curve_json" not in get_table_columns(connection, "backtest_runs"):
        connection.execute("ALTER TABLE backtest_runs ADD COLUMN equity_curve_json TEXT;")

def _create_feature_vectors_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS feature_vectors (
            {_auto_id_column_sql(get_backend_name(connection))},
            symbol        TEXT    NOT NULL,
            timeframe     TEXT    NOT NULL,
            open_time     INTEGER NOT NULL,
            feature_set   TEXT    NOT NULL DEFAULT 'v1',
            features_json TEXT    NOT NULL,
            created_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (symbol, timeframe, open_time, feature_set)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_feature_vectors_symbol_tf"
        " ON feature_vectors (symbol, timeframe, feature_set, open_time);"
    )

def _create_training_jobs_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS training_jobs (
            {_auto_id_column_sql(get_backend_name(connection))},
            symbol       TEXT    NOT NULL,
            timeframe    TEXT    NOT NULL,
            feature_set  TEXT    NOT NULL DEFAULT 'v1',
            status       TEXT    NOT NULL DEFAULT 'pending',
            params_json  TEXT,
            dataset_json TEXT,
            metrics_json TEXT,
            model_json   TEXT,
            error        TEXT,
            started_at   TEXT,
            finished_at  TEXT,
            created_at   TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_training_jobs_symbol"
        " ON training_jobs (symbol, status, created_at);"
    )

def _add_training_jobs_job_type(connection: DBConnection) -> None:
    if table_exists(connection, "training_jobs") and "job_type" not in get_table_columns(connection, "training_jobs"):
        connection.execute("ALTER TABLE training_jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'supervised';")

def _create_model_registry_table(connection: DBConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS model_registry (
            {_auto_id_column_sql(get_backend_name(connection))},
            symbol          TEXT    NOT NULL,
            timeframe       TEXT    NOT NULL,
            feature_set     TEXT    NOT NULL DEFAULT 'v1',
            training_job_id INTEGER,
            version         TEXT    NOT NULL,
            status          TEXT    NOT NULL DEFAULT 'candidate',
            model_json      TEXT    NOT NULL,
            metrics_json    TEXT,
            notes           TEXT,
            promoted_at     TEXT,
            created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_registry_symbol"
        " ON model_registry (symbol, timeframe, feature_set, status, created_at);"
    )

def _ensure_postgres_identity_pk(
    connection: DBConnection,
    table_name: str,
) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, table_name):
        return
    if "id" not in get_table_columns(connection, table_name):
        return

    sequence_name = f"{table_name}_id_seq"
    connection.execute(
        f"CREATE SEQUENCE IF NOT EXISTS {sequence_name};"
    )
    connection.execute(
        f"ALTER SEQUENCE {sequence_name} OWNED BY {table_name}.id;"
    )
    connection.execute(
        f"ALTER TABLE {table_name} ALTER COLUMN id SET DEFAULT nextval('{sequence_name}');"
    )
    connection.execute(
        f"""
        SELECT setval(
            '{sequence_name}',
            GREATEST(COALESCE((SELECT MAX(id) FROM {table_name}), 0), 1),
            COALESCE((SELECT MAX(id) FROM {table_name}), 0) > 0
        );
        """
    )

def _ensure_postgres_training_model_identity_columns(connection: DBConnection) -> None:
    _ensure_postgres_identity_pk(connection, "training_jobs")
    _ensure_postgres_identity_pk(connection, "model_registry")

def _add_risk_events_signal_id_index(connection: DBConnection) -> None:
    """Index risk_events(signal_id) to speed up the fill-replay JOIN chain.

    SELECT_STRATEGY_FILLS_SQL joins fills → orders → risk_events → signals.
    Without this index, the join ``signals s ON s.id = re.signal_id`` requires
    a full scan of risk_events when the planner starts from the signals side.
    """
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_events_signal_id"
        " ON risk_events(signal_id);"
    )

def _add_risk_events_symbol_strategy_decision_index(connection: DBConnection) -> None:
    """Composite index for broker-protection rejection-streak queries.

    The hot-path query in _broker_protection_check filters:
        WHERE symbol = ? AND strategy_name = ? AND decision = 'REJECTED' ORDER BY id DESC LIMIT 5
    The existing idx_risk_events_decision_id(decision, id) scans all REJECTED rows
    before filtering by symbol/strategy.  This composite index makes the lookup O(log N).
    """
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_events_symbol_strategy_decision"
        " ON risk_events(symbol, strategy_name, decision, id);"
    )

def _migrate_pnl_snapshots_to_numeric(connection: DBConnection) -> None:
    """Migrate pnl_snapshots financial columns from REAL to NUMERIC(20,8).

    REAL (IEEE 754 float) can accumulate rounding errors for financial values.
    NUMERIC provides exact decimal storage consistent with other financial tables.
    """
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "pnl_snapshots"):
        return
    for col in ("qty", "avg_price", "market_price", "unrealized_pnl"):
        connection.execute(
            f"ALTER TABLE pnl_snapshots ALTER COLUMN {col}"
            f" TYPE NUMERIC(20,8) USING {col}::NUMERIC;"
        )

_CANDLES_NUMERIC_COLS = [
    "open", "high", "low", "close", "volume",
    "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume",
]

def _migrate_candles_columns_to_numeric(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "candles"):
        return
    for col in _CANDLES_NUMERIC_COLS:
        connection.execute(
            f"ALTER TABLE candles ALTER COLUMN {col} TYPE NUMERIC(20,8)"
            f" USING {col}::NUMERIC;"
        )

def _migrate_financial_columns_to_numeric(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    _FINANCIAL_COLS: dict[str, list[str]] = {
        "positions": ["qty", "avg_price", "realized_pnl"],
        "orders": ["qty", "price"],
        "fills": ["qty", "price"],
        "pnl_snapshots": ["qty", "avg_price", "market_price", "unrealized_pnl"],
        "daily_realized_pnl": ["realized_pnl"],
        "signals": ["short_ma", "long_ma"],
    }
    for table, cols in _FINANCIAL_COLS.items():
        if not table_exists(connection, table):
            continue
        existing = get_table_columns(connection, table)
        for col in cols:
            if col not in existing:
                continue
            connection.execute(
                f"ALTER TABLE {table} ALTER COLUMN {col}"
                f" TYPE NUMERIC(20,8) USING {col}::NUMERIC;"
            )

def _add_candles_symbol_timeframe_index(connection: DBConnection) -> None:
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_candles_symbol_timeframe"
        " ON candles(symbol, timeframe);"
    )

def _migrate_timestamps_to_timestamptz(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    _TABLES_WITH_CREATED_AT = [
        "candles",
        "signals",
        "orders",
        "fills",
        "pnl_snapshots",
        "audit_events",
        "feature_vectors",
        "training_jobs",
        "model_registry",
        "job_queue",
    ]
    for table in _TABLES_WITH_CREATED_AT:
        if not table_exists(connection, table):
            continue
        if "created_at" not in get_table_columns(connection, table):
            continue
        connection.execute(
            f"ALTER TABLE {table} ALTER COLUMN created_at"
            f" TYPE TIMESTAMPTZ USING created_at::TIMESTAMPTZ;"
        )

def _migrate_feature_vectors_open_time_to_bigint(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "feature_vectors"):
        return
    if "open_time" not in get_table_columns(connection, "feature_vectors"):
        return
    connection.execute(
        "ALTER TABLE feature_vectors ALTER COLUMN open_time"
        " TYPE BIGINT USING open_time::BIGINT;"
    )

def _migrate_remaining_real_columns_to_numeric(connection: DBConnection) -> None:
    """Migrate REAL → NUMERIC(20,8) for risk_configs, portfolio_config, backtest_runs."""
    if get_backend_name(connection) != "postgres":
        return
    _COLS: dict[str, list[str]] = {
        "risk_configs": ["order_qty", "max_position_qty", "max_daily_loss"],
        "portfolio_config": ["total_capital", "max_strategy_allocation_pct", "max_total_exposure_pct"],
        "backtest_runs": [
            "initial_capital", "final_equity", "total_return_pct",
            "max_drawdown_pct", "sharpe_ratio", "win_rate_pct", "profit_factor",
        ],
    }
    for table, cols in _COLS.items():
        if not table_exists(connection, table):
            continue
        existing = get_table_columns(connection, table)
        for col in cols:
            if col not in existing:
                continue
            connection.execute(
                f"ALTER TABLE {table} ALTER COLUMN {col}"
                f" TYPE NUMERIC(20,8) USING {col}::NUMERIC;"
            )

def _add_fills_commission(connection: DBConnection) -> None:
    """Add commission, commission_asset, quote_qty, transact_time columns to fills table."""
    if not table_exists(connection, "fills"):
        return
    existing = get_table_columns(connection, "fills")
    if "commission" not in existing:
        connection.execute("ALTER TABLE fills ADD COLUMN commission REAL DEFAULT NULL;")
    if "commission_asset" not in existing:
        connection.execute("ALTER TABLE fills ADD COLUMN commission_asset TEXT DEFAULT NULL;")
    if "quote_qty" not in existing:
        connection.execute("ALTER TABLE fills ADD COLUMN quote_qty REAL DEFAULT NULL;")
    if "transact_time" not in existing:
        connection.execute("ALTER TABLE fills ADD COLUMN transact_time INTEGER DEFAULT NULL;")

# ── Tables: futures data (order book / aggtrade / premium / open interest / liquidation / candles)

def _create_order_book_snapshots(connection: DBConnection) -> None:
    """Create order_book_snapshots table for 1m order book collection."""
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS order_book_snapshots (
            {_auto_id_column_sql(backend)},
            symbol        TEXT NOT NULL,
            timestamp_ms  {_epoch_millis_column_sql(backend)} NOT NULL,
            bids_json     TEXT,
            asks_json     TEXT,
            ob_imbalance  NUMERIC(10,6),
            spread_pct    NUMERIC(10,8),
            mid_price     NUMERIC(20,8),
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_ob_snapshots_symbol_ts"
        " ON order_book_snapshots(symbol, timestamp_ms);"
    )

def _create_futures_order_book_snapshots(connection: DBConnection) -> None:
    """Create futures_order_book_snapshots table for perp order book collection."""
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_order_book_snapshots (
            {_auto_id_column_sql(backend)},
            symbol        TEXT NOT NULL,
            timestamp_ms  {epoch_t} NOT NULL,
            bids_json     TEXT,
            asks_json     TEXT,
            ob_imbalance  NUMERIC(10,6),
            ob_imbalance_mean NUMERIC(10,6),
            ob_imbalance_std  NUMERIC(10,6),
            ob_imbalance_min  NUMERIC(10,6),
            ob_imbalance_max  NUMERIC(10,6),
            spread_pct    NUMERIC(10,8),
            spread_pct_mean NUMERIC(10,8),
            spread_pct_max  NUMERIC(10,8),
            spread_bps    NUMERIC(10,4),
            spread_bps_mean NUMERIC(10,4),
            spread_bps_max  NUMERIC(10,4),
            mid_price     NUMERIC(20,8),
            mid_price_mean NUMERIC(20,8),
            mid_price_min  NUMERIC(20,8),
            mid_price_max  NUMERIC(20,8),
            mid_price_ret_1m NUMERIC(12,8),
            source        TEXT NOT NULL DEFAULT 'rest',
            sample_count  INTEGER NOT NULL DEFAULT 0,
            active_seconds INTEGER NOT NULL DEFAULT 0,
            coverage_ratio NUMERIC(10,6),
            first_event_ms {epoch_t},
            last_event_ms {epoch_t},
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_ob_snapshots_symbol_ts"
        " ON futures_order_book_snapshots(symbol, timestamp_ms);"
    )

def _add_futures_order_book_aggregate_columns(connection: DBConnection) -> None:
    if not table_exists(connection, "futures_order_book_snapshots"):
        return
    existing = get_table_columns(connection, "futures_order_book_snapshots")
    additions = [
        ("ob_imbalance_mean", "NUMERIC(10,6)"),
        ("ob_imbalance_std", "NUMERIC(10,6)"),
        ("ob_imbalance_min", "NUMERIC(10,6)"),
        ("ob_imbalance_max", "NUMERIC(10,6)"),
        ("spread_pct_mean", "NUMERIC(10,8)"),
        ("spread_pct_max", "NUMERIC(10,8)"),
        ("spread_bps", "NUMERIC(10,4)"),
        ("spread_bps_mean", "NUMERIC(10,4)"),
        ("spread_bps_max", "NUMERIC(10,4)"),
        ("mid_price_mean", "NUMERIC(20,8)"),
        ("mid_price_min", "NUMERIC(20,8)"),
        ("mid_price_max", "NUMERIC(20,8)"),
        ("mid_price_ret_1m", "NUMERIC(12,8)"),
        ("active_seconds", "INTEGER"),
        ("coverage_ratio", "NUMERIC(10,6)"),
        ("first_event_ms", _epoch_millis_column_sql(get_backend_name(connection))),
    ]
    for column, sql_type in additions:
        if column not in existing:
            connection.execute(f"ALTER TABLE futures_order_book_snapshots ADD COLUMN {column} {sql_type};")

def _add_futures_order_book_active_seconds(connection: DBConnection) -> None:
    if not table_exists(connection, "futures_order_book_snapshots"):
        return
    existing = get_table_columns(connection, "futures_order_book_snapshots")
    if "active_seconds" not in existing:
        connection.execute("ALTER TABLE futures_order_book_snapshots ADD COLUMN active_seconds INTEGER DEFAULT 0;")

def _backfill_futures_order_book_active_seconds(connection: DBConnection) -> None:
    if not table_exists(connection, "futures_order_book_snapshots"):
        return
    if get_backend_name(connection) != "postgres":
        connection.execute(
            """
            UPDATE futures_order_book_snapshots
            SET active_seconds = MIN(60, MAX(0, ROUND(COALESCE(coverage_ratio, 0) * 60)))
            WHERE COALESCE(active_seconds, 0) = 0
              AND COALESCE(coverage_ratio, 0) > 0
            """
        )
        return
    connection.execute(
        """
        UPDATE futures_order_book_snapshots
        SET active_seconds = LEAST(
            60,
            GREATEST(0, CAST(ROUND(COALESCE(coverage_ratio, 0) * 60) AS INTEGER))
        )
        WHERE COALESCE(active_seconds, 0) = 0
          AND COALESCE(coverage_ratio, 0) > 0
        """
    )

def _create_futures_aggtrade_minutes(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_aggtrade_minutes (
            {_auto_id_column_sql(backend)},
            symbol            TEXT NOT NULL,
            timestamp_ms      {epoch_t} NOT NULL,
            trade_count       INTEGER NOT NULL DEFAULT 0,
            taker_buy_count   INTEGER NOT NULL DEFAULT 0,
            taker_sell_count  INTEGER NOT NULL DEFAULT 0,
            qty_total         NUMERIC(20,8),
            qty_taker_buy     NUMERIC(20,8),
            qty_taker_sell    NUMERIC(20,8),
            quote_total       NUMERIC(24,8),
            quote_taker_buy   NUMERIC(24,8),
            quote_taker_sell  NUMERIC(24,8),
            price_open        NUMERIC(20,8),
            price_high        NUMERIC(20,8),
            price_low         NUMERIC(20,8),
            price_close       NUMERIC(20,8),
            vwap              NUMERIC(20,8),
            avg_trade_size    NUMERIC(20,8),
            first_trade_id    BIGINT,
            last_trade_id     BIGINT,
            first_event_ms    {epoch_t},
            last_event_ms     {epoch_t},
            active_seconds    INTEGER NOT NULL DEFAULT 0,
            coverage_ratio    NUMERIC(10,6),
            source            TEXT NOT NULL DEFAULT 'rest',
            created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_aggtrade_minutes_symbol_ts"
        " ON futures_aggtrade_minutes(symbol, timestamp_ms);"
    )

def _create_futures_premium_metrics(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_premium_metrics (
            {_auto_id_column_sql(backend)},
            symbol               TEXT NOT NULL,
            timestamp_ms         {epoch_t} NOT NULL,
            mark_price           NUMERIC(20,8),
            index_price          NUMERIC(20,8),
            estimated_settle_price NUMERIC(20,8),
            last_funding_rate    NUMERIC(16,10),
            next_funding_time_ms {epoch_t},
            mark_index_basis_pct NUMERIC(16,10),
            mark_index_spread_bps NUMERIC(16,6),
            source               TEXT NOT NULL DEFAULT 'rest',
            created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_premium_metrics_symbol_ts"
        " ON futures_premium_metrics(symbol, timestamp_ms);"
    )

def _create_futures_open_interest_metrics(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_open_interest_metrics (
            {_auto_id_column_sql(backend)},
            symbol               TEXT NOT NULL,
            timestamp_ms         {epoch_t} NOT NULL,
            open_interest        NUMERIC(24,8),
            open_interest_value  NUMERIC(24,8),
            oi_change_1m         NUMERIC(16,10),
            oi_change_pct_1m     NUMERIC(16,10),
            source               TEXT NOT NULL DEFAULT 'rest',
            created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_open_interest_metrics_symbol_ts"
        " ON futures_open_interest_metrics(symbol, timestamp_ms);"
    )

def _create_futures_liquidation_minutes(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_liquidation_minutes (
            {_auto_id_column_sql(backend)},
            symbol                TEXT NOT NULL,
            timestamp_ms          {epoch_t} NOT NULL,
            event_count           INTEGER NOT NULL DEFAULT 0,
            buy_count             INTEGER NOT NULL DEFAULT 0,
            sell_count            INTEGER NOT NULL DEFAULT 0,
            qty_total             NUMERIC(24,8),
            qty_buy               NUMERIC(24,8),
            qty_sell              NUMERIC(24,8),
            quote_total           NUMERIC(24,8),
            quote_buy             NUMERIC(24,8),
            quote_sell            NUMERIC(24,8),
            avg_price             NUMERIC(20,8),
            max_quote             NUMERIC(24,8),
            first_event_ms        {epoch_t},
            last_event_ms         {epoch_t},
            active_seconds        INTEGER NOT NULL DEFAULT 0,
            coverage_ratio        NUMERIC(10,6),
            source                TEXT NOT NULL DEFAULT 'ws',
            created_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp_ms)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_liquidation_minutes_symbol_ts"
        " ON futures_liquidation_minutes(symbol, timestamp_ms);"
    )

def _create_futures_candles_table(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    epoch_t = _epoch_millis_column_sql(backend)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS futures_candles (
            {_auto_id_column_sql(backend)},
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time {epoch_t} NOT NULL,
            open NUMERIC(20,8) NOT NULL,
            high NUMERIC(20,8) NOT NULL,
            low NUMERIC(20,8) NOT NULL,
            close NUMERIC(20,8) NOT NULL,
            volume NUMERIC(20,8) NOT NULL,
            close_time {epoch_t} NOT NULL,
            quote_asset_volume NUMERIC(20,8),
            number_of_trades INTEGER,
            taker_buy_base_volume NUMERIC(20,8),
            taker_buy_quote_volume NUMERIC(20,8),
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe, open_time)
        );
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_candles_symbol_tf_open_time"
        " ON futures_candles(symbol, timeframe, open_time);"
    )

def _widen_futures_open_interest_numeric_columns(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "futures_open_interest_metrics"):
        return
    existing = get_table_columns(connection, "futures_open_interest_metrics")
    if "oi_change_1m" in existing:
        connection.execute(
            "ALTER TABLE futures_open_interest_metrics "
            "ALTER COLUMN oi_change_1m TYPE NUMERIC(24,8) USING oi_change_1m::NUMERIC;"
        )
    if "oi_change_pct_1m" in existing:
        connection.execute(
            "ALTER TABLE futures_open_interest_metrics "
            "ALTER COLUMN oi_change_pct_1m TYPE NUMERIC(20,10) USING oi_change_pct_1m::NUMERIC;"
        )

def _widen_fills_transact_time(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    if not table_exists(connection, "fills"):
        return
    existing = get_table_columns(connection, "fills")
    if "transact_time" not in existing:
        return
    connection.execute(
        "ALTER TABLE fills ALTER COLUMN transact_time TYPE BIGINT USING transact_time::BIGINT;"
    )

def _add_training_jobs_progress(connection: DBConnection) -> None:
    """Add progress_json and job_type columns to training_jobs table."""
    if not table_exists(connection, "training_jobs"):
        return
    existing = get_table_columns(connection, "training_jobs")
    if "progress_json" not in existing:
        connection.execute("ALTER TABLE training_jobs ADD COLUMN progress_json TEXT DEFAULT NULL;")
    if "job_type" not in existing:
        connection.execute("ALTER TABLE training_jobs ADD COLUMN job_type TEXT DEFAULT 'supervised';")

def _add_retention_and_heartbeat_indexes(connection: DBConnection) -> None:
    """Add indexes to support efficient data retention queries."""
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_component"
        " ON runtime_heartbeats(component, last_seen_at);"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_events_source_created"
        " ON audit_events(source, created_at);"
    )

def _migrate_remaining_timestamps_to_timestamptz(connection: DBConnection) -> None:
    """Migrate updated_at / last_seen_at columns missed by migration 033."""
    if get_backend_name(connection) != "postgres":
        return
    targets = {
        "positions": "updated_at",
        "runtime_heartbeats": "last_seen_at",
        "daily_realized_pnl": "updated_at",
        "risk_configs": "updated_at",
        "portfolio_config": "updated_at",
    }
    for table, col in targets.items():
        if not table_exists(connection, table):
            continue
        if col not in get_table_columns(connection, table):
            continue
        connection.execute(
            f"ALTER TABLE {table} ALTER COLUMN {col}"
            f" TYPE TIMESTAMPTZ USING {col}::TIMESTAMPTZ;"
        )

def _add_missing_performance_indexes(connection: DBConnection) -> None:
    """Add indexes for common time-based and lookup queries."""
    indexes = [
        ("idx_candles_symbol_tf_open_time",
         "candles(symbol, timeframe, open_time)"),
        ("idx_orders_created_at",
         "orders(created_at)"),
        ("idx_fills_created_at",
         "fills(created_at)"),
        ("idx_audit_events_created_at",
         "audit_events(created_at)"),
        ("idx_audit_events_event_type",
         "audit_events(event_type, created_at)"),
    ]
    for name, definition in indexes:
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS {name} ON {definition};"
        )

# ── Timestamp normalization utilities ─────────────────────────────────────────────────────────

def _postgres_utc_text_default_sql() -> str:
    return "(to_char(timezone('UTC', now()), 'YYYY-MM-DD\"T\"HH24:MI:SS') || '+00:00')"

def _normalize_legacy_utc_timestamp_strings(connection: DBConnection) -> None:
    """Normalize legacy UTC strings to ISO 8601 UTC on operational tables.

    This preserves the instant in time and only standardizes representation,
    for example:
      2026-04-09 06:05:48        -> 2026-04-09T06:05:48+00:00
      2026-04-09 06:05:48.123456 -> 2026-04-09T06:05:48.123456+00:00
    """

    backend = get_backend_name(connection)
    for table_name, _key_columns, timestamp_columns in LEGACY_UTC_TIMESTAMP_TARGETS:
        if table_name not in STARTUP_LEGACY_UTC_TIMESTAMP_TABLES:
            continue
        if not table_exists(connection, table_name):
            continue
        existing_columns = set(get_table_columns(connection, table_name))
        present_timestamp_columns = [column for column in timestamp_columns if column in existing_columns]
        if not present_timestamp_columns:
            continue
        for column_name in present_timestamp_columns:
            if backend == "postgres":
                column_type = get_table_column_type(connection, table_name, column_name, backend=backend) or "text"
                if column_type not in POSTGRES_TEXT_TIMESTAMP_TYPES:
                    continue
                normalized_expression = f"REPLACE({column_name}::text, ' ', 'T')"
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column_name} = {normalized_expression}
                    WHERE {column_name} IS NOT NULL
                      AND {column_name}::text LIKE '____-__-__ __:__:%%+__:%%'
                      AND {column_name}::text NOT LIKE '%%T%%';
                    """
                )
                normalized_expression = f"REPLACE({column_name}::text, ' ', 'T') || ':00'"
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column_name} = {normalized_expression}
                    WHERE {column_name} IS NOT NULL
                      AND {column_name}::text LIKE '____-__-__ __:__:%%+00'
                      AND {column_name}::text NOT LIKE '%%T%%';
                    """
                )
                normalized_expression = f"REPLACE({column_name}::text, ' ', 'T') || '+00:00'"
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column_name} = {normalized_expression}
                    WHERE {column_name} IS NOT NULL
                      AND {column_name}::text LIKE '____-__-__ __:__:%%'
                      AND {column_name}::text NOT LIKE '%%T%%'
                      AND {column_name}::text NOT LIKE '%%+__:%%'
                      AND {column_name}::text NOT LIKE '%%+__'
                      AND {column_name}::text NOT LIKE '%%Z';
                    """
                )
            else:
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column_name} = REPLACE({column_name}, ' ', 'T') || '+00:00'
                    WHERE {column_name} IS NOT NULL
                      AND {column_name} LIKE '____-__-__ __:__:%'
                      AND {column_name} NOT LIKE '%T%'
                      AND {column_name} NOT LIKE '%+__:%'
                      AND {column_name} NOT LIKE '%+__'
                      AND {column_name} NOT LIKE '%Z';
                    """
                )

def normalize_legacy_utc_timestamp_strings_offline(
    connection: DBConnection,
    *,
    batch_size: int = 10_000,
    table_names: set[str] | None = None,
) -> dict[str, int]:
    """Normalize legacy UTC timestamps in batches without blocking app startup."""

    normalized_counts: dict[str, int] = {}
    backend = get_backend_name(connection)
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    for table_name, key_columns, timestamp_columns in LEGACY_UTC_TIMESTAMP_TARGETS:
        if table_names is not None and table_name not in table_names:
            continue
        if not table_exists(connection, table_name):
            continue
        existing_columns = set(get_table_columns(connection, table_name))
        present_timestamp_columns = [column for column in timestamp_columns if column in existing_columns]
        if not present_timestamp_columns:
            continue
        for column_name in present_timestamp_columns:
            updated_total = 0
            if backend == "postgres":
                column_type = get_table_column_type(connection, table_name, column_name, backend=backend) or "text"
                if column_type not in POSTGRES_TEXT_TIMESTAMP_TYPES:
                    continue
                key_select = ", ".join(key_columns)
                order_by = ", ".join(key_columns)
                join_clause = " AND ".join(f"target.{key} = batch.{key}" for key in key_columns)
                batch_limit = int(batch_size)
                normalized_with_colon_offset = "REPLACE(target.{column_name}::text, ' ', 'T')".format(
                    column_name=column_name
                )
                normalized_with_offset = "REPLACE(target.{column_name}::text, ' ', 'T') || ':00'".format(
                    column_name=column_name
                )
                normalized_without_offset = "REPLACE(target.{column_name}::text, ' ', 'T') || '+00:00'".format(
                    column_name=column_name
                )
                while True:
                    rows = connection.execute(
                        f"""
                        WITH batch AS (
                            SELECT {key_select}
                            FROM {table_name}
                            WHERE {column_name} IS NOT NULL
                              AND (
                                ({column_name}::text LIKE '____-__-__ __:__:%%+__:%%' AND {column_name}::text NOT LIKE '%%T%%')
                                OR
                                ({column_name}::text LIKE '____-__-__ __:__:%%+00' AND {column_name}::text NOT LIKE '%%T%%')
                                OR (
                                  {column_name}::text LIKE '____-__-__ __:__:%%'
                                  AND {column_name}::text NOT LIKE '%%T%%'
                                  AND {column_name}::text NOT LIKE '%%+__:%%'
                                  AND {column_name}::text NOT LIKE '%%+__'
                                  AND {column_name}::text NOT LIKE '%%Z'
                                )
                              )
                            ORDER BY {order_by}
                            LIMIT {batch_limit}
                        )
                        UPDATE {table_name} AS target
                        SET {column_name} = CASE
                            WHEN target.{column_name}::text LIKE '____-__-__ __:__:%%+__:%%'
                              AND target.{column_name}::text NOT LIKE '%%T%%'
                            THEN {normalized_with_colon_offset}
                            WHEN target.{column_name}::text LIKE '____-__-__ __:__:%%+00'
                              AND target.{column_name}::text NOT LIKE '%%T%%'
                            THEN {normalized_with_offset}
                            ELSE {normalized_without_offset}
                        END
                        FROM batch
                        WHERE {join_clause}
                        RETURNING 1;
                        """
                    ).fetchall()
                    updated = len(rows)
                    updated_total += updated
                    connection.commit()
                    if updated < batch_size:
                        break
            else:
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column_name} = REPLACE({column_name}, ' ', 'T') || '+00:00'
                    WHERE {column_name} IS NOT NULL
                      AND {column_name} LIKE '____-__-__ __:__:%'
                      AND {column_name} NOT LIKE '%T%'
                      AND {column_name} NOT LIKE '%+__:%'
                      AND {column_name} NOT LIKE '%+__'
                      AND {column_name} NOT LIKE '%Z';
                    """
                )
                connection.commit()
                count_row = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE {column_name} LIKE '____-__-__T__:%+00:00';
                    """
                ).fetchone()
                updated_total = int(count_row[0] or 0)
            if updated_total:
                normalized_counts[f"{table_name}.{column_name}"] = updated_total
    return normalized_counts

def _set_postgres_text_timestamp_defaults_to_utc_iso(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    default_sql = _postgres_utc_text_default_sql()
    for table_name, column_names in POSTGRES_TEXT_TIMESTAMP_DEFAULT_TABLES:
        if not table_exists(connection, table_name):
            continue
        existing_columns = get_table_columns(connection, table_name)
        for column_name in column_names:
            if column_name not in existing_columns:
                continue
            column_type = get_table_column_type(connection, table_name, column_name, backend="postgres")
            if column_type not in POSTGRES_TEXT_TIMESTAMP_TYPES:
                continue
            connection.execute(
                f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DEFAULT {default_sql};"
            )

# ── Ordered migration registry ────────────────────────────────────────────────────────────────

MIGRATIONS: list[Migration] = [
    ("001_create_candles_table", _create_candles_table),
    ("002_create_signals_table", _create_signals_table),
    ("003_create_risk_events_table", _create_risk_events_table),
    ("004_add_risk_events_signal_id", _add_risk_events_signal_id),
    ("005_create_orders_and_fills_tables", _create_orders_and_fills_tables),
    ("006_add_orders_risk_event_id", _add_orders_risk_event_id),
    ("007_create_positions_table", _create_positions_table),
    ("008_add_positions_realized_pnl", _add_positions_realized_pnl),
    ("009_create_pnl_snapshots_table", _create_pnl_snapshots_table),
    ("010_create_daily_realized_pnl_table", _create_daily_realized_pnl_table),
    ("011_create_audit_events_table", _create_audit_events_table),
    ("012_create_runtime_heartbeats_table", _create_runtime_heartbeats_table),
    ("013_alter_candles_epoch_columns_to_bigint", _alter_candles_epoch_columns_to_bigint),
    ("014_create_job_queue_table", _create_job_queue_table),
    ("015_add_job_queue_depends_on", _add_job_queue_depends_on),
    ("016_create_risk_configs_table", _create_risk_configs_table),
    ("017_create_portfolio_config_table", _create_portfolio_config_table),
    ("018_add_orders_broker_metadata", _add_orders_broker_metadata),
    ("019_add_performance_indexes", _add_performance_indexes),
    ("020_create_backtest_runs_table", _create_backtest_runs_table),
    ("021_add_backtest_runs_experiment_name", _add_backtest_runs_experiment_name),
    ("022_add_backtest_runs_tags_notes", _add_backtest_runs_tags_notes),
    ("023_add_backtest_runs_promoted_at", _add_backtest_runs_promoted_at),
    ("024_add_backtest_runs_wf_columns", _add_backtest_runs_wf_columns),
    ("025_add_backtest_runs_equity_curve", _add_backtest_runs_equity_curve),
    ("026_create_feature_vectors_table", _create_feature_vectors_table),
    ("027_create_training_jobs_table", _create_training_jobs_table),
    ("028_create_model_registry_table", _create_model_registry_table),
    ("029_add_training_jobs_job_type", _add_training_jobs_job_type),
    ("030_migrate_candles_columns_to_numeric", _migrate_candles_columns_to_numeric),
    ("031_migrate_financial_columns_to_numeric", _migrate_financial_columns_to_numeric),
    ("032_add_candles_symbol_timeframe_index", _add_candles_symbol_timeframe_index),
    ("033_migrate_timestamps_to_timestamptz", _migrate_timestamps_to_timestamptz),
    ("034_migrate_feature_vectors_open_time_to_bigint", _migrate_feature_vectors_open_time_to_bigint),
    ("035_migrate_remaining_timestamps_to_timestamptz", _migrate_remaining_timestamps_to_timestamptz),
    ("036_add_missing_performance_indexes", _add_missing_performance_indexes),
    ("037_migrate_remaining_real_columns_to_numeric", _migrate_remaining_real_columns_to_numeric),
    ("038_add_retention_and_heartbeat_indexes", _add_retention_and_heartbeat_indexes),
    ("039_add_fills_commission", _add_fills_commission),
    ("040_add_training_jobs_progress", _add_training_jobs_progress),
    ("041_create_order_book_snapshots", _create_order_book_snapshots),
    ("042_create_futures_order_book_snapshots", _create_futures_order_book_snapshots),
    ("043_add_futures_order_book_aggregate_columns", _add_futures_order_book_aggregate_columns),
    ("044_add_futures_order_book_active_seconds", _add_futures_order_book_active_seconds),
    ("045_backfill_futures_order_book_active_seconds", _backfill_futures_order_book_active_seconds),
    ("046_create_futures_aggtrade_minutes", _create_futures_aggtrade_minutes),
    ("047_create_futures_premium_metrics", _create_futures_premium_metrics),
    ("048_create_futures_open_interest_metrics", _create_futures_open_interest_metrics),
    ("049_create_futures_liquidation_minutes", _create_futures_liquidation_minutes),
    ("050_create_futures_candles_table", _create_futures_candles_table),
    ("051_widen_futures_open_interest_numeric_columns", _widen_futures_open_interest_numeric_columns),
    ("052_widen_fills_transact_time", _widen_fills_transact_time),
    ("053_ensure_postgres_training_model_identity_columns", _ensure_postgres_training_model_identity_columns),
    ("054_add_risk_events_signal_id_index", _add_risk_events_signal_id_index),
    ("055_add_risk_events_symbol_strategy_decision_index", _add_risk_events_symbol_strategy_decision_index),
    ("056_migrate_pnl_snapshots_to_numeric", _migrate_pnl_snapshots_to_numeric),
    ("057_normalize_legacy_utc_timestamp_strings", _normalize_legacy_utc_timestamp_strings),
    ("058_set_postgres_text_timestamp_defaults_to_utc_iso", _set_postgres_text_timestamp_defaults_to_utc_iso),
    ("059_add_risk_configs_stop_loss_pct", _add_risk_configs_stop_loss_pct),
    ("060_add_signals_lookup_index", _add_signals_lookup_index),
    ("061_add_fills_symbol_index", _add_fills_symbol_index),
    ("062_add_job_queue_batch_id_column", _add_job_queue_batch_id_column),
]

# ── Migration runner infrastructure ───────────────────────────────────────────────────────────

def _ensure_migration_table(connection: DBConnection) -> None:
    connection.execute(CREATE_SCHEMA_MIGRATIONS_TABLE_SQL)
    connection.commit()

def _get_applied_versions(connection: DBConnection) -> set[str]:
    rows = connection.execute("SELECT version FROM schema_migrations;").fetchall()
    return {str(row[0]) for row in rows}

def _acquire_migration_lock(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    connection.execute("SELECT pg_advisory_lock(?);", (POSTGRES_MIGRATION_LOCK_ID,))

def _release_migration_lock(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        return
    connection.execute("SELECT pg_advisory_unlock(?);", (POSTGRES_MIGRATION_LOCK_ID,))

def _record_applied_version(connection: DBConnection, version: str) -> None:
    if get_backend_name(connection) == "postgres":
        connection.execute(
            """
            INSERT INTO schema_migrations (version, applied_at)
            VALUES (?, ?)
            ON CONFLICT (version) DO NOTHING;
            """,
            (version, utc_now_iso()),
        )
        return
    connection.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?);",
        (version, utc_now_iso()),
    )

def run_migrations(connection: DBConnection) -> list[str]:
    active_error: Exception | None = None
    _acquire_migration_lock(connection)
    try:
        _ensure_migration_table(connection)
        applied_versions = _get_applied_versions(connection)
        newly_applied: list[str] = []

        for version, migration in MIGRATIONS:
            if version in applied_versions:
                continue
            migration(connection)
            _record_applied_version(connection, version)
            connection.commit()
            newly_applied.append(version)
            applied_versions.add(version)

        return newly_applied
    except Exception as exc:
        active_error = exc
        rollback = getattr(connection, "rollback", None)
        if callable(rollback):
            rollback()
        raise
    finally:
        try:
            _release_migration_lock(connection)
        except Exception:
            if active_error is None:
                raise
