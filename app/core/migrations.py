from typing import Callable

from app.core.db import DBConnection
from app.core.db import get_backend_name
from app.core.db import get_table_columns
from app.core.db import table_exists


Migration = tuple[str, Callable[[DBConnection], None]]


CREATE_SCHEMA_MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

POSTGRES_MIGRATION_LOCK_ID = 8_455_771_239


def _auto_id_column_sql(backend: str) -> str:
    if backend == "postgres":
        return "id BIGSERIAL PRIMARY KEY"
    return "id INTEGER PRIMARY KEY"


def _epoch_millis_column_sql(backend: str) -> str:
    if backend == "postgres":
        return "BIGINT"
    return "INTEGER"


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
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS signals (
            {_auto_id_column_sql(backend)},
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
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS risk_events (
            {_auto_id_column_sql(backend)},
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
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pnl_snapshots (
            {_auto_id_column_sql(backend)},
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


def _create_audit_events_table(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS audit_events (
            {_auto_id_column_sql(backend)},
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
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS job_queue (
            {_auto_id_column_sql(backend)},
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


def _create_risk_configs_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_configs (
            strategy_name TEXT PRIMARY KEY,
            order_qty NUMERIC(20,8) NOT NULL,
            max_position_qty NUMERIC(20,8) NOT NULL,
            cooldown_seconds INTEGER NOT NULL,
            max_daily_loss NUMERIC(20,8) NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
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


def _create_backtest_runs_table(connection: DBConnection) -> None:
    backend = get_backend_name(connection)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS backtest_runs (
            {_auto_id_column_sql(backend)},
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
        """
        CREATE TABLE IF NOT EXISTS feature_vectors (
            id            INTEGER PRIMARY KEY,
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
        """
        CREATE TABLE IF NOT EXISTS training_jobs (
            id           INTEGER PRIMARY KEY,
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
        """
        CREATE TABLE IF NOT EXISTS model_registry (
            id              INTEGER PRIMARY KEY,
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


_CANDLES_NUMERIC_COLS = [
    "open", "high", "low", "close", "volume",
    "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume",
]


def _migrate_candles_columns_to_numeric(connection: DBConnection) -> None:
    if not table_exists(connection, "candles"):
        return
    backend = get_backend_name(connection)
    if backend == "postgres":
        for col in _CANDLES_NUMERIC_COLS:
            connection.execute(
                f"ALTER TABLE candles ALTER COLUMN {col} TYPE NUMERIC(20,8)"
                f" USING {col}::NUMERIC;"
            )
    else:
        # SQLite does not support ALTER COLUMN — rebuild the table
        connection.execute("ALTER TABLE candles RENAME TO candles_old;")
        connection.execute(
            f"""
            CREATE TABLE candles (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open NUMERIC(20,8) NOT NULL,
                high NUMERIC(20,8) NOT NULL,
                low NUMERIC(20,8) NOT NULL,
                close NUMERIC(20,8) NOT NULL,
                volume NUMERIC(20,8) NOT NULL,
                close_time INTEGER NOT NULL,
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
            """
            INSERT INTO candles
              (id, symbol, timeframe, open_time, open, high, low, close, volume,
               close_time, quote_asset_volume, number_of_trades,
               taker_buy_base_volume, taker_buy_quote_volume, created_at)
            SELECT
              id, symbol, timeframe, open_time,
              CAST(open AS REAL), CAST(high AS REAL),
              CAST(low AS REAL), CAST(close AS REAL), CAST(volume AS REAL),
              close_time,
              CAST(quote_asset_volume AS REAL),
              number_of_trades,
              CAST(taker_buy_base_volume AS REAL),
              CAST(taker_buy_quote_volume AS REAL),
              created_at
            FROM candles_old;
            """
        )
        connection.execute("DROP TABLE candles_old;")


def _migrate_financial_columns_to_numeric(connection: DBConnection) -> None:
    if get_backend_name(connection) != "postgres":
        # SQLite uses type affinity; NUMERIC affinity already works for numbers
        # stored as REAL. No rebuild needed.
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
]


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
            INSERT INTO schema_migrations (version)
            VALUES (?)
            ON CONFLICT (version) DO NOTHING;
            """,
            (version,),
        )
        return

    connection.execute("INSERT INTO schema_migrations (version) VALUES (?);", (version,))


def run_migrations(connection: DBConnection) -> list[str]:
    active_error: Optional[Exception] = None
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
