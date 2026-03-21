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
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS candles (
            {_auto_id_column_sql(backend)},
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time {_epoch_millis_column_sql(backend)} NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume TEXT NOT NULL,
            close_time {_epoch_millis_column_sql(backend)} NOT NULL,
            quote_asset_volume TEXT,
            number_of_trades INTEGER,
            taker_buy_base_volume TEXT,
            taker_buy_quote_volume TEXT,
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
            qty REAL NOT NULL,
            price REAL NOT NULL,
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
            qty REAL NOT NULL,
            price REAL NOT NULL,
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
            qty REAL NOT NULL,
            avg_price REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0,
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
            order_qty REAL NOT NULL,
            max_position_qty REAL NOT NULL,
            cooldown_seconds INTEGER NOT NULL,
            max_daily_loss REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _create_portfolio_config_table(connection: DBConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_config (
            id INTEGER PRIMARY KEY,
            total_capital REAL NOT NULL DEFAULT 0,
            max_strategy_allocation_pct REAL NOT NULL DEFAULT 0.5,
            max_total_exposure_pct REAL NOT NULL DEFAULT 0.8,
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
            initial_capital REAL,
            final_equity REAL,
            total_return_pct REAL,
            max_drawdown_pct REAL,
            sharpe_ratio REAL,
            win_rate_pct REAL,
            profit_factor REAL,
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
