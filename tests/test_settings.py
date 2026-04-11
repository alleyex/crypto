import importlib


def test_database_backend_defaults_to_postgres_when_database_url_is_set(monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_DB_BACKEND", raising=False)
    monkeypatch.setenv("CRYPTO_DATABASE_URL", "postgresql://crypto:crypto@postgres:5432/crypto")

    import app.core.settings as settings

    importlib.reload(settings)
    try:
        assert settings.DB_BACKEND == "postgres"
    finally:
        importlib.reload(settings)


def test_database_backend_respects_explicit_sqlite_even_when_database_url_is_set(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_DB_BACKEND", "sqlite")
    monkeypatch.setenv("CRYPTO_DATABASE_URL", "postgresql://crypto:crypto@postgres:5432/crypto")

    import app.core.settings as settings

    importlib.reload(settings)
    try:
        assert settings.DB_BACKEND == "sqlite"
    finally:
        importlib.reload(settings)
