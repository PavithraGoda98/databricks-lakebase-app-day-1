"""Lakebase connection helper for a Databricks App."""

import os
from contextlib import contextmanager

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

_w = WorkspaceClient()
_ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")

if not _ENDPOINT_NAME:
    raise RuntimeError(
        "ENDPOINT_NAME is not set. Add to app.yaml:\n"
        '  - name: ENDPOINT_NAME\n'
        '    valueFrom: database'
    )


def _connection_kwargs() -> dict:
    """Build Lakebase connection parameters using the app resource and OAuth."""
    credential = _w.postgres.generate_database_credential(
        endpoint=_ENDPOINT_NAME
    )
    return {
        "host": os.environ["PGHOST"],
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ["PGDATABASE"],
        "user": os.environ["PGUSER"],
        "password": credential.token,
        "sslmode": os.environ.get("PGSSLMODE", "require"),
        "cursor_factory": RealDictCursor,
    }


def _new_connection():
    """Create a new Lakebase connection with a fresh OAuth credential."""
    return psycopg2.connect(**_connection_kwargs())


@contextmanager
def get_connection():
    """Yield a raw psycopg2 connection with a RealDictCursor factory."""
    conn = _new_connection()
    try:
        yield conn
    finally:
        conn.close()


def get_engine():
    """Return a SQLAlchemy engine that creates OAuth-authenticated connections."""
    return create_engine("postgresql+psycopg2://", creator=_new_connection)


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
