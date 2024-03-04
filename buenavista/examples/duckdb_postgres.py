import os
import sys
from typing import Tuple

import duckdb

from buenavista.backends.duckdb import DuckDBConnection
from buenavista import bv_dialects, postgres, rewrite


class DuckDBPostgresRewriter(rewrite.Rewriter):
    def rewrite(self, sql: str) -> str:
        if sql.lower() == "select pg_catalog.version()":
            return "SELECT 'PostgreSQL 9.3' as version"
        else:
            return super().rewrite(sql)


rewriter = DuckDBPostgresRewriter(bv_dialects.BVPostgres(), bv_dialects.BVDuckDB())


def create(
        ddb: duckdb.DuckDBPyConnection, host_addr: Tuple[str, int], auth: dict = None
) -> postgres.BuenaVistaServer:
    bu_server = postgres.BuenaVistaServer(
        host_addr, DuckDBConnection(ddb), rewriter=rewriter, auth=auth
    )
    return bu_server


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
        # db = duckdb.connect()
    else:
        print("Using DuckDB database at %s" % sys.argv[1])
        db = duckdb.connect(sys.argv[1])
    db.execute(
        "SELECT * FROM duckdb_secrets()"
    ).fetchall()
    official_extensions = []
    local_extensions = []
    secret_sqls = []
    local_extensions_repo = ""
    if "OFFICIAL_DUCKDB_EXTENSIONS" in os.environ:
        official_extensions_str = os.environ["OFFICIAL_DUCKDB_EXTENSIONS"]
        print("official_extensions_str=" + official_extensions_str)
        official_extensions = official_extensions_str.split(",")

    if "LOCAL_DUCKDB_EXTENSIONS" in os.environ:
        local_extensions_str = os.environ["LOCAL_DUCKDB_EXTENSIONS"]
        print("local_extensions_str=" + local_extensions_str)
        local_extensions = local_extensions_str.split(",")

    if "LOCAL_DUCKDB_EXTENSION_REPO" in os.environ:
        local_extensions_repo = os.environ["LOCAL_DUCKDB_EXTENSION_REPO"]
        print("local_extensions_repo=" + local_extensions_repo)

    if "DUCKDB_SECRET_SQLS" in os.environ:
        secret_sql_str = os.environ["DUCKDB_SECRET_SQLS"]
        secret_sqls = secret_sql_str.split("||")

    # install officially available duck extensions
    for e in official_extensions:
        e = e.strip()
        if len(e) > 0:
            db.install_extension(e)
            db.load_extension(e)
    # install locally available duck extensions
    if len(local_extensions_repo) > 0:
        custom_repo_sql = "set custom_extension_repository = '{0}'".format(local_extensions_repo)
        print("custom_repo_sql=" + custom_repo_sql)
        db.sql(custom_repo_sql)
        for e in local_extensions:
            e = e.strip()
            if len(e) > 0:
                db.install_extension(e)
                db.load_extension(e)
        db.sql("reset custom_extension_repository")
    if len(secret_sqls) > 0:
        for s in secret_sqls:
            s = s.strip()
            if len(s) > 0:
                db.sql(s)

    s_host = "0.0.0.0"
    s_port = 5433

    if "PGDUCK_HOST" in os.environ:
        s_host = os.environ["PGDUCK_HOST"]

    if "PGDUCK_PORT" in os.environ:
        s_port = int(os.environ["PGDUCK_PORT"])

    address = (s_host, s_port)
    server = create(db, address)
    ip, port = server.server_address
    print(f"Listening on {ip}:{port}")

    try:
        server.serve_forever()
    except Exception as e:
        print(e)
    finally:
        server.shutdown()
        db.close()
