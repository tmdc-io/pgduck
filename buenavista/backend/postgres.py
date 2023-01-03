import os
import re
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg2

from buenavista.adapter import Adapter, AdapterHandle, QueryResult
from buenavista.types import PGType


class PGQueryResult(QueryResult):
    def __init__(
        self, fields: List[Tuple[str, PGType]], rows: List[List[Optional[str]]]
    ):
        self.fields = fields
        self._rows = rows

    def has_results(self) -> bool:
        return bool(self.fields)

    def column_count(self):
        return len(self.fields)

    def column(self, index: int) -> Tuple[str, int]:
        field = self.fields[index]
        return (field[0], field[1].oid)

    def rows(self) -> Iterator[List[Optional[str]]]:
        def t(row):
            return [v if v is None else self.fields[i][1].converter(v) for i, v in enumerate(row)]

        return iter(map(t, self._rows))


class PGAdapterHandle(AdapterHandle):
    def execute_sql(self, sql: str, params=None) -> QueryResult:
        if params:
            sql = re.sub(r"\$\d+", r"%s", sql)
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)
        if self.cursor.description:
            rows = self.cursor.fetchall()
            return self.to_query_result(self.cursor.description, rows)
        else:
            return PGQueryResult([], [])

    def to_query_result(self, description, rows) -> QueryResult:
        fields = []
        for d in description:
            name, oid = d[0], d[1]
            f = (name, PGType.find_by_oid(oid))
            fields.append(f)
        return PGQueryResult(fields, rows)


class PGAdapter(Adapter):
    def __init__(self, **kwargs):
        super().__init__()
        self.db = psycopg2.connect(**kwargs)

    def new_handle(self) -> AdapterHandle:
        return PGAdapterHandle(self.db.cursor())

    def parameters(self) -> Dict[str, str]:
        return {"server_version": "BV.pg8000.1", "client_encoding": "UTF8"}


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer
    from buenavista.extensions.dbt import DbtPythonRunner

    address = ("localhost", 5433)
    server = BuenaVistaServer(
        address,
        PGAdapter(
            host="localhost", port=5432, user=os.getenv("USER"), database="postgres"
        ),
        extensions=[DbtPythonRunner()],
    )
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    server.serve_forever()