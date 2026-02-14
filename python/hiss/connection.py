from __future__ import annotations
from .hiss_native import (
    native_connect,
    native_query,
    native_execute,
    native_execute_raw,
    native_execute_many,
    native_close,
)
from .record import Record
from .transaction import Transaction


class Connection:
    """Async connection to SQL Server — no run_in_executor, pure async bridge."""

    def __init__(self, conn_id: int):
        self._id = conn_id
        self._closed = False

    async def fetch(self, query: str, *args) -> list[Record]:
        if self._closed:
            raise RuntimeError("Connection is closed")
        result = await native_query(self._id, query, list(args))
        if result is None:
            return []
        col_names, values, row_count, col_count = result
        records = []
        for r in range(row_count):
            offset = r * col_count
            row_values = values[offset:offset + col_count]
            records.append(Record(col_names, row_values))
        return records

    async def fetchrow(self, query: str, *args) -> Record | None:
        rows = await self.fetch(query, *args)
        return rows[0] if rows else None

    async def fetchval(self, query: str, *args, column: int = 0):
        row = await self.fetchrow(query, *args)
        if row is None:
            return None
        return row[column]

    async def execute(self, query: str, *args) -> str:
        if self._closed:
            raise RuntimeError("Connection is closed")
        return await native_execute(self._id, query, list(args))

    async def executemany(self, query: str, args_list) -> None:
        if self._closed:
            raise RuntimeError("Connection is closed")
        await native_execute_many(self._id, query, [list(a) for a in args_list])

    def transaction(self) -> Transaction:
        return Transaction(self)

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await native_close(self._id)

    async def _run_raw(self, sql: str) -> None:
        if self._closed:
            raise RuntimeError("Connection is closed")
        await native_execute_raw(self._id, sql)

    @property
    def is_closed(self) -> bool:
        return self._closed


async def connect(dsn: str, **kwargs) -> Connection:
    conn_id = await native_connect(dsn)
    return Connection(conn_id)
