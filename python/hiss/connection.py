from __future__ import annotations
import asyncio
from functools import partial
from .hiss_native import NativeConnection
from .record import Record
from .transaction import Transaction


class Connection:
    """Async connection to SQL Server with asyncpg-style API."""

    def __init__(self, native: NativeConnection):
        self._native = native
        self._closed = False

    async def fetch(self, query: str, *args) -> list[Record]:
        """Execute a query and return all rows as Records."""
        loop = asyncio.get_running_loop()
        params = list(args)
        result = await loop.run_in_executor(None, partial(self._native.query, query, params))
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
        """Execute a query and return the first row, or None."""
        rows = await self.fetch(query, *args)
        return rows[0] if rows else None

    async def fetchval(self, query: str, *args, column: int = 0):
        """Execute a query and return a single value from the first row."""
        row = await self.fetchrow(query, *args)
        if row is None:
            return None
        return row[column]

    async def execute(self, query: str, *args) -> str:
        """Execute a statement and return a status string."""
        loop = asyncio.get_running_loop()
        params = list(args)
        return await loop.run_in_executor(None, partial(self._native.execute, query, params))

    async def executemany(self, query: str, args_list) -> None:
        """Execute a statement for each set of parameters."""
        loop = asyncio.get_running_loop()
        params = [list(a) for a in args_list]
        await loop.run_in_executor(None, partial(self._native.execute_many, query, params))

    def transaction(self) -> Transaction:
        """Create a transaction context manager."""
        return Transaction(self)

    async def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self._closed = True
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._native.close)

    async def _run_raw(self, sql: str) -> None:
        """Execute raw SQL (for transaction control)."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, partial(self._native.execute_raw, sql))

    @property
    def is_closed(self) -> bool:
        return self._closed


async def connect(dsn: str, **kwargs) -> Connection:
    """Connect to SQL Server and return an async Connection."""
    loop = asyncio.get_running_loop()
    native = await loop.run_in_executor(None, partial(NativeConnection.connect, dsn))
    return Connection(native)
