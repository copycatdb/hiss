from __future__ import annotations
import asyncio
from .connection import Connection, connect as _connect


class _AcquireContext:
    """Context manager for pool.acquire()."""
    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    async def __aenter__(self) -> Connection:
        self._conn = await self._pool._acquire_one()
        return self._conn

    async def __aexit__(self, *exc):
        if self._conn is not None:
            await self._pool.release(self._conn)
            self._conn = None


class Pool:
    """Simple async connection pool."""

    def __init__(self, dsn: str, min_size: int, max_size: int, **kwargs):
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._kwargs = kwargs
        self._idle: list[Connection] = []
        self._in_use: set[Connection] = set()
        self._closed = False
        self._lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(max_size)

    async def _init(self):
        for _ in range(self._min_size):
            conn = await _connect(self._dsn, **self._kwargs)
            self._idle.append(conn)

    async def _acquire_one(self) -> Connection:
        await self._sem.acquire()
        async with self._lock:
            if self._idle:
                conn = self._idle.pop()
            else:
                conn = await _connect(self._dsn, **self._kwargs)
            self._in_use.add(conn)
            return conn

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self)

    async def release(self, conn: Connection) -> None:
        async with self._lock:
            self._in_use.discard(conn)
            if not conn.is_closed and not self._closed:
                self._idle.append(conn)
            self._sem.release()

    async def fetch(self, query: str, *args) -> list:
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args, column: int = 0):
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args, column=column)

    async def execute(self, query: str, *args) -> str:
        async with self.acquire() as conn:
            return await conn.execute(query, *args)

    async def close(self) -> None:
        self._closed = True
        async with self._lock:
            for conn in self._idle:
                await conn.close()
            self._idle.clear()
            for conn in list(self._in_use):
                await conn.close()
            self._in_use.clear()

    def get_size(self) -> int:
        return len(self._idle) + len(self._in_use)

    def get_idle_size(self) -> int:
        return len(self._idle)


async def create_pool(dsn: str, *, min_size: int = 5, max_size: int = 20, **kwargs) -> Pool:
    pool = Pool(dsn, min_size, max_size, **kwargs)
    await pool._init()
    return pool
