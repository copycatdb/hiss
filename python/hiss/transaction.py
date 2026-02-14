from __future__ import annotations


class Transaction:
    """Async transaction context manager."""

    def __init__(self, conn):
        self._conn = conn
        self._started = False
        self._finished = False

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Transaction already started")
        await self._conn._run_raw("BEGIN TRANSACTION")
        self._started = True

    async def commit(self) -> None:
        if not self._started or self._finished:
            raise RuntimeError("Transaction not active")
        self._finished = True
        await self._conn._run_raw("COMMIT TRANSACTION")

    async def rollback(self) -> None:
        if not self._started or self._finished:
            raise RuntimeError("Transaction not active")
        self._finished = True
        await self._conn._run_raw("ROLLBACK TRANSACTION")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._finished:
            return
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
