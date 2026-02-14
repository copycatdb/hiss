# hiss 🐍

A high-performance **async** Python driver for SQL Server, powered by [tabby](https://github.com/copycatdb/tabby) (Rust TDS). Clean `async/await` API inspired by [asyncpg](https://github.com/MagicStack/asyncpg).

Part of the [CopyCat](https://github.com/copycatdb) ecosystem. For sync DB-API 2.0, see [whiskers](https://github.com/copycatdb/whiskers).

## Why async?

Single-query speed is roughly the same — both hiss and whiskers use tabby for TDS. The difference is **concurrency**. When your web server handles 500 requests hitting SQL Server simultaneously, a sync driver blocks a thread per query. hiss runs them all on one thread.

## Benchmarks

All benchmarks run against SQL Server 2022 on localhost. Both hiss and whiskers use tabby for TDS wire protocol — the difference is purely in the async vs sync execution model.

Run them yourself: `python benchmarks/bench_hiss.py`

**Single query speed** — comparable, same engine underneath:

```
                               hiss    whiskers
100K rows (2 cols)            0.229s    0.195s
10K rows (10 cols)            0.062s    0.066s
50K datetimes (3 cols)        0.130s    0.138s
```

**Concurrent queries** — async overhead at low N, wins at scale:

```
    N    hiss(async)   whiskers(seq)    speedup
   10        0.064s          0.022s       0.3x
   50        0.104s          0.056s       0.5x
  100        0.113s          0.193s       1.7x
```

> At low concurrency, async scheduling overhead exceeds the benefit. At N=100+, the connection pool and event loop dominate.

**Web server simulation** — 500 requests, each fetching one row:

```
hiss (async pool):       0.151s  (3,317 req/s)
whiskers (sequential):   0.559s  (895 req/s)
speedup: 3.7x
```

**Mixed workload** — 50 fast reads + 20 medium reads + 10 slow queries (100ms each) + 20 writes:

```
hiss (async):          0.397s
whiskers (sequential): 1.284s
speedup: 3.2x
```

> Slow queries don't block fast ones. The 10 WAITFOR DELAY queries run concurrently instead of adding 1s sequentially.

**Bottom line**: For web servers, APIs, and any concurrent workload — hiss. For scripts and single-threaded batch work — whiskers.

## Installation

```bash
pip install copycatdb-hiss
```

## Quick Start

```python
import asyncio
import hiss

async def main():
    conn = await hiss.connect(
        "Server=localhost,1433;Database=mydb;UID=sa;PWD=secret;"
        "TrustServerCertificate=yes;"
    )

    # Fetch rows
    rows = await conn.fetch("SELECT @p1 AS name, @p2 AS age", "Alice", 30)
    for row in rows:
        print(row["name"], row["age"])  # dict-style access
        print(row[0], row[1])           # index access

    # Single row / value
    row = await conn.fetchrow("SELECT 42 AS answer")
    val = await conn.fetchval("SELECT COUNT(*) FROM users")

    # Execute
    await conn.execute("INSERT INTO users VALUES (@p1, @p2)", "Bob", 25)

    # Transactions — auto-commit on success, auto-rollback on exception
    async with conn.transaction():
        await conn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = @p1", 1)
        await conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = @p1", 2)

    await conn.close()

asyncio.run(main())
```

## Connection Pool

```python
async def main():
    pool = await hiss.create_pool(
        "Server=localhost;Database=mydb;UID=sa;PWD=secret;TrustServerCertificate=yes;",
        min_size=5,
        max_size=20,
    )

    # Query directly on the pool (auto acquire + release)
    rows = await pool.fetch("SELECT * FROM users")
    val = await pool.fetchval("SELECT COUNT(*) FROM users")

    # Acquire a connection for multiple operations
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO users VALUES (@p1)", "Alice")
            await conn.execute("INSERT INTO logs VALUES (@p1)", "added Alice")

    print(f"Pool: {pool.get_size()} total, {pool.get_idle_size()} idle")
    await pool.close()
```

## Architecture

```
Python: await pool.fetch("SELECT ...")
  │
  ├─ creates asyncio.Future
  ├─ submits task to tokio runtime (non-blocking, returns immediately)
  │   └─ Python event loop is FREE to run other coroutines
  │
  ▼ (background, on tokio thread)
  tabby: TDS wire protocol over TCP/TLS
  │
  ├─ sends TDS packet to SQL Server
  ├─ receives response, decodes rows
  ├─ converts to Python objects (brief GIL acquire)
  │
  ▼
  loop.call_soon_threadsafe(future.set_result, rows)
  │
  └─ Python: await resumes with rows
```

One tokio thread handles **all** connections. No thread-per-query. No `run_in_executor`.

## API Reference

**Connection**

| Method | Returns | Description |
|--------|---------|-------------|
| `await hiss.connect(dsn)` | `Connection` | Connect to SQL Server |
| `await conn.fetch(query, *args)` | `list[Record]` | Execute query, return all rows |
| `await conn.fetchrow(query, *args)` | `Record \| None` | Return first row |
| `await conn.fetchval(query, *args)` | `Any \| None` | Return first column of first row |
| `await conn.execute(query, *args)` | `str` | Execute statement, return status |
| `await conn.executemany(query, args_list)` | `None` | Execute for each parameter set |
| `conn.transaction()` | `Transaction` | Async context manager (commit/rollback) |
| `await conn.close()` | `None` | Close connection |

**Pool**

| Method | Returns | Description |
|--------|---------|-------------|
| `await hiss.create_pool(dsn, *, min_size, max_size)` | `Pool` | Create connection pool |
| `pool.acquire()` | `AsyncContextManager[Connection]` | Borrow a connection |
| `await pool.fetch/fetchrow/fetchval/execute(...)` | varies | Auto acquire + release |
| `pool.get_size()` / `pool.get_idle_size()` | `int` | Pool stats |
| `await pool.close()` | `None` | Close all connections |

**Record** — supports `row["col"]`, `row[0]`, `row.keys()`, `row.values()`, `row.items()`, `row.get(key)`, `len(row)`.

## Supported Types

int, bigint, float, real, decimal, varchar, nvarchar, char, nchar, text, ntext, bit, date, datetime, datetime2, smalldatetime, time, uniqueidentifier, binary, varbinary, image, NULL

## CopyCat Ecosystem

| Driver | Style | Use case |
|--------|-------|----------|
| **hiss** 🐍 | async / asyncpg-style | Web servers, APIs, high concurrency |
| [whiskers](https://github.com/copycatdb/whiskers) 🐈 | sync / DB-API 2.0 | Scripts, ETL, pyodbc replacement |
| [pounce](https://github.com/copycatdb/pounce) 🐾 | Arrow / ADBC | Analytics, DataFrames, zero-copy |
| [furball](https://github.com/copycatdb/furball) 🐱 | ODBC C API | Legacy ODBC app compatibility |

All powered by [tabby](https://github.com/copycatdb/tabby) 🐾 — a pure Rust TDS implementation.

## License

MIT
