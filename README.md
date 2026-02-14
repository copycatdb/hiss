# hiss 🐍

A high-performance async Python driver for SQL Server, built on [tabby](https://github.com/copycatdb/tabby) (Rust TDS implementation) with an asyncpg-style API.

## Architecture

hiss uses a **true async bridge** — no `run_in_executor`, no thread pool per query. A single background tokio thread manages all SQL Server connections. Python async methods return `asyncio.Future` objects that are resolved via `loop.call_soon_threadsafe()`, giving you the concurrency of N queries on one thread.

## Installation

```bash
pip install copycatdb-hiss
```

## Quick Start

```python
import asyncio
import hiss

async def main():
    # Connect
    conn = await hiss.connect(
        "Server=localhost,1433;Database=master;UID=sa;PWD=secret;TrustServerCertificate=yes;"
    )

    # Query
    rows = await conn.fetch("SELECT @p1 AS name, @p2 AS age", "Alice", 30)
    for row in rows:
        print(row["name"], row["age"])

    # Single row / value
    row = await conn.fetchrow("SELECT 42 AS answer")
    val = await conn.fetchval("SELECT COUNT(*) FROM my_table")

    # Execute
    status = await conn.execute("INSERT INTO users VALUES (@p1, @p2)", "Bob", 25)

    # Transactions
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
        "Server=localhost,1433;Database=master;UID=sa;PWD=secret;TrustServerCertificate=yes;",
        min_size=5,
        max_size=20,
    )

    # Direct pool queries
    rows = await pool.fetch("SELECT * FROM users")
    val = await pool.fetchval("SELECT COUNT(*) FROM users")

    # Acquire for multiple operations
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO users VALUES (@p1)", "Alice")

    await pool.close()

asyncio.run(main())
```

## API

### Connection

| Method | Description |
|--------|-------------|
| `await connect(dsn)` | Connect to SQL Server |
| `await conn.fetch(query, *args)` | Execute query, return list of Records |
| `await conn.fetchrow(query, *args)` | Return first row or None |
| `await conn.fetchval(query, *args)` | Return first value or None |
| `await conn.execute(query, *args)` | Execute statement, return status string |
| `await conn.executemany(query, args_list)` | Execute for each parameter set |
| `conn.transaction()` | Create transaction context manager |
| `await conn.close()` | Close connection |

### Record

Records support both dict-style (`row["col"]`) and index (`row[0]`) access, plus `keys()`, `values()`, `items()`, `get()`, `len()`.

### Pool

| Method | Description |
|--------|-------------|
| `await create_pool(dsn, min_size=5, max_size=20)` | Create pool |
| `pool.acquire()` | Async context manager for a connection |
| `await pool.fetch/fetchrow/fetchval/execute(...)` | Direct pool queries |
| `pool.get_size()` / `pool.get_idle_size()` | Pool stats |
| `await pool.close()` | Close all connections |

## Supported Types

int, bigint, float, real, decimal, varchar, nvarchar, bit, date, datetime, datetime2, time, uniqueidentifier, binary, varbinary, NULL

## Tests

47 tests covering: connectivity, CRUD, types, parameters, records, transactions, pooling, concurrency, error handling, unicode, and large result sets.

```bash
pytest tests/test_hiss.py -q
# 47 passed
```

## License

MIT
