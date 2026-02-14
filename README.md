# hiss 🐍

**Async Python driver for SQL Server** — powered by [tabby](https://github.com/copycatdb/tabby) (Rust TDS) via PyO3/maturin.

Think **asyncpg, but for SQL Server**. Native async, no ODBC, no DB-API 2.0 ceremony.

> Looking for sync DB-API 2.0? See [whiskers](https://github.com/copycatdb/whiskers).

## Status

🟢 **Working** — connects, queries, pools, transactions all functional against SQL Server.

## Installation

```bash
pip install copycatdb-hiss
```

Or build from source:

```bash
pip install maturin
git clone https://github.com/copycatdb/hiss.git
cd hiss
maturin develop
```

## Quick Start

```python
import asyncio
import hiss

DSN = "Server=localhost,1433;Database=master;UID=sa;PWD=YourPassword;TrustServerCertificate=yes;"

async def main():
    # Single connection
    conn = await hiss.connect(DSN)

    # Fetch rows
    rows = await conn.fetch("SELECT id, name FROM users WHERE age > @p1", 25)
    for row in rows:
        print(row["id"], row["name"])  # dict-style access
        print(row[0], row[1])          # index access

    # Single row / single value
    row = await conn.fetchrow("SELECT * FROM users WHERE id = @p1", 42)
    count = await conn.fetchval("SELECT COUNT(*) FROM users")

    # Execute (INSERT/UPDATE/DELETE)
    status = await conn.execute("INSERT INTO users (name) VALUES (@p1)", "Alice")

    # Executemany
    await conn.executemany(
        "INSERT INTO users (name, age) VALUES (@p1, @p2)",
        [("Bob", 30), ("Charlie", 25)]
    )

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
    pool = await hiss.create_pool(DSN, min_size=5, max_size=20)

    # Direct pool queries (auto acquire/release)
    rows = await pool.fetch("SELECT * FROM users")
    val = await pool.fetchval("SELECT COUNT(*) FROM users")

    # Manual acquire
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO users (name) VALUES (@p1)", "Dave")

    # Concurrent queries
    import asyncio
    results = await asyncio.gather(
        pool.fetchval("SELECT 1"),
        pool.fetchval("SELECT 2"),
        pool.fetchval("SELECT 3"),
    )

    await pool.close()
```

## API

### `hiss.connect(dsn) -> Connection`
### `hiss.create_pool(dsn, *, min_size=5, max_size=20) -> Pool`

### Connection
| Method | Returns |
|--------|---------|
| `await conn.fetch(query, *args)` | `list[Record]` |
| `await conn.fetchrow(query, *args)` | `Record \| None` |
| `await conn.fetchval(query, *args)` | scalar value |
| `await conn.execute(query, *args)` | status string |
| `await conn.executemany(query, args_list)` | `None` |
| `conn.transaction()` | `Transaction` (async context manager) |
| `await conn.close()` | `None` |

### Pool
Same query methods as Connection, plus:
| Method | Returns |
|--------|---------|
| `pool.acquire()` | async context manager → `Connection` |
| `await pool.release(conn)` | `None` |
| `pool.get_size()` | `int` |
| `pool.get_idle_size()` | `int` |
| `await pool.close()` | `None` |

### Record
```python
row["column_name"]  # dict access
row[0]              # index access
row.keys()          # column names
row.values()        # values
row.items()         # (name, value) pairs
len(row)            # column count
```

### Parameters
Use SQL Server native `@p1, @p2, ...` positional parameters:
```python
await conn.fetch("SELECT * FROM t WHERE id = @p1 AND name = @p2", 42, "Alice")
```

## Architecture

```
Python asyncio          Rust (tokio)
─────────────          ─────────────
hiss.Connection  ──►  hiss_native (PyO3)
  run_in_executor       └─► tabby (TDS protocol)
                             └─► TCP + TLS → SQL Server
```

- **tabby**: Pure Rust TDS 7.4+ wire protocol
- **hiss_native**: PyO3 bridge with tokio runtime
- **hiss**: Python async wrapper using `run_in_executor`

## License

MIT
