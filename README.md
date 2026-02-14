# hiss 🐍

Async Python driver for SQL Server. Like [asyncpg](https://github.com/MagicStack/asyncpg), but angrier.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A fast, async-native Python driver for SQL Server built on [tabby](https://github.com/copycatdb/tabby) (Rust TDS protocol) and PyO3. Real `async/await` that plays nice with asyncio. Direct TDS wire decode — no intermediate layers.

```python
import hiss
import asyncio

async def main():
    pool = await hiss.create_pool(
        "Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes",
        min_size=5, max_size=20
    )

    rows = await pool.fetch("SELECT * FROM users WHERE id = @p1", 42)
    print(rows[0]["name"])

    # The killer feature: Arrow-native fetch
    table = await pool.fetch_arrow("SELECT * FROM big_table")
    df = table.to_pandas()  # zero-copy

    await pool.close()

asyncio.run(main())
```

## Standing on the shoulders of giants

ODBC is the OG. It's been the universal database API since 1992 — connecting everything from mainframes to microservices, surviving every platform shift, every language trend, every "this will replace SQL" hype cycle. [pyodbc](https://github.com/mkleehammer/pyodbc) took that foundation and made it Pythonic, battle-tested across millions of production deployments. Respect.

hiss takes a different path — not because ODBC got it wrong, but because starting fresh lets us make different tradeoffs:

- **Native async** — built for `async/await` from day one, no thread pool wrappers
- **Zero system deps** — pure Rust TDS implementation, no driver manager or ODBC headers to install
- **Arrow-native** — `fetch_arrow()` for zero-copy analytics pipelines
- **Direct wire decode** — TDS bytes straight to Python objects, no intermediate representations

If pyodbc works for you, keep using it. It's rock solid. hiss is for when you want async-native, dependency-free, and don't mind riding something newer.

## Performance

hiss decodes TDS wire protocol directly into Python objects using tabby's `RowWriter` trait — no `SqlValue` enum, no chrono, no boxing. Combined with cached Python type constructors (a trick borrowed from pyodbc's own source), this makes hiss fast:

- **3x+ faster** on bulk fetches (100K+ rows)
- **4x+ faster** on datetime-heavy workloads
- **2x+ faster** on parameterized inserts
- **554/555** pyodbc-compatible tests passing

## Status

🚧 Early release. DB-API 2.0 compatible. Async pool API coming soon.

## Attribution

Inspired by [asyncpg](https://github.com/MagicStack/asyncpg) by MagicStack — they showed the world what a database driver *should* feel like. And by [pyodbc](https://github.com/mkleehammer/pyodbc), whose clean C source taught us half the optimization tricks in this codebase. We just... copied them both. For SQL Server. Like a cat.

## License

MIT
