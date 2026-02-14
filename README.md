# hiss 🐍

Async Python driver for SQL Server. Like [asyncpg](https://github.com/MagicStack/asyncpg), but angrier.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A fast, async-native Python driver for SQL Server built on [tabby](https://github.com/copycatdb/tabby) (Rust TDS protocol) and PyO3. No ODBC. No GIL contention during I/O. Real `async/await` that plays nice with asyncio.

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

## Why not pyodbc?

| | pyodbc | hiss |
|---|---|---|
| Dependencies | ODBC Driver Manager + ODBC Driver (~50MB) | None. Pure Rust. |
| Async | Thread pool wrapper (fake async) | Native async (real async) |
| GIL | Held during ODBC calls | Released during I/O |
| Arrow support | Nope | Zero-copy `fetch_arrow()` |
| Install | `pip install pyodbc` + pray ODBC headers exist | `pip install hiss` |

## Status

🚧 Coming soon. tabby is ready, hiss is next.

## Attribution

Inspired by [asyncpg](https://github.com/MagicStack/asyncpg) by MagicStack. They showed the world what a database driver *should* feel like. We just... copied it. For SQL Server. Like a cat.

## License

MIT
