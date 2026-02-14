import pytest_asyncio
import pytest
import asyncio
import hiss

DSN = "Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes;"


@pytest_asyncio.fixture
async def conn():
    c = await hiss.connect(DSN)
    yield c
    await c.close()


@pytest_asyncio.fixture
async def pool():
    p = await hiss.create_pool(DSN, min_size=2, max_size=5)
    yield p
    await p.close()


# 1. Basic connect/close

async def test_connect_close():
    c = await hiss.connect(DSN)
    assert not c.is_closed
    await c.close()
    assert c.is_closed


# 2. fetch, fetchrow, fetchval

async def test_fetch(conn):
    rows = await conn.fetch("SELECT 1 AS a, 2 AS b")
    assert len(rows) == 1
    assert rows[0]["a"] == 1
    assert rows[0]["b"] == 2



async def test_fetchrow(conn):
    row = await conn.fetchrow("SELECT 42 AS val")
    assert row is not None
    assert row["val"] == 42



async def test_fetchval(conn):
    val = await conn.fetchval("SELECT 99")
    assert val == 99



async def test_fetchrow_none(conn):
    row = await conn.fetchrow("SELECT 1 WHERE 1=0")
    assert row is None



async def test_fetchval_none(conn):
    val = await conn.fetchval("SELECT 1 WHERE 1=0")
    assert val is None


# 3. execute with params

async def test_params(conn):
    rows = await conn.fetch("SELECT @p1 AS x, @p2 AS y", 42, "hello")
    assert rows[0]["x"] == 42
    assert rows[0]["y"] == "hello"


# 4. executemany

async def test_executemany(conn):
    await conn.execute("IF OBJECT_ID('tempdb..#em_test') IS NOT NULL DROP TABLE #em_test")
    await conn.execute("CREATE TABLE #em_test (id INT, name NVARCHAR(50))")
    await conn.executemany(
        "INSERT INTO #em_test VALUES (@p1, @p2)",
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    )
    rows = await conn.fetch("SELECT * FROM #em_test ORDER BY id")
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[2]["name"] == "Charlie"


# 5. SQL Server types

async def test_types_int(conn):
    row = await conn.fetchrow("SELECT CAST(1 AS TINYINT) AS a, CAST(2 AS SMALLINT) AS b, CAST(3 AS INT) AS c, CAST(4 AS BIGINT) AS d")
    assert row["a"] == 1
    assert row["b"] == 2
    assert row["c"] == 3
    assert row["d"] == 4



async def test_types_float(conn):
    row = await conn.fetchrow("SELECT CAST(3.14 AS FLOAT) AS f, CAST(2.5 AS REAL) AS r")
    assert abs(row["f"] - 3.14) < 0.001
    assert abs(row["r"] - 2.5) < 0.001



async def test_types_decimal(conn):
    import decimal
    row = await conn.fetchrow("SELECT CAST(123.45 AS DECIMAL(10,2)) AS d")
    assert row["d"] == decimal.Decimal("123.45")



async def test_types_varchar(conn):
    row = await conn.fetchrow("SELECT CAST('hello' AS VARCHAR(50)) AS v, CAST(N'world' AS NVARCHAR(50)) AS nv")
    assert row["v"] == "hello"
    assert row["nv"] == "world"



async def test_types_bit(conn):
    row = await conn.fetchrow("SELECT CAST(1 AS BIT) AS b")
    assert row["b"] is True



async def test_types_datetime(conn):
    import datetime
    row = await conn.fetchrow("SELECT CAST('2024-01-15 10:30:00' AS DATETIME) AS dt")
    assert row["dt"].year == 2024
    assert row["dt"].month == 1
    assert row["dt"].day == 15



async def test_types_date(conn):
    import datetime
    row = await conn.fetchrow("SELECT CAST('2024-06-15' AS DATE) AS d")
    assert row["d"] == datetime.date(2024, 6, 15)



async def test_types_uniqueidentifier(conn):
    import uuid
    row = await conn.fetchrow("SELECT CAST('12345678-1234-1234-1234-123456789012' AS UNIQUEIDENTIFIER) AS u")
    assert row["u"] == uuid.UUID('12345678-1234-1234-1234-123456789012')



async def test_types_binary(conn):
    row = await conn.fetchrow("SELECT CAST(0x48454C4C4F AS VARBINARY(10)) AS b")
    assert row["b"] == b"HELLO"


# 6. Record access

async def test_record_access(conn):
    row = await conn.fetchrow("SELECT 1 AS id, N'Alice' AS name")
    # Dict access
    assert row["id"] == 1
    assert row["name"] == "Alice"
    # Index access
    assert row[0] == 1
    assert row[1] == "Alice"
    # keys/values/items
    assert row.keys() == ["id", "name"]
    assert row.values() == [1, "Alice"]
    assert row.items() == [("id", 1), ("name", "Alice")]
    assert len(row) == 2
    assert "id" in row


# 7. Transaction

async def test_transaction_commit(conn):
    await conn.execute("IF OBJECT_ID('tempdb..#tx_test') IS NOT NULL DROP TABLE #tx_test")
    await conn.execute("CREATE TABLE #tx_test (id INT)")
    async with conn.transaction():
        await conn.execute("INSERT INTO #tx_test VALUES (1)")
    rows = await conn.fetch("SELECT * FROM #tx_test")
    assert len(rows) == 1



async def test_transaction_rollback(conn):
    await conn.execute("IF OBJECT_ID('tempdb..#tx_test2') IS NOT NULL DROP TABLE #tx_test2")
    await conn.execute("CREATE TABLE #tx_test2 (id INT)")
    await conn.execute("INSERT INTO #tx_test2 VALUES (1)")
    try:
        async with conn.transaction():
            await conn.execute("INSERT INTO #tx_test2 VALUES (2)")
            raise ValueError("force rollback")
    except ValueError:
        pass
    rows = await conn.fetch("SELECT * FROM #tx_test2")
    assert len(rows) == 1  # only the first insert survived


# 8. Pool

async def test_pool_basic(pool):
    rows = await pool.fetch("SELECT 1 AS val")
    assert rows[0]["val"] == 1



async def test_pool_acquire_release(pool):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 42 AS x")
        assert row["x"] == 42



async def test_pool_sizing(pool):
    assert pool.get_size() >= 2
    assert pool.get_idle_size() >= 0


# 9. Pool concurrency

async def test_pool_concurrency(pool):
    async def query(i):
        return await pool.fetchval(f"SELECT {i}")
    results = await asyncio.gather(*[query(i) for i in range(10)])
    assert sorted(results) == list(range(10))


# 10. Error handling

async def test_bad_sql(conn):
    with pytest.raises(Exception):
        await conn.fetch("SELECT * FROM nonexistent_table_xyz")



async def test_bad_connection():
    with pytest.raises(Exception):
        await hiss.connect("Server=localhost,9999;UID=sa;PWD=wrong;TrustServerCertificate=yes;")


# 11. NULL handling

async def test_null(conn):
    row = await conn.fetchrow("SELECT NULL AS x, CAST(NULL AS INT) AS y, CAST(NULL AS VARCHAR(10)) AS z")
    assert row["x"] is None
    assert row["y"] is None
    assert row["z"] is None



async def test_null_param(conn):
    row = await conn.fetchrow("SELECT @p1 AS x", None)
    assert row["x"] is None


# 12. Large result sets

async def test_large_result_set(conn):
    rows = await conn.fetch("""
        ;WITH nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < 10000
        )
        SELECT n FROM nums OPTION (MAXRECURSION 10000)
    """)
    assert len(rows) == 10000
    assert rows[0]["n"] == 1
    assert rows[9999]["n"] == 10000


# 13. Unicode strings

async def test_unicode(conn):
    row = await conn.fetchrow("SELECT N'こんにちは世界' AS greeting, N'🎉🚀' AS emoji")
    assert row["greeting"] == "こんにちは世界"
    assert row["emoji"] == "🎉🚀"



async def test_unicode_param(conn):
    row = await conn.fetchrow("SELECT @p1 AS val", "café ñ 日本語")
    assert row["val"] == "café ñ 日本語"


# Execute returns status string

async def test_execute_status(conn):
    await conn.execute("IF OBJECT_ID('tempdb..#st_test') IS NOT NULL DROP TABLE #st_test")
    await conn.execute("CREATE TABLE #st_test (id INT)")
    status = await conn.execute("INSERT INTO #st_test VALUES (1)")
    assert "1" in status  # "1 row(s) affected"


# Connection used after close

async def test_connection_after_close():
    c = await hiss.connect(DSN)
    await c.close()
    with pytest.raises(RuntimeError, match="closed"):
        await c.fetch("SELECT 1")


# Concurrent queries on same connection (serialized via tokio mutex)

async def test_concurrent_same_conn(conn):
    async def q(i):
        return await conn.fetchval(f"SELECT {i}")
    # These will serialize on the connection's tokio mutex
    results = await asyncio.gather(*[q(i) for i in range(5)])
    assert sorted(results) == list(range(5))


# Pool exhaustion — more acquires than max_size should wait

async def test_pool_exhaustion():
    pool = await hiss.create_pool(DSN, min_size=1, max_size=2)
    acquired = []
    for _ in range(2):
        ctx = pool.acquire()
        conn = await ctx.__aenter__()
        acquired.append((ctx, conn))

    # Third acquire should block; use wait_for with timeout
    async def try_acquire():
        async with pool.acquire() as c:
            return await c.fetchval("SELECT 1")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(try_acquire(), timeout=0.3)

    # Release one, now it should work
    ctx0, conn0 = acquired.pop()
    await ctx0.__aexit__(None, None, None)
    val = await asyncio.wait_for(try_acquire(), timeout=2.0)
    assert val == 1

    # Cleanup
    for ctx, conn in acquired:
        await ctx.__aexit__(None, None, None)
    await pool.close()


# Pool close

async def test_pool_close():
    pool = await hiss.create_pool(DSN, min_size=2, max_size=3)
    assert pool.get_size() >= 2
    await pool.close()
    assert pool.get_size() == 0


# Empty result set

async def test_empty_result(conn):
    rows = await conn.fetch("SELECT 1 AS x WHERE 1=0")
    assert rows == []


# Datetime param round-trip

async def test_datetime_param(conn):
    import datetime
    dt = datetime.datetime(2024, 6, 15, 10, 30, 45, 123456)
    row = await conn.fetchrow("SELECT @p1 AS dt", dt)
    assert row["dt"].year == 2024
    assert row["dt"].month == 6
    assert row["dt"].hour == 10


# Date param round-trip

async def test_date_param(conn):
    import datetime
    d = datetime.date(2024, 12, 25)
    row = await conn.fetchrow("SELECT CAST(@p1 AS DATE) AS d", d)
    assert row["d"] == d


# UUID param round-trip

async def test_uuid_param(conn):
    import uuid
    u = uuid.UUID('12345678-1234-1234-1234-123456789012')
    row = await conn.fetchrow("SELECT CAST(@p1 AS UNIQUEIDENTIFIER) AS u", u)
    assert row["u"] == u


# Decimal param round-trip

async def test_decimal_param(conn):
    import decimal
    d = decimal.Decimal("99.99")
    row = await conn.fetchrow("SELECT CAST(@p1 AS DECIMAL(10,2)) AS d", d)
    assert row["d"] == d


# Bool param

async def test_bool_param(conn):
    row = await conn.fetchrow("SELECT @p1 AS b", True)
    assert row["b"] == 1


# Bytes param

async def test_bytes_param(conn):
    row = await conn.fetchrow("SELECT @p1 AS b", b"\x01\x02\x03")
    assert row["b"] == b"\x01\x02\x03"


# Pool concurrency — 20 concurrent queries

async def test_pool_high_concurrency():
    pool = await hiss.create_pool(DSN, min_size=3, max_size=10)
    async def q(i):
        return await pool.fetchval(f"SELECT {i}")
    results = await asyncio.gather(*[q(i) for i in range(20)])
    assert sorted(results) == list(range(20))
    await pool.close()


# Record .get() method

async def test_record_get(conn):
    row = await conn.fetchrow("SELECT 1 AS x")
    assert row.get("x") == 1
    assert row.get("missing", 42) == 42


# Time type

async def test_types_time(conn):
    import datetime
    row = await conn.fetchrow("SELECT CAST('10:30:45' AS TIME) AS t")
    assert row["t"].hour == 10
    assert row["t"].minute == 30
    assert row["t"].second == 45


# datetime2 type

async def test_types_datetime2(conn):
    row = await conn.fetchrow("SELECT CAST('2024-06-15 10:30:45.1234567' AS DATETIME2(7)) AS dt")
    assert row["dt"].year == 2024
    assert row["dt"].microsecond > 0
