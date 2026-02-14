"""
hiss benchmarks — async vs sync performance comparison.

Compares hiss (async, tokio bridge) vs whiskers (sync, DB-API 2.0).
Both use tabby under the hood for TDS wire protocol.

Usage:
    pip install copycatdb-hiss copycatdb-whiskers
    python benchmarks/bench_hiss.py
"""

import asyncio
import time
import statistics
import sys

DSN = "Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes;"
RUNS = 3


def avg(times):
    return statistics.mean(times)


def fmt(t):
    return f"{t:.3f}s"


async def main():
    import hiss
    from hiss.pool import Pool
    from whiskers import connect as wconnect

    # ── Benchmark 1: Single Query Speed ──────────────────────────────
    print("=" * 60)
    print("Benchmark 1: Single Query Speed")
    print("=" * 60)
    print(f"{'':30s} {'hiss':>10s} {'whiskers':>10s}")
    print("-" * 52)

    conn = await hiss.connect(DSN)
    wc = wconnect(DSN)
    wcur = wc.cursor()

    tests = [
        (
            "100K rows (2 cols)",
            "SELECT TOP 100000 a.number, CAST(a.number AS VARCHAR(20)) "
            "FROM master..spt_values a CROSS JOIN master..spt_values b",
        ),
        (
            "10K rows (10 cols)",
            "SELECT TOP 10000 a.number,a.number+1,a.number+2,a.number+3,"
            "a.number+4,CAST(a.number AS VARCHAR(20)),"
            "CAST(a.number+1 AS VARCHAR(20)),"
            "CAST(a.number+2 AS VARCHAR(20)),GETDATE(),NEWID() "
            "FROM master..spt_values a CROSS JOIN master..spt_values b",
        ),
        (
            "50K datetimes (3 cols)",
            "SELECT TOP 50000 GETDATE(),GETDATE(),GETDATE() "
            "FROM master..spt_values a CROSS JOIN master..spt_values b",
        ),
    ]

    for name, q in tests:
        ht = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            await conn.fetch(q)
            ht.append(time.perf_counter() - t0)
        wt = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            wcur.execute(q)
            wcur.fetchall()
            wt.append(time.perf_counter() - t0)
        print(f"{name:30s} {fmt(avg(ht)):>10s} {fmt(avg(wt)):>10s}")

    await conn.close()
    wc.close()

    print()
    print("Both use tabby for TDS — single-query speed is comparable.")

    # ── Benchmark 2: Concurrent Queries ──────────────────────────────
    print()
    print("=" * 60)
    print("Benchmark 2: Concurrent Queries")
    print("=" * 60)
    print(
        f"{'N':>5s} {'hiss(async)':>14s} {'whiskers(seq)':>14s} {'speedup':>10s}"
    )
    print("-" * 45)

    for N in [10, 50, 100, 200]:
        pool = Pool(DSN, min_size=5, max_size=10)
        ht = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            await asyncio.gather(
                *[
                    pool.fetch("SELECT TOP 100 * FROM master..spt_values")
                    for _ in range(N)
                ]
            )
            ht.append(time.perf_counter() - t0)
        await pool.close()

        wt = []
        for _ in range(RUNS):
            wc2 = wconnect(DSN)
            wcur2 = wc2.cursor()
            t0 = time.perf_counter()
            for _ in range(N):
                wcur2.execute("SELECT TOP 100 * FROM master..spt_values")
                wcur2.fetchall()
            wt.append(time.perf_counter() - t0)
            wc2.close()

        h, w = avg(ht), avg(wt)
        ratio = f"{w / h:.1f}x" if h > 0 else "—"
        print(f"{N:5d} {fmt(h):>14s} {fmt(w):>14s} {ratio:>10s}")

    # ── Benchmark 3: Web Server Simulation ───────────────────────────
    print()
    print("=" * 60)
    print("Benchmark 3: Web Server Simulation (500 requests)")
    print("=" * 60)

    pool = Pool(DSN, min_size=5, max_size=10)
    ht = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        await asyncio.gather(
            *[
                pool.fetchrow("SELECT @p1 as id, GETDATE() as ts", i)
                for i in range(500)
            ]
        )
        ht.append(time.perf_counter() - t0)
    await pool.close()

    wt = []
    for _ in range(RUNS):
        wc3 = wconnect(DSN)
        wcur3 = wc3.cursor()
        t0 = time.perf_counter()
        for i in range(500):
            wcur3.execute("SELECT ? as id, GETDATE() as ts", i)
            wcur3.fetchone()
        wt.append(time.perf_counter() - t0)
        wc3.close()

    h, w = avg(ht), avg(wt)
    print(f"hiss (async pool):       {fmt(h)}  ({500 / h:.0f} req/s)")
    print(f"whiskers (sequential):   {fmt(w)}  ({500 / w:.0f} req/s)")
    print(f"speedup: {w / h:.1f}x")

    # ── Benchmark 4: Mixed Workload ──────────────────────────────────
    print()
    print("=" * 60)
    print("Benchmark 4: Mixed Workload (100 concurrent tasks)")
    print("=" * 60)
    print("50 fast reads + 20 medium reads + 10 slow (100ms) + 20 writes")
    print()

    # Setup
    conn = await hiss.connect(DSN)
    await conn.execute(
        "IF OBJECT_ID('dbo.hiss_bench_tmp') IS NOT NULL DROP TABLE dbo.hiss_bench_tmp"
    )
    await conn.execute("CREATE TABLE dbo.hiss_bench_tmp (id INT IDENTITY, val INT)")
    await conn.execute("INSERT INTO dbo.hiss_bench_tmp (val) VALUES (1)")
    await conn.close()

    async def hiss_mixed(pool):
        tasks = []
        for _ in range(50):
            tasks.append(pool.fetch("SELECT 1"))
        for _ in range(20):
            tasks.append(pool.fetch("SELECT TOP 1000 * FROM master..spt_values"))
        for _ in range(10):
            tasks.append(pool.fetch("WAITFOR DELAY '00:00:00.100'; SELECT 1"))
        for _ in range(20):
            tasks.append(
                pool.execute(
                    "INSERT INTO dbo.hiss_bench_tmp (val) VALUES (1); "
                    "DELETE TOP(1) FROM dbo.hiss_bench_tmp"
                )
            )
        await asyncio.gather(*tasks)

    ht = []
    for _ in range(RUNS):
        pool = Pool(DSN, min_size=5, max_size=10)
        t0 = time.perf_counter()
        await hiss_mixed(pool)
        ht.append(time.perf_counter() - t0)
        await pool.close()

    def whiskers_seq():
        wc4 = wconnect(DSN)
        wcur4 = wc4.cursor()
        for _ in range(50):
            wcur4.execute("SELECT 1")
            wcur4.fetchall()
        for _ in range(20):
            wcur4.execute("SELECT TOP 1000 * FROM master..spt_values")
            wcur4.fetchall()
        for _ in range(10):
            wcur4.execute("WAITFOR DELAY '00:00:00.100'; SELECT 1")
            wcur4.fetchall()
        for _ in range(20):
            wcur4.execute(
                "INSERT INTO dbo.hiss_bench_tmp (val) VALUES (1); "
                "DELETE TOP(1) FROM dbo.hiss_bench_tmp"
            )
        wc4.close()

    wt = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        whiskers_seq()
        wt.append(time.perf_counter() - t0)

    h, w = avg(ht), avg(wt)
    print(f"hiss (async):          {fmt(h)}")
    print(f"whiskers (sequential): {fmt(w)}")
    print(f"speedup: {w / h:.1f}x")

    # Cleanup
    conn = await hiss.connect(DSN)
    await conn.execute(
        "IF OBJECT_ID('dbo.hiss_bench_tmp') IS NOT NULL DROP TABLE dbo.hiss_bench_tmp"
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
