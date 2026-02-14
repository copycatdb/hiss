# hiss_native drop-in replacement for ddbc_bindings
# Re-exports everything from the Rust hiss_native module

from hiss.hiss_native import (
    Connection,
    StatementHandle,
    NumericData,
    ParamInfo,
    DDBCSQLExecute,
    DDBCSQLRowCount,
    DDBCSQLDescribeCol,
    DDBCSQLFetchOne,
    DDBCSQLFetchMany,
    DDBCSQLFetchAll,
    DDBCSQLMoreResults,
    DDBCSQLSetStmtAttr,
    DDBCSQLGetAllDiagRecords,
    DDBCSetDecimalSeparator,
    DDBCSQLTables,
    DDBCSQLColumns,
    DDBCSQLPrimaryKeys,
    DDBCSQLForeignKeys,
    DDBCSQLStatistics,
    DDBCSQLProcedures,
    DDBCSQLSpecialColumns,
    DDBCSQLGetTypeInfo,
    DDBCSQLFetchScroll,
    SQLExecuteMany,
)

import platform

def normalize_architecture(platform_name, architecture):
    """Stub - not needed for hiss_native but kept for compatibility."""
    return architecture

class _ErrorInfo:
    def __init__(self):
        self.sqlState = ""
        self.ddbcErrorMsg = ""

def DDBCSQLCheckError(handle_type, handle, ret):
    """Stub - hiss_native raises Python exceptions directly."""
    info = _ErrorInfo()
    info.sqlState = "HY000"
    info.ddbcErrorMsg = f"Error code: {ret}"
    return info

import threading as _threading
import time as _time
import collections as _collections

class _ConnectionPool:
    """Simple connection pool for hiss_native connections."""
    def __init__(self):
        self._lock = _threading.Lock()
        self._pools = _collections.defaultdict(list)
        self._enabled = False
        self._max_size = 100
        self._idle_timeout = 600

    def enable(self, max_size=100, idle_timeout=600):
        self._enabled = True
        self._max_size = max_size
        self._idle_timeout = idle_timeout

    def disable(self):
        self._enabled = False
        with self._lock:
            for conn_str, conns in self._pools.items():
                for conn, _ in conns:
                    try:
                        conn.close()
                    except Exception:
                        pass
            self._pools.clear()

    def get(self, conn_str):
        if not self._enabled:
            return None
        with self._lock:
            pool = self._pools.get(conn_str, [])
            now = _time.monotonic()
            while pool:
                conn, returned_at = pool.pop()
                if now - returned_at < self._idle_timeout:
                    try:
                        conn.alloc_statement_handle().free()
                        return conn
                    except Exception:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue
                try:
                    conn.close()
                except Exception:
                    pass
            return None

    def put(self, conn_str, conn):
        if not self._enabled:
            conn.close()
            return
        with self._lock:
            pool = self._pools[conn_str]
            if len(pool) < self._max_size:
                pool.append((conn, _time.monotonic()))
            else:
                conn.close()

_pool = _ConnectionPool()

def enable_pooling(max_size=100, idle_timeout=600):
    _pool.enable(max_size, idle_timeout)

def close_pooling():
    _pool.disable()

def GetDriverPathCpp(module_dir):
    return ""
