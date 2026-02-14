use parking_lot::Mutex;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tabby::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

mod errors;
mod row_writer;
mod types;

use errors::to_pyerr;
use row_writer::{CompactValue, MultiSetWriter};
use types::{compact_value_to_py, py_to_sql_literal};

type TdsClient = Client<Compat<TcpStream>>;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Bridge {
    rt: tokio::runtime::Runtime,
    connections: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<TdsClient>>>>,
}

static BRIDGE: std::sync::LazyLock<Bridge> = std::sync::LazyLock::new(|| Bridge {
    rt: tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime"),
    connections: Mutex::new(HashMap::new()),
});

fn parse_connection_string(conn_str: &str) -> (String, u16, String, String, String, bool) {
    let mut host = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = "master".to_string();
    let mut uid = String::new();
    let mut pwd = String::new();
    let mut trust_cert = false;

    for part in conn_str.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = part.find('=') {
            let key = part[..idx].trim().to_lowercase();
            let val = part[idx + 1..].trim().to_string();
            match key.as_str() {
                "server" => {
                    if let Some(comma) = val.find(',') {
                        host = val[..comma].to_string();
                        if let Ok(p) = val[comma + 1..].trim().parse() {
                            port = p;
                        }
                    } else {
                        host = val;
                    }
                }
                "database" | "initial catalog" => database = val,
                "uid" | "user id" => uid = val,
                "pwd" | "password" => pwd = val,
                "trustservercertificate" => {
                    trust_cert = val.eq_ignore_ascii_case("yes")
                        || val == "1"
                        || val.eq_ignore_ascii_case("true")
                }
                _ => {}
            }
        }
    }
    (host, port, database, uid, pwd, trust_cert)
}

/// Result from a query: column names + flat values + row_count + col_count
type QueryResult = Option<(Vec<String>, Vec<CompactValue>, usize, usize)>;

async fn do_connect(dsn: String) -> Result<u64, PyErr> {
    let (host, port, database, uid, pwd, trust_cert) = parse_connection_string(&dsn);
    let mut config = Config::new();
    config.host(&host);
    config.port(port);
    config.database(&database);
    config.authentication(AuthMethod::sql_server(&uid, &pwd));
    if trust_cert {
        config.trust_cert();
    }
    config.encryption(EncryptionLevel::Required);

    let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
        pyo3::exceptions::PyConnectionError::new_err(format!("TCP connect failed: {}", e))
    })?;
    tcp.set_nodelay(true)
        .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(format!("{}", e)))?;
    let client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| {
            pyo3::exceptions::PyConnectionError::new_err(format!("TDS connect failed: {}", e))
        })?;

    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    BRIDGE
        .connections
        .lock()
        .insert(id, Arc::new(tokio::sync::Mutex::new(client)));
    Ok(id)
}

fn get_conn(id: u64) -> PyResult<Arc<tokio::sync::Mutex<TdsClient>>> {
    BRIDGE
        .connections
        .lock()
        .get(&id)
        .cloned()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Connection is closed"))
}

async fn do_query(id: u64, sql: String) -> Result<QueryResult, PyErr> {
    let conn = get_conn(id)?;
    let mut c = conn.lock().await;
    let mut msw = MultiSetWriter::new();
    c.batch_into(&sql, &mut msw).await.map_err(to_pyerr)?;
    drop(c);
    let sets = msw.finalize();
    for (cols, writer) in &sets {
        if cols.is_empty() {
            continue;
        }
        let col_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        let row_count = writer.row_count();
        let col_count = writer.col_count;
        let mut values: Vec<CompactValue> = Vec::with_capacity(row_count * col_count);
        for r in 0..row_count {
            for c in 0..col_count {
                values.push(writer.get(r, c).clone());
            }
        }
        return Ok(Some((col_names, values, row_count, col_count)));
    }
    Ok(None)
}

async fn do_execute(id: u64, sql: String) -> Result<String, PyErr> {
    let conn = get_conn(id)?;
    let trimmed = sql.trim().to_uppercase();
    let needs_rowcount = trimmed.starts_with("INSERT ")
        || trimmed.starts_with("UPDATE ")
        || trimmed.starts_with("DELETE ")
        || trimmed.starts_with("MERGE ");

    let batch_sql = if needs_rowcount {
        format!("{}\nSELECT @@ROWCOUNT AS __rc__", sql)
    } else {
        sql
    };

    let mut c = conn.lock().await;
    let mut msw = MultiSetWriter::new();
    c.batch_into(&batch_sql, &mut msw).await.map_err(to_pyerr)?;
    drop(c);
    let sets = msw.finalize();

    let mut rowcount = 0i64;
    for (cols, writer) in &sets {
        if cols.len() == 1 && cols[0].name == "__rc__" && writer.row_count() > 0 {
            if let CompactValue::I64(v) = writer.get(0, 0) {
                rowcount = *v;
            }
        }
    }

    if needs_rowcount {
        Ok(format!("{} row(s) affected", rowcount))
    } else {
        Ok("OK".to_string())
    }
}

async fn do_execute_raw(id: u64, sql: String) -> Result<(), PyErr> {
    let conn = get_conn(id)?;
    let mut c = conn.lock().await;
    let mut msw = MultiSetWriter::new();
    c.batch_into(&sql, &mut msw).await.map_err(to_pyerr)?;
    Ok(())
}

async fn do_execute_many(id: u64, sqls: Vec<String>) -> Result<(), PyErr> {
    let conn = get_conn(id)?;
    for sql in sqls {
        let mut c = conn.lock().await;
        let mut msw = MultiSetWriter::new();
        c.batch_into(&sql, &mut msw).await.map_err(to_pyerr)?;
    }
    Ok(())
}

async fn do_close(id: u64) -> Result<(), PyErr> {
    BRIDGE.connections.lock().remove(&id);
    Ok(())
}

/// Helper: create an asyncio.Future, spawn work on tokio, resolve the future from tokio.
/// Returns the future to Python for awaiting.
fn spawn_future<'py, F, T, E>(
    py: Python<'py>,
    fut: F,
    convert: fn(Python<'_>, T) -> PyResult<PyObject>,
) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Into<PyErr> + Send + 'static,
{
    let asyncio = py.import("asyncio")?;
    let loop_ = asyncio.call_method0("get_running_loop")?;
    let future = loop_.call_method0("create_future")?;
    let future_ref = future.clone().unbind();
    let loop_ref = loop_.clone().unbind();

    BRIDGE.rt.spawn(async move {
        let result = fut.await;
        Python::with_gil(|py| {
            let loop_ = loop_ref.bind(py);
            let future = future_ref.bind(py);
            match result {
                Ok(val) => {
                    let py_val = match convert(py, val) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = loop_.call_method1(
                                "call_soon_threadsafe",
                                (future.getattr("set_exception").unwrap(), e),
                            );
                            return;
                        }
                    };
                    let _ = loop_.call_method1(
                        "call_soon_threadsafe",
                        (future.getattr("set_result").unwrap(), py_val),
                    );
                }
                Err(e) => {
                    let py_err: PyErr = e.into();
                    let _ = loop_.call_method1(
                        "call_soon_threadsafe",
                        (
                            future.getattr("set_exception").unwrap(),
                            py_err.into_value(py),
                        ),
                    );
                }
            }
        });
    });

    Ok(future)
}

fn substitute_params(py: Python<'_>, sql: &str, params: &[PyObject]) -> PyResult<String> {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("@p{}", i + 1);
        let bound = param.bind(py);
        let literal = py_to_sql_literal(py, bound)?;
        result = result.replace(&placeholder, &literal);
    }
    Ok(result)
}

fn convert_id(py: Python<'_>, id: u64) -> PyResult<PyObject> {
    Ok(id.into_pyobject(py)?.into_any().unbind())
}

fn convert_query_result(py: Python<'_>, result: QueryResult) -> PyResult<PyObject> {
    match result {
        None => Ok(py.None()),
        Some((col_names, values, row_count, col_count)) => {
            let mut py_values: Vec<PyObject> = Vec::with_capacity(values.len());
            for v in &values {
                py_values.push(compact_value_to_py(py, v)?);
            }
            let result = (col_names, py_values, row_count, col_count);
            Ok(result.into_pyobject(py)?.into_any().unbind())
        }
    }
}

fn convert_string(py: Python<'_>, s: String) -> PyResult<PyObject> {
    Ok(s.into_pyobject(py)?.into_any().unbind())
}

fn convert_unit(py: Python<'_>, _: ()) -> PyResult<PyObject> {
    Ok(py.None())
}

#[pyfunction]
fn native_connect<'py>(py: Python<'py>, dsn: String) -> PyResult<Bound<'py, PyAny>> {
    spawn_future(py, do_connect(dsn), convert_id)
}

#[pyfunction]
fn native_query<'py>(
    py: Python<'py>,
    conn_id: u64,
    sql: String,
    params: Vec<PyObject>,
) -> PyResult<Bound<'py, PyAny>> {
    let final_sql = if params.is_empty() {
        sql
    } else {
        substitute_params(py, &sql, &params)?
    };
    spawn_future(py, do_query(conn_id, final_sql), convert_query_result)
}

#[pyfunction]
fn native_execute<'py>(
    py: Python<'py>,
    conn_id: u64,
    sql: String,
    params: Vec<PyObject>,
) -> PyResult<Bound<'py, PyAny>> {
    let final_sql = if params.is_empty() {
        sql
    } else {
        substitute_params(py, &sql, &params)?
    };
    spawn_future(py, do_execute(conn_id, final_sql), convert_string)
}

#[pyfunction]
fn native_execute_raw<'py>(
    py: Python<'py>,
    conn_id: u64,
    sql: String,
) -> PyResult<Bound<'py, PyAny>> {
    spawn_future(py, do_execute_raw(conn_id, sql), convert_unit)
}

#[pyfunction]
fn native_execute_many<'py>(
    py: Python<'py>,
    conn_id: u64,
    sql: String,
    args_list: Vec<Vec<PyObject>>,
) -> PyResult<Bound<'py, PyAny>> {
    let sqls: Vec<String> = args_list
        .iter()
        .map(|params| {
            if params.is_empty() {
                Ok(sql.clone())
            } else {
                Python::with_gil(|py| substitute_params(py, &sql, params))
            }
        })
        .collect::<PyResult<_>>()?;
    spawn_future(py, do_execute_many(conn_id, sqls), convert_unit)
}

#[pyfunction]
fn native_close<'py>(py: Python<'py>, conn_id: u64) -> PyResult<Bound<'py, PyAny>> {
    spawn_future(py, do_close(conn_id), convert_unit)
}

#[pymodule]
fn hiss_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(native_connect, m)?)?;
    m.add_function(wrap_pyfunction!(native_query, m)?)?;
    m.add_function(wrap_pyfunction!(native_execute, m)?)?;
    m.add_function(wrap_pyfunction!(native_execute_raw, m)?)?;
    m.add_function(wrap_pyfunction!(native_execute_many, m)?)?;
    m.add_function(wrap_pyfunction!(native_close, m)?)?;
    Ok(())
}
