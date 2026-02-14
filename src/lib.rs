use pyo3::prelude::*;
use pyo3::types::PyList;
use tabby::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use std::sync::{Arc, Mutex};

mod errors;
mod row_writer;
mod runtime;
mod types;

use errors::to_pyerr;
use row_writer::{ColumnInfo, CompactValue, MultiSetWriter, PyRowWriter};
use types::{compact_value_to_py, py_to_sql_literal};

type SharedClient = Arc<Mutex<Client<Compat<TcpStream>>>>;

fn parse_connection_string(conn_str: &str) -> (String, u16, String, String, String, bool) {
    let mut host = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = "master".to_string();
    let mut uid = String::new();
    let mut pwd = String::new();
    let mut trust_cert = false;

    for part in conn_str.split(';') {
        let part = part.trim();
        if part.is_empty() { continue; }
        if let Some(idx) = part.find('=') {
            let key = part[..idx].trim().to_lowercase();
            let val = part[idx + 1..].trim().to_string();
            match key.as_str() {
                "server" => {
                    if let Some(comma) = val.find(',') {
                        host = val[..comma].to_string();
                        if let Ok(p) = val[comma + 1..].trim().parse() { port = p; }
                    } else { host = val; }
                }
                "database" | "initial catalog" => database = val,
                "uid" | "user id" => uid = val,
                "pwd" | "password" => pwd = val,
                "trustservercertificate" => {
                    trust_cert = val.eq_ignore_ascii_case("yes") || val == "1" || val.eq_ignore_ascii_case("true")
                }
                _ => {}
            }
        }
    }
    (host, port, database, uid, pwd, trust_cert)
}

/// Internal connection object exposed to Python.
/// Python async wrappers call these methods via run_in_executor.
#[pyclass]
pub struct NativeConnection {
    client: Option<SharedClient>,
}

#[pymethods]
impl NativeConnection {
    /// Connect to SQL Server. Called from Python (blocking).
    #[staticmethod]
    fn connect(dsn: &str) -> PyResult<NativeConnection> {
        let (host, port, database, uid, pwd, trust_cert) = parse_connection_string(dsn);
        let client = Python::with_gil(|py| {
            py.allow_threads(|| {
                runtime::block_on(async {
                    let mut config = Config::new();
                    config.host(&host);
                    config.port(port);
                    config.database(&database);
                    config.authentication(AuthMethod::sql_server(&uid, &pwd));
                    if trust_cert { config.trust_cert(); }
                    config.encryption(EncryptionLevel::Required);
                    let tcp = TcpStream::connect(config.get_addr()).await
                        .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(format!("TCP connect failed: {}", e)))?;
                    tcp.set_nodelay(true).map_err(|e| pyo3::exceptions::PyConnectionError::new_err(format!("{}", e)))?;
                    let client = Client::connect(config, tcp.compat_write()).await
                        .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(format!("TDS connect failed: {}", e)))?;
                    Ok::<_, PyErr>(client)
                })
            })
        })?;
        Ok(NativeConnection { client: Some(Arc::new(Mutex::new(client))) })
    }

    /// Execute a query with parameter substitution. Returns (column_names, rows_as_flat_list, row_count, col_count).
    /// Each row is col_count consecutive PyObjects in the flat list.
    fn query(&self, py: Python<'_>, sql: String, params: Vec<PyObject>) -> PyResult<PyObject> {
        let client = self.get_client()?;
        let final_sql = if params.is_empty() {
            sql
        } else {
            self.substitute_params(py, &sql, &params)?
        };

        let result_sets: Vec<(Vec<ColumnInfo>, PyRowWriter)> = py.allow_threads(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let mut msw = MultiSetWriter::new();
                c.batch_into(final_sql, &mut msw).await.map_err(to_pyerr)?;
                drop(c);
                Ok::<_, PyErr>(msw.finalize())
            })
        })?;

        // Find first non-empty result set (skip __rowcount__ etc.)
        // Return as (col_names, values_list, row_count)
        for (cols, writer) in &result_sets {
            if cols.is_empty() { continue; }
            let col_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            let row_count = writer.row_count();
            let col_count = writer.col_count;
            let mut py_values: Vec<PyObject> = Vec::with_capacity(row_count * col_count);
            for r in 0..row_count {
                for c in 0..col_count {
                    py_values.push(compact_value_to_py(py, writer.get(r, c))?);
                }
            }
            let result = (col_names, py_values, row_count, col_count);
            return Ok(result.into_pyobject(py)?.into_any().unbind());
        }

        // No result set — return None (for execute-style queries)
        Ok(py.None())
    }

    /// Execute a statement that doesn't return rows. Returns affected row count string.
    fn execute(&self, py: Python<'_>, sql: String, params: Vec<PyObject>) -> PyResult<String> {
        let client = self.get_client()?;
        let final_sql = if params.is_empty() {
            sql
        } else {
            self.substitute_params(py, &sql, &params)?
        };

        // Append @@ROWCOUNT for DML
        let trimmed = final_sql.trim().to_uppercase();
        let needs_rowcount = trimmed.starts_with("INSERT ") || trimmed.starts_with("UPDATE ")
            || trimmed.starts_with("DELETE ") || trimmed.starts_with("MERGE ");

        let batch_sql = if needs_rowcount {
            format!("{}\nSELECT @@ROWCOUNT AS __rc__", final_sql)
        } else {
            final_sql
        };

        let rowcount = py.allow_threads(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let mut msw = MultiSetWriter::new();
                c.batch_into(batch_sql, &mut msw).await.map_err(to_pyerr)?;
                drop(c);
                let sets = msw.finalize();
                // Look for __rc__ result
                for (cols, writer) in &sets {
                    if cols.len() == 1 && cols[0].name == "__rc__" && writer.row_count() > 0 {
                        if let CompactValue::I64(v) = writer.get(0, 0) {
                            return Ok::<_, PyErr>(*v);
                        }
                    }
                }
                Ok(0i64)
            })
        })?;

        if needs_rowcount {
            Ok(format!("{} row(s) affected", rowcount))
        } else {
            Ok("OK".to_string())
        }
    }

    /// Execute a batch of statements (executemany).
    fn execute_many(&self, py: Python<'_>, sql: String, args_list: Vec<Vec<PyObject>>) -> PyResult<()> {
        let client = self.get_client()?;
        for params in &args_list {
            let final_sql = if params.is_empty() {
                sql.clone()
            } else {
                self.substitute_params(py, &sql, params)?
            };
            py.allow_threads(|| {
                runtime::block_on(async {
                    let mut c = client.lock().unwrap();
                    let mut msw = MultiSetWriter::new();
                    c.batch_into(&final_sql, &mut msw).await.map_err(to_pyerr)?;
                    Ok::<_, PyErr>(())
                })
            })?;
        }
        Ok(())
    }

    /// Execute raw SQL without results (for BEGIN/COMMIT/ROLLBACK).
    fn execute_raw(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        let client = self.get_client()?;
        py.allow_threads(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let mut msw = MultiSetWriter::new();
                c.batch_into(sql, &mut msw).await.map_err(to_pyerr)?;
                Ok(())
            })
        })
    }

    fn close(&mut self) -> PyResult<()> {
        self.client = None;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.client.is_none()
    }
}

impl NativeConnection {
    fn get_client(&self) -> PyResult<SharedClient> {
        self.client.clone().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Connection is closed"))
    }

    fn substitute_params(&self, py: Python<'_>, sql: &str, params: &[PyObject]) -> PyResult<String> {
        // Replace @p1, @p2, ... with literal values
        let mut result = sql.to_string();
        for (i, param) in params.iter().enumerate().rev() {
            let placeholder = format!("@p{}", i + 1);
            let bound = param.bind(py);
            let literal = py_to_sql_literal(py, bound)?;
            result = result.replace(&placeholder, &literal);
        }
        Ok(result)
    }
}

#[pymodule]
fn hiss_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeConnection>()?;
    Ok(())
}
