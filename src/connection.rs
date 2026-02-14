use pyo3::prelude::*;
use pyo3::types::PyDict;
use tabby::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::cursor::{SharedTxState, TdsCursor, TransactionState};
use crate::errors::to_pyerr;
use crate::runtime;
use std::sync::{Arc, Mutex};

pub type SharedClient = Arc<Mutex<Client<Compat<TcpStream>>>>;

pub struct TdsConnection {
    client: Option<SharedClient>,
    tx_state: SharedTxState,
}

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

impl TdsConnection {
    pub fn new(connection_str: &str, _attrs_before: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let (host, port, database, uid, pwd, trust_cert) = parse_connection_string(connection_str);

        // Check for required connection string parameters
        let has_server = connection_str.split(';').any(|part| {
            if let Some(idx) = part.find('=') {
                let key = part[..idx].trim().to_lowercase();
                key == "server"
            } else {
                false
            }
        });
        if !has_server {
            return Err(pyo3::exceptions::PyConnectionError::new_err(
                "Neither DSN nor SERVER keyword supplied",
            ));
        }

        let client = Python::with_gil(|py| {
            py.allow_threads(|| {
                runtime::block_on(async {
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
                        pyo3::exceptions::PyConnectionError::new_err(format!(
                            "TCP connect failed: {}",
                            e
                        ))
                    })?;
                    tcp.set_nodelay(true).map_err(|e| {
                        pyo3::exceptions::PyConnectionError::new_err(format!(
                            "set_nodelay failed: {}",
                            e
                        ))
                    })?;

                    let client =
                        Client::connect(config, tcp.compat_write())
                            .await
                            .map_err(|e| {
                                pyo3::exceptions::PyConnectionError::new_err(format!(
                                    "TDS connect failed: {}",
                                    e
                                ))
                            })?;

                    Ok::<_, PyErr>(client)
                })
            })
        })?;

        Ok(TdsConnection {
            client: Some(Arc::new(Mutex::new(client))),
            tx_state: Arc::new(Mutex::new(TransactionState {
                autocommit: false,
                in_transaction: false,
            })),
        })
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.client = None;
        Ok(())
    }

    fn get_client(&self) -> PyResult<SharedClient> {
        self.client
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Connection is closed"))
    }

    fn exec_simple(&self, sql: &str) -> PyResult<()> {
        let client = self.get_client()?;
        let sql = sql.to_string();
        Python::with_gil(|py| {
            py.allow_threads(|| {
                runtime::block_on(async {
                    let mut c = client.lock().unwrap();
                    c.execute_raw(sql)
                        .await
                        .map_err(to_pyerr)?
                        .into_results()
                        .await
                        .map_err(to_pyerr)?;
                    Ok(())
                })
            })
        })
    }

    pub fn commit(&mut self) -> PyResult<()> {
        let mut state = self.tx_state.lock().unwrap();
        if state.in_transaction {
            state.in_transaction = false;
            drop(state);
            self.exec_simple("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")?;
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> PyResult<()> {
        let mut state = self.tx_state.lock().unwrap();
        if state.in_transaction {
            state.in_transaction = false;
            drop(state);
            let _ = self.exec_simple("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION");
        }
        Ok(())
    }

    pub fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        let mut state = self.tx_state.lock().unwrap();
        if value && state.in_transaction {
            state.in_transaction = false;
            state.autocommit = value;
            drop(state);
            self.exec_simple("COMMIT TRANSACTION")?;
        } else {
            state.autocommit = value;
        }
        Ok(())
    }

    pub fn get_autocommit(&self) -> bool {
        self.tx_state.lock().unwrap().autocommit
    }

    pub fn alloc_cursor(&mut self) -> PyResult<TdsCursor> {
        let client = self.get_client()?;
        Ok(TdsCursor::new(client, self.tx_state.clone()))
    }

    pub fn query_single_string(&self, sql: &str) -> PyResult<Option<String>> {
        let client = self.get_client()?;
        let sql = sql.to_string();
        Python::with_gil(|py| {
            py.allow_threads(|| {
                runtime::block_on(async {
                    let mut c = client.lock().unwrap();
                    let results = c
                        .execute_raw(sql)
                        .await
                        .map_err(to_pyerr)?
                        .into_results()
                        .await
                        .map_err(to_pyerr)?;
                    drop(c);
                    for result_set in &results {
                        for row in result_set {
                            if let Some(val) = row.try_get::<&str, _>(0).map_err(to_pyerr)? {
                                return Ok(Some(val.to_string()));
                            }
                        }
                    }
                    Ok(None)
                })
            })
        })
    }
}
