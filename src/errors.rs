use pyo3::prelude::*;
use tabby::error::Error as TabbyError;

pub fn to_pyerr(e: TabbyError) -> PyErr {
    let msg = format!("{}", e);
    match &e {
        TabbyError::Server(_) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        TabbyError::Io { .. } => pyo3::exceptions::PyConnectionError::new_err(msg),
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
