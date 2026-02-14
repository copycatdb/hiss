use pyo3::prelude::*;
use tabby::error::Error as TabbyError;

fn sqlstate_desc(code: u32) -> &'static str {
    match code {
        102 | 156 | 170 | 207 | 2812 => "Syntax error or access violation",
        208 | 3701 => "Base table or view not found",
        547 | 2601 | 2627 => "Integrity constraint violation",
        245 | 220 | 8115 | 8114 => "Data exception",
        8152 | 2628 => "String or binary data would be truncated",
        _ => "",
    }
}

fn exc_class_name(code: u32, class: u8) -> &'static str {
    match code {
        102 | 156 | 170 | 207 | 208 | 2812 | 3701 => "ProgrammingError",
        547 | 2601 | 2627 => "IntegrityError",
        245 | 8152 | 220 | 8115 | 8114 | 2628 => "DataError",
        _ if class >= 20 => "InternalError",
        _ if class >= 17 => "OperationalError",
        _ => "DatabaseError",
    }
}

pub fn to_pyerr(e: TabbyError) -> PyErr {
    let msg = format!("{}", e);

    match &e {
        TabbyError::Server(token_err) => {
            let code = token_err.code();
            let class = token_err.class();
            let server_msg = token_err.message().to_string();
            let state_desc = sqlstate_desc(code);
            let cls_name = exc_class_name(code, class);

            let driver_msg = if state_desc.is_empty() {
                server_msg.clone()
            } else {
                state_desc.to_string()
            };
            let ddbc_msg = format!("[CopyCat][hiss_native][SQL Server]{}", server_msg);

            Python::with_gil(|py| {
                let result: PyResult<PyErr> = (|| {
                    let exc_mod = py.import("hiss.exceptions")?;
                    let exc_class = exc_mod.getattr(cls_name)?;
                    let err_obj = exc_class.call1((&driver_msg, &ddbc_msg))?;
                    Ok(PyErr::from_value(
                        err_obj.into_any().unbind().into_bound(py),
                    ))
                })();

                result.unwrap_or_else(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Driver Error: {}; DDBC Error: {}",
                        driver_msg, ddbc_msg
                    ))
                })
            })
        }
        TabbyError::Io { .. } => pyo3::exceptions::PyConnectionError::new_err(msg),
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
