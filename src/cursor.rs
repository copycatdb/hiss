use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::connection::SharedClient;
use crate::errors::to_pyerr;
use crate::row_writer::{CompactValue, MultiSetWriter, PyRowWriter};
use crate::runtime;
use crate::types::{column_type_to_sql_type, compact_value_to_py, py_to_sql_literal};
use std::sync::{Arc, Mutex};

fn convert_call_syntax(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inner = trimmed[1..trimmed.len() - 1].trim();
        if inner.to_uppercase().starts_with("CALL ") {
            let rest = inner[5..].trim();
            if let Some(paren_idx) = rest.find('(') {
                let proc_name = &rest[..paren_idx];
                let args = &rest[paren_idx + 1..rest.len() - 1];
                return format!("EXEC {} {}", proc_name.trim(), args.trim());
            } else {
                return format!("EXEC {}", rest);
            }
        }
    }
    sql.to_string()
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: i32,
    pub column_size: i64,
    pub decimal_digits: i32,
    pub nullable: i32,
}

pub struct TransactionState {
    pub autocommit: bool,
    pub in_transaction: bool,
}

pub type SharedTxState = Arc<Mutex<TransactionState>>;

struct ResultSet {
    columns: Vec<ColumnInfo>,
    writer: PyRowWriter,
}

pub struct TdsCursor {
    client: SharedClient,
    tx_state: SharedTxState,
    columns: Option<Vec<ColumnInfo>>,
    writer: Option<PyRowWriter>,
    row_index: usize,
    _rowcount: i64,
    pending: Vec<ResultSet>,
    messages: Vec<(String, String)>,
}

impl TdsCursor {
    pub fn new(client: SharedClient, tx_state: SharedTxState) -> Self {
        TdsCursor {
            client,
            tx_state,
            columns: None,
            writer: None,
            row_index: 0,
            _rowcount: -1,
            pending: Vec::new(),
            messages: Vec::new(),
        }
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.columns = None;
        self.writer = None;
        self.pending.clear();
        Ok(())
    }

    fn begin_transaction_if_needed(&self) -> PyResult<Option<String>> {
        let mut state = self.tx_state.lock().unwrap();
        if !state.autocommit && !state.in_transaction {
            state.in_transaction = true;
            Ok(Some("BEGIN TRANSACTION\n".to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[Bound<'_, PyAny>]) -> PyResult<i32> {
        let sql = convert_call_syntax(sql);
        let final_sql = if params.is_empty() {
            sql.to_string()
        } else {
            Python::with_gil(|py| substitute_params(py, &sql, params))?
        };

        let tx_prefix = self.begin_transaction_if_needed()?;
        let client = self.client.clone();

        self.columns = None;
        self.writer = None;
        self.row_index = 0;
        self._rowcount = -1;
        self.pending.clear();

        self.execute_direct(client, &final_sql, tx_prefix)
    }

    /// Execute via batch_into: TDS wire bytes → RowWriter → CompactValue.
    /// No SqlValue. No claw. Direct decode.
    fn execute_direct(
        &mut self,
        client: SharedClient,
        final_sql: &str,
        tx_prefix: Option<String>,
    ) -> PyResult<i32> {
        let trimmed_upper = final_sql.trim().to_uppercase();

        let needs_rowcount = trimmed_upper.starts_with("INSERT ")
            || trimmed_upper.starts_with("UPDATE ")
            || trimmed_upper.starts_with("DELETE ")
            || trimmed_upper.starts_with("TRUNCATE ")
            || trimmed_upper.starts_with("MERGE ");

        let skip_rowcount = trimmed_upper.starts_with("CREATE VIEW")
            || trimmed_upper.starts_with("ALTER VIEW")
            || trimmed_upper.starts_with("CREATE TRIGGER")
            || trimmed_upper.starts_with("ALTER TRIGGER")
            || trimmed_upper.starts_with("CREATE FUNCTION")
            || trimmed_upper.starts_with("ALTER FUNCTION")
            || trimmed_upper.starts_with("CREATE PROCEDURE")
            || trimmed_upper.starts_with("ALTER PROCEDURE")
            || trimmed_upper.starts_with("CREATE PROC ")
            || trimmed_upper.starts_with("ALTER PROC ")
            || trimmed_upper.starts_with("CREATE OR ALTER");

        let mut batch_sql =
            String::with_capacity(final_sql.len() + tx_prefix.as_ref().map_or(0, |p| p.len()) + 40);
        if let Some(prefix) = tx_prefix {
            batch_sql.push_str(&prefix);
        }
        batch_sql.push_str(final_sql);
        if needs_rowcount && !skip_rowcount {
            batch_sql.push_str("\nSELECT @@ROWCOUNT AS __rowcount__");
        }

        // batch_into: sends raw SQL, decodes TDS wire bytes directly into
        // MultiSetWriter via RowWriter trait. No SqlValue created at any point.
        let (result_sets, messages) = Python::with_gil(|py| {
            py.allow_threads(|| {
                runtime::block_on(async {
                    let mut c = client.lock().unwrap();
                    let mut msw = MultiSetWriter::new();

                    c.batch_into(batch_sql, &mut msw).await.map_err(to_pyerr)?;

                    drop(c);
                    let messages = msw.messages.clone();
                    Ok::<_, PyErr>((msw.finalize(), messages))
                })
            })
        })?;

        self.messages = messages;
        self.process_results(result_sets, needs_rowcount && !skip_rowcount)
    }

    fn process_results(
        &mut self,
        results: Vec<(Vec<ColumnInfo>, PyRowWriter)>,
        check_rowcount: bool,
    ) -> PyResult<i32> {
        let mut all_sets: Vec<ResultSet> = Vec::new();
        let mut rowcount_from_batch: Option<i64> = None;

        for (cols, writer) in results {
            if cols.is_empty() {
                continue;
            }

            if check_rowcount && cols.len() == 1 && cols[0].name == "__rowcount__" {
                if writer.row_count() > 0 {
                    if let CompactValue::I64(v) = writer.get(0, 0) {
                        rowcount_from_batch = Some(*v);
                    }
                }
                continue;
            }

            all_sets.push(ResultSet {
                columns: cols,
                writer,
            });
        }

        if !all_sets.is_empty() {
            let first = all_sets.remove(0);
            self.columns = Some(first.columns);
            self.writer = Some(first.writer);
            self._rowcount = -1;
            self.pending = all_sets;
        } else {
            self.columns = None;
            self.writer = None;
            self._rowcount = rowcount_from_batch.unwrap_or(0);
        }

        Ok(0)
    }

    #[inline]
    fn row_to_py(&self, py: Python<'_>, row_idx: usize) -> PyResult<Vec<PyObject>> {
        let writer = self.writer.as_ref().unwrap();
        let col_count = writer.col_count;
        let mut py_row = Vec::with_capacity(col_count);
        for c in 0..col_count {
            py_row.push(compact_value_to_py(py, writer.get(row_idx, c))?);
        }
        Ok(py_row)
    }

    pub fn column_to_info(c: &tabby::Column) -> ColumnInfo {
        let type_name = format!("{:?}", c.column_type());
        let sql_type = column_type_to_sql_type(&type_name);

        let (column_size, decimal_digits, nullable) = if let Some(ti) = c.type_info() {
            use tabby::DataType;
            match ti {
                DataType::FixedLen(ft) => {
                    use tabby::FixedLenType;
                    let size = match ft {
                        FixedLenType::Null => 0,
                        FixedLenType::Bit => 1,
                        FixedLenType::Int1 => 3,
                        FixedLenType::Int2 => 5,
                        FixedLenType::Int4 => 10,
                        FixedLenType::Int8 => 19,
                        FixedLenType::Float4 => 24,
                        FixedLenType::Float8 => 53,
                        FixedLenType::Datetime4 => 16,
                        FixedLenType::Datetime => 23,
                        FixedLenType::Money4 => 10,
                        FixedLenType::Money => 19,
                    };
                    (size as i64, 0i32, c.nullable().unwrap_or(true))
                }
                DataType::VarLenSized(ctx) => {
                    use tabby::VarLenType;
                    let len = ctx.len() as i64;
                    let size = match ctx.r#type() {
                        VarLenType::Intn => match ctx.len() {
                            1 => 3,
                            2 => 5,
                            4 => 10,
                            _ => 19,
                        },
                        VarLenType::Bitn => 1,
                        VarLenType::Floatn => match ctx.len() {
                            4 => 24,
                            _ => 53,
                        },
                        VarLenType::Guid => 36,
                        VarLenType::NVarchar | VarLenType::NChar => {
                            if len > 4000 {
                                0
                            } else {
                                len / 2
                            }
                        }
                        VarLenType::BigVarChar | VarLenType::BigChar => {
                            if len > 8000 {
                                0
                            } else {
                                len
                            }
                        }
                        VarLenType::BigVarBin | VarLenType::BigBinary => {
                            if len > 8000 {
                                0
                            } else {
                                len
                            }
                        }
                        VarLenType::Datetimen => match ctx.len() {
                            4 => 16,
                            _ => 23,
                        },
                        VarLenType::Daten => 10,
                        VarLenType::Timen => 8 + ctx.len() as i64,
                        VarLenType::Datetime2 => {
                            19 + if ctx.len() > 0 {
                                1 + ctx.len() as i64
                            } else {
                                0
                            }
                        }
                        VarLenType::DatetimeOffsetn => {
                            26 + if ctx.len() > 0 {
                                1 + ctx.len() as i64
                            } else {
                                0
                            }
                        }
                        VarLenType::Money => 19,
                        VarLenType::Text | VarLenType::NText => 0,
                        VarLenType::Image => 0,
                        VarLenType::Xml => 0,
                        _ => len,
                    };
                    (size, 0, c.nullable().unwrap_or(true))
                }
                DataType::VarLenSizedPrecision {
                    precision, scale, ..
                } => (
                    *precision as i64,
                    *scale as i32,
                    c.nullable().unwrap_or(true),
                ),
                DataType::Xml { .. } => (0, 0, c.nullable().unwrap_or(true)),
            }
        } else {
            (0, 0, true)
        };

        ColumnInfo {
            name: c.name().to_string(),
            sql_type,
            column_size,
            decimal_digits,
            nullable: if nullable { 1 } else { 0 },
        }
    }

    pub fn execute_many(
        &mut self,
        sql: &str,
        columnwise_params: &[Bound<'_, PyList>],
        row_count: usize,
    ) -> PyResult<i32> {
        let mut total_affected: i64 = 0;
        for row_idx in 0..row_count {
            let params: Vec<PyObject> = Python::with_gil(|_py| -> PyResult<Vec<PyObject>> {
                let mut row_params = Vec::new();
                for col in columnwise_params {
                    let val = col.get_item(row_idx)?;
                    row_params.push(val.unbind());
                }
                Ok(row_params)
            })?;
            Python::with_gil(|py| -> PyResult<()> {
                let bound_params: Vec<Bound<'_, PyAny>> =
                    params.iter().map(|p| p.bind(py).clone()).collect();
                self.execute(sql, &bound_params)?;
                if self._rowcount >= 0 {
                    total_affected += self._rowcount;
                }
                Ok(())
            })?;
        }
        self._rowcount = total_affected;
        Ok(0)
    }

    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Vec<PyObject>>> {
        if let Some(ref writer) = self.writer {
            if self.row_index < writer.row_count() {
                let row = self.row_to_py(py, self.row_index)?;
                self.row_index += 1;
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub fn fetchmany(&mut self, py: Python<'_>, size: usize) -> PyResult<Vec<Vec<PyObject>>> {
        let total = self.writer.as_ref().map_or(0, |w| w.row_count());
        let end = std::cmp::min(self.row_index + size, total);
        let mut result = Vec::with_capacity(end - self.row_index);
        for i in self.row_index..end {
            result.push(self.row_to_py(py, i)?);
        }
        self.row_index = end;
        Ok(result)
    }

    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<Vec<PyObject>>> {
        let total = self.writer.as_ref().map_or(0, |w| w.row_count());
        let remaining = total - self.row_index;
        let mut result = Vec::with_capacity(remaining);
        for i in self.row_index..total {
            result.push(self.row_to_py(py, i)?);
        }
        self.row_index = total;
        Ok(result)
    }

    pub fn nextset(&mut self) -> PyResult<bool> {
        if self.pending.is_empty() {
            self.columns = None;
            self.writer = None;
            self.row_index = 0;
            return Ok(false);
        }
        let next = self.pending.remove(0);
        self.columns = Some(next.columns);
        self.writer = Some(next.writer);
        self.row_index = 0;
        self._rowcount = -1;
        Ok(true)
    }

    pub fn rowcount(&self) -> i64 {
        self._rowcount
    }
    pub fn description(&self) -> Option<&Vec<ColumnInfo>> {
        self.columns.as_ref()
    }
    pub fn row_count_total(&self) -> usize {
        self.writer.as_ref().map_or(0, |w| w.row_count())
    }
    pub fn current_row_index(&self) -> usize {
        self.row_index
    }
    pub fn set_row_index(&mut self, idx: usize) {
        self.row_index = idx;
    }
    pub fn get_messages(&self) -> &[(String, String)] {
        &self.messages
    }
}

fn substitute_params(py: Python<'_>, sql: &str, params: &[Bound<'_, PyAny>]) -> PyResult<String> {
    let mut result = String::with_capacity(sql.len() + params.len() * 16);
    let mut param_idx = 0;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '?' {
            if param_idx < params.len() {
                let literal = py_to_sql_literal(py, &params[param_idx])?;
                result.push_str(&literal);
                param_idx += 1;
            } else {
                result.push('?');
            }
        } else if c == '\'' {
            result.push(c);
            while let Some(sc) = chars.next() {
                result.push(sc);
                if sc == '\'' {
                    if chars.peek() == Some(&'\'') {
                        result.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }
    Ok(result)
}
