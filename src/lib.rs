use pyo3::prelude::*;

mod connection;
mod cursor;
mod errors;
pub mod row_writer;
mod runtime;
mod types;

use connection::TdsConnection;
use cursor::TdsCursor;

#[pyclass]
#[derive(Clone)]
pub struct NumericData {
    #[pyo3(get, set)]
    pub precision: i32,
    #[pyo3(get, set)]
    pub scale: i32,
    #[pyo3(get, set)]
    pub sign: i32,
    #[pyo3(get, set)]
    pub val: i64,
}

#[pymethods]
impl NumericData {
    #[new]
    fn new() -> Self {
        NumericData {
            precision: 0,
            scale: 0,
            sign: 1,
            val: 0,
        }
    }
}

#[pyclass]
pub struct ParamInfo {
    #[pyo3(get, set, name = "paramCType")]
    pub param_c_type: i32,
    #[pyo3(get, set, name = "paramSQLType")]
    pub param_sql_type: i32,
    #[pyo3(get, set, name = "inputOutputType")]
    pub input_output_type: i32,
    #[pyo3(get, set, name = "columnSize")]
    pub column_size: i64,
    #[pyo3(get, set, name = "decimalDigits")]
    pub decimal_digits: i32,
    #[pyo3(get, set, name = "isDAE")]
    pub is_dae: bool,
    #[pyo3(get, set, name = "dataPtr")]
    pub data_ptr: PyObject,
}

#[pymethods]
impl ParamInfo {
    #[new]
    fn new(py: Python<'_>) -> Self {
        ParamInfo {
            param_c_type: 0,
            param_sql_type: 0,
            input_output_type: 0,
            column_size: 0,
            decimal_digits: 0,
            is_dae: false,
            data_ptr: py.None().into(),
        }
    }
}

#[pyclass]
pub struct StatementHandle {
    pub cursor: TdsCursor,
}

#[pymethods]
impl StatementHandle {
    fn free(&mut self) -> PyResult<()> {
        self.cursor.close()
    }
}

#[pyclass(name = "Connection")]
pub struct PyConnection {
    inner: TdsConnection,
}

#[pymethods]
impl PyConnection {
    #[new]
    #[pyo3(signature = (connection_str, _pooling=false, _attrs_before=None))]
    fn new(
        connection_str: &str,
        _pooling: bool,
        _attrs_before: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let conn = TdsConnection::new(connection_str, _attrs_before)?;
        Ok(PyConnection { inner: conn })
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner.close()
    }
    fn commit(&mut self) -> PyResult<()> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> PyResult<()> {
        self.inner.rollback()
    }
    fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        self.inner.set_autocommit(value)
    }
    fn get_autocommit(&self) -> bool {
        self.inner.get_autocommit()
    }

    fn alloc_statement_handle(&mut self) -> PyResult<StatementHandle> {
        let cursor = self.inner.alloc_cursor()?;
        Ok(StatementHandle { cursor })
    }

    fn get_info(&self, info_type: u16) -> PyResult<Option<PyObject>> {
        Python::with_gil(|py| {
            let result: Option<PyObject> = match info_type {
                0 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                1 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                2 => Some("".into_pyobject(py).unwrap().into_any().unbind()),
                6 => Some("hiss_native".into_pyobject(py).unwrap().into_any().unbind()),
                7 => Some("01.00.0000".into_pyobject(py).unwrap().into_any().unbind()),
                13 => match self.inner.query_single_string("SELECT @@SERVERNAME") {
                    Ok(Some(s)) => Some(s.into_pyobject(py).unwrap().into_any().unbind()),
                    _ => Some("".into_pyobject(py).unwrap().into_any().unbind()),
                },
                16 => match self.inner.query_single_string("SELECT DB_NAME()") {
                    Ok(Some(s)) => Some(s.into_pyobject(py).unwrap().into_any().unbind()),
                    _ => Some("".into_pyobject(py).unwrap().into_any().unbind()),
                },
                17 => Some(
                    "Microsoft SQL Server"
                        .into_pyobject(py)
                        .unwrap()
                        .into_any()
                        .unbind(),
                ),
                18 => match self.inner.query_single_string(
                    "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(128))",
                ) {
                    Ok(Some(s)) => Some(s.into_pyobject(py).unwrap().into_any().unbind()),
                    _ => Some("".into_pyobject(py).unwrap().into_any().unbind()),
                },
                19 => Some("Y".into_pyobject(py).unwrap().into_any().unbind()),
                20 => Some("Y".into_pyobject(py).unwrap().into_any().unbind()),
                21 => Some("Y".into_pyobject(py).unwrap().into_any().unbind()),
                25 => Some("N".into_pyobject(py).unwrap().into_any().unbind()),
                27 => Some("Y".into_pyobject(py).unwrap().into_any().unbind()),
                30 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                32 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                34 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                35 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                46 => Some(2u32.into_pyobject(py).unwrap().into_any().unbind()),
                47 => match self.inner.query_single_string("SELECT CURRENT_USER") {
                    Ok(Some(s)) => Some(s.into_pyobject(py).unwrap().into_any().unbind()),
                    _ => Some("".into_pyobject(py).unwrap().into_any().unbind()),
                },
                48 => Some(3u32.into_pyobject(py).unwrap().into_any().unbind()),
                49 => Some(0x00FFFFFFu32.into_pyobject(py).unwrap().into_any().unbind()),
                50 => Some(0x00FFFFFFu32.into_pyobject(py).unwrap().into_any().unbind()),
                51 => Some(0x001FFFFFu32.into_pyobject(py).unwrap().into_any().unbind()),
                72 => Some(0x0Fu32.into_pyobject(py).unwrap().into_any().unbind()),
                97 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                98 => Some(32u32.into_pyobject(py).unwrap().into_any().unbind()),
                99 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                100 => Some(4096u32.into_pyobject(py).unwrap().into_any().unbind()),
                101 => Some(1024u32.into_pyobject(py).unwrap().into_any().unbind()),
                104 => Some(8060u32.into_pyobject(py).unwrap().into_any().unbind()),
                105 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                106 => Some(256u32.into_pyobject(py).unwrap().into_any().unbind()),
                107 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                108 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                112 => Some(0u32.into_pyobject(py).unwrap().into_any().unbind()),
                10005 => Some(128u32.into_pyobject(py).unwrap().into_any().unbind()),
                _ => Some("".into_pyobject(py).unwrap().into_any().unbind()),
            };
            Ok(result)
        })
    }
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[pyfunction]
#[pyo3(signature = (stmt, sql, params, param_types, is_prepared, use_prepare))]
#[pyo3(name = "DDBCSQLExecute")]
fn ddbc_sql_execute(
    stmt: &mut StatementHandle,
    sql: &str,
    params: Vec<Bound<'_, PyAny>>,
    param_types: Vec<Bound<'_, PyAny>>,
    is_prepared: &Bound<'_, pyo3::types::PyList>,
    use_prepare: bool,
) -> PyResult<i32> {
    let _ = (param_types, is_prepared, use_prepare);
    stmt.cursor.execute(sql, &params)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLRowCount")]
fn ddbc_sql_row_count(stmt: &StatementHandle) -> PyResult<i64> {
    Ok(stmt.cursor.rowcount())
}

#[pyfunction]
#[pyo3(name = "DDBCSQLDescribeCol")]
fn ddbc_sql_describe_col(
    stmt: &StatementHandle,
    metadata: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<()> {
    let py = metadata.py();
    if let Some(cols) = stmt.cursor.description() {
        for col in cols {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("ColumnName", &col.name)?;
            dict.set_item("DataType", col.sql_type)?;
            dict.set_item("ColumnSize", col.column_size)?;
            dict.set_item("DecimalDigits", col.decimal_digits)?;
            dict.set_item("Nullable", col.nullable)?;
            metadata.append(dict)?;
        }
    }
    Ok(())
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchOne")]
fn ddbc_sql_fetch_one(
    stmt: &mut StatementHandle,
    row_data: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<i32> {
    let py = row_data.py();
    match stmt.cursor.fetchone(py)? {
        Some(row) => {
            for val in row {
                row_data.append(val)?;
            }
            Ok(0)
        }
        None => Ok(100),
    }
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchMany")]
fn ddbc_sql_fetch_many(
    stmt: &mut StatementHandle,
    rows_data: &Bound<'_, pyo3::types::PyList>,
    size: usize,
) -> PyResult<i32> {
    let py = rows_data.py();
    let rows = stmt.cursor.fetchmany(py, size)?;
    for row in rows {
        let py_list = pyo3::types::PyList::new(py, &row)?;
        rows_data.append(py_list)?;
    }
    Ok(0)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchAll")]
fn ddbc_sql_fetch_all(
    stmt: &mut StatementHandle,
    rows_data: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<i32> {
    let py = rows_data.py();
    let rows = stmt.cursor.fetchall(py)?;
    for row in rows {
        let py_list = pyo3::types::PyList::new(py, &row)?;
        rows_data.append(py_list)?;
    }
    Ok(0)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLMoreResults")]
fn ddbc_sql_more_results(stmt: &mut StatementHandle) -> PyResult<i32> {
    match stmt.cursor.nextset()? {
        true => Ok(0),
        false => Ok(100),
    }
}

#[pyfunction]
#[pyo3(name = "DDBCSQLSetStmtAttr")]
fn ddbc_sql_set_stmt_attr(_stmt: &StatementHandle, _attr: i32, _value: i32) -> PyResult<i32> {
    Ok(0)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetAllDiagRecords")]
fn ddbc_sql_get_all_diag_records(stmt: &StatementHandle) -> PyResult<Vec<(String, String)>> {
    Ok(stmt.cursor.get_messages().to_vec())
}

#[pyfunction]
#[pyo3(name = "DDBCSetDecimalSeparator")]
fn ddbc_set_decimal_separator(_sep: &str) -> PyResult<()> {
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (stmt, catalog, schema, table, types))]
#[pyo3(name = "DDBCSQLTables")]
fn ddbc_sql_tables(
    stmt: &mut StatementHandle,
    catalog: &str,
    schema: &str,
    table: &str,
    types: &str,
) -> PyResult<i32> {
    let mut conditions = Vec::new();
    if !catalog.is_empty() {
        conditions.push(format!("TABLE_CATALOG = N'{}'", escape_sql(catalog)));
    }
    if !schema.is_empty() {
        conditions.push(format!("TABLE_SCHEMA LIKE N'{}'", escape_sql(schema)));
    }
    if !table.is_empty() {
        conditions.push(format!("TABLE_NAME LIKE N'{}'", escape_sql(table)));
    }
    if !types.is_empty() {
        let type_list: Vec<String> = types
            .split(',')
            .map(|t| {
                let t = t.trim().trim_matches('\'');
                let mapped = if t.eq_ignore_ascii_case("TABLE") {
                    "BASE TABLE"
                } else {
                    t
                };
                format!("N'{}'", mapped)
            })
            .collect();
        conditions.push(format!("TABLE_TYPE IN ({})", type_list.join(",")));
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT TABLE_CATALOG AS table_cat, TABLE_SCHEMA AS table_schem, TABLE_NAME AS table_name, \
         CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS table_type, \
         CAST(NULL AS VARCHAR(254)) AS remarks \
         FROM INFORMATION_SCHEMA.TABLES {} ORDER BY table_type, table_cat, table_schem, table_name",
        where_clause
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, catalog=None, schema=None, table=None, column=None))]
#[pyo3(name = "DDBCSQLColumns")]
fn ddbc_sql_columns(
    stmt: &mut StatementHandle,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: Option<&str>,
    column: Option<&str>,
) -> PyResult<i32> {
    let mut conditions = Vec::new();
    if let Some(c) = catalog {
        if !c.is_empty() {
            conditions.push(format!("c.TABLE_CATALOG = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = schema {
        if !s.is_empty() {
            conditions.push(format!("c.TABLE_SCHEMA LIKE N'{}'", escape_sql(s)));
        }
    }
    if let Some(t) = table {
        if !t.is_empty() {
            conditions.push(format!("c.TABLE_NAME LIKE N'{}'", escape_sql(t)));
        }
    }
    if let Some(col) = column {
        if !col.is_empty() {
            conditions.push(format!("c.COLUMN_NAME LIKE N'{}'", escape_sql(col)));
        }
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT c.TABLE_CATALOG AS table_cat, c.TABLE_SCHEMA AS table_schem, c.TABLE_NAME AS table_name, \
         c.COLUMN_NAME AS column_name, \
         CASE c.DATA_TYPE \
           WHEN 'int' THEN 4 WHEN 'bigint' THEN -5 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN -6 \
           WHEN 'bit' THEN -7 WHEN 'float' THEN 6 WHEN 'real' THEN 7 \
           WHEN 'decimal' THEN 3 WHEN 'numeric' THEN 2 WHEN 'money' THEN 3 WHEN 'smallmoney' THEN 3 \
           WHEN 'char' THEN 1 WHEN 'varchar' THEN 12 WHEN 'text' THEN -1 \
           WHEN 'nchar' THEN -8 WHEN 'nvarchar' THEN -9 WHEN 'ntext' THEN -10 \
           WHEN 'binary' THEN -2 WHEN 'varbinary' THEN -3 WHEN 'image' THEN -4 \
           WHEN 'datetime' THEN 93 WHEN 'smalldatetime' THEN 93 WHEN 'datetime2' THEN 93 \
           WHEN 'date' THEN 91 WHEN 'time' THEN 92 WHEN 'datetimeoffset' THEN -155 \
           WHEN 'uniqueidentifier' THEN -11 WHEN 'xml' THEN -152 \
           ELSE 0 END AS data_type, \
         c.DATA_TYPE AS type_name, \
         COALESCE(c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, \
           CASE c.DATA_TYPE WHEN 'datetime' THEN 23 WHEN 'smalldatetime' THEN 16 WHEN 'datetime2' THEN 27 \
           WHEN 'date' THEN 10 WHEN 'time' THEN 16 WHEN 'datetimeoffset' THEN 34 \
           WHEN 'bit' THEN 1 WHEN 'uniqueidentifier' THEN 36 ELSE 0 END) AS column_size, \
         COALESCE(c.CHARACTER_OCTET_LENGTH, c.NUMERIC_PRECISION, 0) AS buffer_length, \
         c.NUMERIC_SCALE AS decimal_digits, \
         CASE WHEN c.NUMERIC_PRECISION_RADIX IS NOT NULL THEN 10 ELSE NULL END AS num_prec_radix, \
         CASE c.IS_NULLABLE WHEN 'YES' THEN 1 WHEN 'NO' THEN 0 ELSE 2 END AS nullable, \
         CAST(NULL AS VARCHAR(254)) AS remarks, \
         c.COLUMN_DEFAULT AS column_def, \
         CASE c.DATA_TYPE \
           WHEN 'int' THEN 4 WHEN 'bigint' THEN -5 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN -6 \
           WHEN 'bit' THEN -7 WHEN 'float' THEN 6 WHEN 'real' THEN 7 \
           WHEN 'decimal' THEN 3 WHEN 'numeric' THEN 2 WHEN 'money' THEN 3 WHEN 'smallmoney' THEN 3 \
           WHEN 'char' THEN 1 WHEN 'varchar' THEN 12 WHEN 'text' THEN -1 \
           WHEN 'nchar' THEN -8 WHEN 'nvarchar' THEN -9 WHEN 'ntext' THEN -10 \
           WHEN 'binary' THEN -2 WHEN 'varbinary' THEN -3 WHEN 'image' THEN -4 \
           WHEN 'datetime' THEN 9 WHEN 'smalldatetime' THEN 9 WHEN 'datetime2' THEN 9 \
           WHEN 'date' THEN 9 WHEN 'time' THEN 9 WHEN 'datetimeoffset' THEN 9 \
           WHEN 'uniqueidentifier' THEN -11 WHEN 'xml' THEN -152 \
           ELSE 0 END AS sql_data_type, \
         CASE c.DATA_TYPE \
           WHEN 'datetime' THEN 3 WHEN 'smalldatetime' THEN 0 WHEN 'datetime2' THEN 7 \
           WHEN 'time' THEN 7 WHEN 'datetimeoffset' THEN 7 ELSE NULL END AS sql_datetime_sub, \
         c.CHARACTER_OCTET_LENGTH AS char_octet_length, \
         c.ORDINAL_POSITION AS ordinal_position, \
         c.IS_NULLABLE AS is_nullable, \
         CAST(NULL AS INT) AS ss_data_type \
         FROM INFORMATION_SCHEMA.COLUMNS c {} \
         ORDER BY table_cat, table_schem, table_name, ordinal_position",
        where_clause
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, catalog=None, schema=None, table=""))]
#[pyo3(name = "DDBCSQLPrimaryKeys")]
fn ddbc_sql_primary_keys(
    stmt: &mut StatementHandle,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
) -> PyResult<i32> {
    let mut conditions = vec!["tc.CONSTRAINT_TYPE = 'PRIMARY KEY'".to_string()];
    if let Some(c) = catalog {
        if !c.is_empty() {
            conditions.push(format!("tc.TABLE_CATALOG = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = schema {
        if !s.is_empty() {
            conditions.push(format!("tc.TABLE_SCHEMA = N'{}'", escape_sql(s)));
        }
    }
    if !table.is_empty() {
        conditions.push(format!("tc.TABLE_NAME = N'{}'", escape_sql(table)));
    }
    let sql = format!(
        "SELECT tc.TABLE_CATALOG AS table_cat, tc.TABLE_SCHEMA AS table_schem, tc.TABLE_NAME AS table_name, \
         kcu.COLUMN_NAME AS column_name, kcu.ORDINAL_POSITION AS key_seq, tc.CONSTRAINT_NAME AS pk_name \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
           ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_CATALOG = kcu.TABLE_CATALOG \
         WHERE {} ORDER BY table_cat, table_schem, table_name, key_seq",
        conditions.join(" AND ")
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, fk_catalog=None, fk_schema=None, fk_table=None, pk_catalog=None, pk_schema=None, pk_table=None))]
#[pyo3(name = "DDBCSQLForeignKeys")]
fn ddbc_sql_foreign_keys(
    stmt: &mut StatementHandle,
    fk_catalog: Option<&str>,
    fk_schema: Option<&str>,
    fk_table: Option<&str>,
    pk_catalog: Option<&str>,
    pk_schema: Option<&str>,
    pk_table: Option<&str>,
) -> PyResult<i32> {
    let mut conditions = Vec::new();
    if let Some(c) = fk_catalog {
        if !c.is_empty() {
            conditions.push(format!("pk_kcu.TABLE_CATALOG = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = fk_schema {
        if !s.is_empty() {
            conditions.push(format!("pk_kcu.TABLE_SCHEMA = N'{}'", escape_sql(s)));
        }
    }
    if let Some(t) = fk_table {
        if !t.is_empty() {
            conditions.push(format!("pk_kcu.TABLE_NAME = N'{}'", escape_sql(t)));
        }
    }
    if let Some(c) = pk_catalog {
        if !c.is_empty() {
            conditions.push(format!("fk_kcu.TABLE_CATALOG = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = pk_schema {
        if !s.is_empty() {
            conditions.push(format!("fk_kcu.TABLE_SCHEMA = N'{}'", escape_sql(s)));
        }
    }
    if let Some(t) = pk_table {
        if !t.is_empty() {
            conditions.push(format!("fk_kcu.TABLE_NAME = N'{}'", escape_sql(t)));
        }
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("AND {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT pk_kcu.TABLE_CATALOG AS pktable_cat, pk_kcu.TABLE_SCHEMA AS pktable_schem, pk_kcu.TABLE_NAME AS pktable_name, \
         pk_kcu.COLUMN_NAME AS pkcolumn_name, \
         fk_kcu.TABLE_CATALOG AS fktable_cat, fk_kcu.TABLE_SCHEMA AS fktable_schem, fk_kcu.TABLE_NAME AS fktable_name, \
         fk_kcu.COLUMN_NAME AS fkcolumn_name, \
         fk_kcu.ORDINAL_POSITION AS key_seq, \
         CASE rc.UPDATE_RULE WHEN 'CASCADE' THEN 0 WHEN 'SET NULL' THEN 2 WHEN 'SET DEFAULT' THEN 4 ELSE 1 END AS update_rule, \
         CASE rc.DELETE_RULE WHEN 'CASCADE' THEN 0 WHEN 'SET NULL' THEN 2 WHEN 'SET DEFAULT' THEN 4 ELSE 1 END AS delete_rule, \
         rc.CONSTRAINT_NAME AS fk_name, \
         rc.UNIQUE_CONSTRAINT_NAME AS pk_name, \
         7 AS deferrability \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc \
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_kcu ON rc.CONSTRAINT_NAME = fk_kcu.CONSTRAINT_NAME AND rc.CONSTRAINT_SCHEMA = fk_kcu.TABLE_SCHEMA \
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_kcu ON rc.UNIQUE_CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk_kcu.TABLE_SCHEMA AND fk_kcu.ORDINAL_POSITION = pk_kcu.ORDINAL_POSITION \
         WHERE 1=1 {} \
         ORDER BY fktable_cat, fktable_schem, fktable_name, key_seq",
        where_clause
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, catalog=None, schema=None, table="", unique=0, _reserved=0))]
#[pyo3(name = "DDBCSQLStatistics")]
fn ddbc_sql_statistics(
    stmt: &mut StatementHandle,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
    unique: i32,
    _reserved: i32,
) -> PyResult<i32> {
    let mut conditions = Vec::new();
    if let Some(c) = catalog {
        if !c.is_empty() {
            conditions.push(format!("DB_NAME() = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = schema {
        if !s.is_empty() {
            conditions.push(format!("s.name = N'{}'", escape_sql(s)));
        }
    }
    if !table.is_empty() {
        conditions.push(format!("t.name = N'{}'", escape_sql(table)));
    }
    if unique == 0 {
        conditions.push("i.is_unique = 1".to_string());
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT DB_NAME() AS table_cat, s.name AS table_schem, t.name AS table_name, \
         CASE WHEN i.is_unique = 1 THEN 0 ELSE 1 END AS non_unique, \
         DB_NAME() AS index_qualifier, i.name AS index_name, \
         CASE i.type WHEN 1 THEN 1 WHEN 2 THEN 3 WHEN 3 THEN 3 ELSE 0 END AS type, \
         ic.key_ordinal AS ordinal_position, \
         c.name AS column_name, \
         CASE WHEN ic.is_descending_key = 1 THEN 'D' ELSE 'A' END AS asc_or_desc, \
         CAST(NULL AS INT) AS cardinality, \
         CAST(NULL AS INT) AS pages, \
         CAST(NULL AS VARCHAR(128)) AS filter_condition \
         FROM sys.tables t \
         JOIN sys.schemas s ON t.schema_id = s.schema_id \
         JOIN sys.indexes i ON t.object_id = i.object_id \
         JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
         JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         {} ORDER BY non_unique, index_name, ordinal_position",
        where_clause
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, catalog=None, schema=None, procedure=None))]
#[pyo3(name = "DDBCSQLProcedures")]
fn ddbc_sql_procedures(
    stmt: &mut StatementHandle,
    catalog: Option<&str>,
    schema: Option<&str>,
    procedure: Option<&str>,
) -> PyResult<i32> {
    let mut conditions = Vec::new();
    if let Some(c) = catalog {
        if !c.is_empty() {
            conditions.push(format!("ROUTINE_CATALOG = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = schema {
        if !s.is_empty() {
            conditions.push(format!("ROUTINE_SCHEMA LIKE N'{}'", escape_sql(s)));
        }
    }
    if let Some(p) = procedure {
        if !p.is_empty() {
            conditions.push(format!("ROUTINE_NAME LIKE N'{}'", escape_sql(p)));
        }
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT ROUTINE_CATALOG AS procedure_cat, ROUTINE_SCHEMA AS procedure_schem, \
         ROUTINE_NAME + ';1' AS procedure_name, \
         CAST(NULL AS INT) AS num_input_params, CAST(NULL AS INT) AS num_output_params, CAST(NULL AS INT) AS num_result_sets, \
         CAST(NULL AS VARCHAR(254)) AS remarks, \
         CASE ROUTINE_TYPE WHEN 'PROCEDURE' THEN 1 WHEN 'FUNCTION' THEN 2 ELSE 0 END AS procedure_type \
         FROM INFORMATION_SCHEMA.ROUTINES {} ORDER BY procedure_cat, procedure_schem, procedure_name",
        where_clause
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(signature = (stmt, id_type, catalog=None, schema=None, table="", _scope=0, _nullable=0))]
#[pyo3(name = "DDBCSQLSpecialColumns")]
fn ddbc_sql_special_columns(
    stmt: &mut StatementHandle,
    id_type: i32,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
    _scope: i32,
    _nullable: i32,
) -> PyResult<i32> {
    if id_type == 1 && _nullable == 0 {
        let sql = "SELECT 2 AS scope, '' AS column_name, 0 AS data_type, '' AS type_name, 0 AS column_size, 0 AS buffer_length, 0 AS decimal_digits, 1 AS pseudo_column WHERE 1=0";
        return stmt.cursor.execute(sql, &[]);
    }

    let mut conditions = Vec::new();
    if let Some(c) = catalog {
        if !c.is_empty() {
            conditions.push(format!("DB_NAME() = N'{}'", escape_sql(c)));
        }
    }
    if let Some(s) = schema {
        if !s.is_empty() {
            conditions.push(format!("s.name = N'{}'", escape_sql(s)));
        }
    }
    if !table.is_empty() {
        conditions.push(format!("t.name = N'{}'", escape_sql(table)));
    }
    let where_clause = if conditions.is_empty() {
        "WHERE 1=1".to_string()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let sql = if id_type == 2 {
        format!(
            "SELECT 2 AS scope, c.name AS column_name, \
             CASE tp.name WHEN 'timestamp' THEN -2 WHEN 'rowversion' THEN -2 ELSE 0 END AS data_type, \
             tp.name AS type_name, c.max_length AS column_size, c.max_length AS buffer_length, \
             0 AS decimal_digits, 1 AS pseudo_column \
             FROM sys.tables t \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             JOIN sys.columns c ON t.object_id = c.object_id \
             JOIN sys.types tp ON c.system_type_id = tp.system_type_id AND c.user_type_id = tp.user_type_id \
             {} AND tp.name IN ('timestamp', 'rowversion') \
             ORDER BY scope",
            where_clause
        )
    } else {
        format!(
            "SELECT 2 AS scope, c.name AS column_name, \
             CASE tp.name \
               WHEN 'int' THEN 4 WHEN 'bigint' THEN -5 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN -6 \
               WHEN 'uniqueidentifier' THEN -11 WHEN 'nvarchar' THEN -9 WHEN 'varchar' THEN 12 \
               ELSE 0 END AS data_type, \
             tp.name AS type_name, \
             COALESCE(c.max_length, 0) AS column_size, \
             COALESCE(c.max_length, 0) AS buffer_length, \
             COALESCE(c.scale, 0) AS decimal_digits, \
             1 AS pseudo_column \
             FROM sys.tables t \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1 \
             JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
             JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
             JOIN sys.types tp ON c.system_type_id = tp.system_type_id AND c.user_type_id = tp.user_type_id \
             {} ORDER BY scope",
            where_clause
        )
    };
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetTypeInfo")]
fn ddbc_sql_get_type_info(stmt: &mut StatementHandle, sql_type: i32) -> PyResult<i32> {
    let type_filter = if sql_type == 0 {
        String::new()
    } else {
        format!("WHERE data_type = {}", sql_type)
    };
    let sql = format!(
        "SELECT type_name, data_type, CASE type_name \
           WHEN 'bigint' THEN 19 WHEN 'int' THEN 10 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN 3 \
           WHEN 'bit' THEN 1 WHEN 'float' THEN 53 WHEN 'real' THEN 24 \
           WHEN 'decimal' THEN 38 WHEN 'numeric' THEN 38 WHEN 'money' THEN 19 WHEN 'smallmoney' THEN 10 \
           WHEN 'char' THEN 8000 WHEN 'varchar' THEN 8000 WHEN 'text' THEN 2147483647 \
           WHEN 'nchar' THEN 4000 WHEN 'nvarchar' THEN 4000 WHEN 'ntext' THEN 1073741823 \
           WHEN 'binary' THEN 8000 WHEN 'varbinary' THEN 8000 WHEN 'image' THEN 2147483647 \
           WHEN 'datetime' THEN 23 WHEN 'smalldatetime' THEN 16 WHEN 'datetime2' THEN 27 \
           WHEN 'date' THEN 10 WHEN 'time' THEN 16 WHEN 'datetimeoffset' THEN 34 \
           WHEN 'uniqueidentifier' THEN 36 WHEN 'xml' THEN 0 ELSE 0 END AS column_size, \
         CASE WHEN type_name IN ('char','varchar','nchar','nvarchar','text','ntext','binary','varbinary','image') THEN '''' \
           WHEN type_name IN ('datetime','smalldatetime','datetime2','date','time','datetimeoffset') THEN '''' \
           ELSE NULL END AS literal_prefix, \
         CASE WHEN type_name IN ('char','varchar','nchar','nvarchar','text','ntext','binary','varbinary','image') THEN '''' \
           WHEN type_name IN ('datetime','smalldatetime','datetime2','date','time','datetimeoffset') THEN '''' \
           ELSE NULL END AS literal_suffix, \
         CASE WHEN type_name IN ('decimal','numeric') THEN 'precision,scale' \
           WHEN type_name IN ('char','varchar','binary','varbinary','nchar','nvarchar') THEN 'max length' \
           WHEN type_name IN ('float') THEN 'precision' \
           WHEN type_name IN ('datetime2','time','datetimeoffset') THEN 'scale' \
           ELSE NULL END AS create_params, \
         1 AS nullable, \
         CASE WHEN type_name IN ('char','varchar','nchar','nvarchar') THEN 1 ELSE 0 END AS case_sensitive, \
         3 AS searchable, \
         CASE WHEN type_name IN ('tinyint','bit') THEN 1 ELSE 0 END AS unsigned_attribute, \
         0 AS fixed_prec_scale, \
         CASE WHEN type_name IN ('int','bigint','smallint','tinyint','decimal','numeric') THEN 1 ELSE 0 END AS auto_unique_value, \
         type_name AS local_type_name, \
         0 AS minimum_scale, \
         CASE type_name WHEN 'decimal' THEN 38 WHEN 'numeric' THEN 38 WHEN 'datetime2' THEN 7 WHEN 'time' THEN 7 ELSE 0 END AS maximum_scale, \
         data_type AS sql_data_type, \
         CAST(NULL AS SMALLINT) AS sql_datetime_sub, \
         CASE WHEN type_name IN ('decimal','numeric') THEN 10 ELSE NULL END AS num_prec_radix, \
         0 AS interval_precision \
         FROM (SELECT name AS type_name, \
           CASE name \
             WHEN 'int' THEN 4 WHEN 'bigint' THEN -5 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN -6 \
             WHEN 'bit' THEN -7 WHEN 'float' THEN 6 WHEN 'real' THEN 7 \
             WHEN 'decimal' THEN 3 WHEN 'numeric' THEN 2 WHEN 'money' THEN 3 WHEN 'smallmoney' THEN 3 \
             WHEN 'char' THEN 1 WHEN 'varchar' THEN 12 WHEN 'text' THEN -1 \
             WHEN 'nchar' THEN -8 WHEN 'nvarchar' THEN -9 WHEN 'ntext' THEN -10 \
             WHEN 'binary' THEN -2 WHEN 'varbinary' THEN -3 WHEN 'image' THEN -4 \
             WHEN 'datetime' THEN 93 WHEN 'smalldatetime' THEN 93 WHEN 'datetime2' THEN 93 \
             WHEN 'date' THEN 91 WHEN 'time' THEN 92 WHEN 'datetimeoffset' THEN -155 \
             WHEN 'uniqueidentifier' THEN -11 WHEN 'xml' THEN -152 \
             ELSE 0 END AS data_type, \
           CASE name \
             WHEN 'datetime' THEN 9 WHEN 'smalldatetime' THEN 9 WHEN 'datetime2' THEN 9 \
             WHEN 'date' THEN 9 WHEN 'time' THEN 9 WHEN 'datetimeoffset' THEN 9 \
             ELSE CASE name WHEN 'int' THEN 4 WHEN 'bigint' THEN -5 WHEN 'smallint' THEN 5 WHEN 'tinyint' THEN -6 \
               WHEN 'bit' THEN -7 WHEN 'float' THEN 6 WHEN 'real' THEN 7 \
               WHEN 'decimal' THEN 3 WHEN 'numeric' THEN 2 WHEN 'money' THEN 3 WHEN 'smallmoney' THEN 3 \
               WHEN 'char' THEN 1 WHEN 'varchar' THEN 12 WHEN 'text' THEN -1 \
               WHEN 'nchar' THEN -8 WHEN 'nvarchar' THEN -9 WHEN 'ntext' THEN -10 \
               WHEN 'binary' THEN -2 WHEN 'varbinary' THEN -3 WHEN 'image' THEN -4 \
               WHEN 'uniqueidentifier' THEN -11 WHEN 'xml' THEN -152 ELSE 0 END END AS sql_data_type \
         FROM sys.types WHERE is_user_defined = 0 AND name != 'sysname') AS t {} ORDER BY data_type",
        type_filter
    );
    stmt.cursor.execute(&sql, &[])
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchScroll")]
fn ddbc_sql_fetch_scroll(
    stmt: &mut StatementHandle,
    orientation: i32,
    offset: i64,
    _row_data: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<i32> {
    let total_rows = stmt.cursor.row_count_total();
    match orientation {
        6 => {
            // SQL_FETCH_RELATIVE
            let new_pos = stmt.cursor.current_row_index() as i64 + offset;
            if new_pos < 0 || new_pos >= total_rows as i64 {
                return Ok(100);
            }
            stmt.cursor.set_row_index(new_pos as usize);
            Ok(0)
        }
        5 => {
            // SQL_FETCH_ABSOLUTE
            if offset < 0 || total_rows == 0 {
                stmt.cursor.set_row_index(0);
                return Ok(if total_rows == 0 { 100 } else { 0 });
            }
            if offset as usize >= total_rows {
                return Ok(100);
            }
            stmt.cursor.set_row_index(offset as usize);
            Ok(0)
        }
        _ => Ok(100),
    }
}

#[pyfunction]
#[pyo3(signature = (stmt, sql, columnwise_params, param_types, row_count))]
#[pyo3(name = "SQLExecuteMany")]
fn sql_execute_many(
    stmt: &mut StatementHandle,
    sql: &str,
    columnwise_params: Vec<Bound<'_, pyo3::types::PyList>>,
    param_types: Vec<Bound<'_, PyAny>>,
    row_count: usize,
) -> PyResult<i32> {
    let _ = param_types;
    stmt.cursor.execute_many(sql, &columnwise_params, row_count)
}

#[pymodule]
fn hiss_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConnection>()?;
    m.add_class::<StatementHandle>()?;
    m.add_class::<NumericData>()?;
    m.add_class::<ParamInfo>()?;
    m.add_function(wrap_pyfunction!(ddbc_sql_execute, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_row_count, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_describe_col, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_fetch_one, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_fetch_many, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_fetch_all, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_more_results, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_set_stmt_attr, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_get_all_diag_records, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_set_decimal_separator, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_tables, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_columns, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_primary_keys, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_foreign_keys, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_statistics, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_procedures, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_special_columns, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_get_type_info, m)?)?;
    m.add_function(wrap_pyfunction!(ddbc_sql_fetch_scroll, m)?)?;
    m.add_function(wrap_pyfunction!(sql_execute_many, m)?)?;
    Ok(())
}
