//! PyRowWriter: Direct TDS wire → Python objects via tabby's RowWriter trait.
//!
//! Path: TDS bytes → decode_direct → CompactValue → PyObject
//! No SqlValue enum. No claw. Just tabby + hiss.

use tabby::RowWriter;

/// Compact value — the only intermediate between TDS wire and Python.
#[derive(Clone)]
pub enum CompactValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    DateTime(i64),
    DateTimeOffset(i64, i16),
    Decimal(i128, u8, u8),
    Guid([u8; 16]),
}

/// Single result set: column info + flat CompactValue storage.
pub struct PyRowWriter {
    pub col_count: usize,
    pub values: Vec<CompactValue>,
    current_row: Vec<CompactValue>,
}

impl PyRowWriter {
    pub fn new(col_count: usize) -> Self {
        Self {
            col_count,
            values: Vec::with_capacity(col_count * 64),
            current_row: Vec::with_capacity(col_count),
        }
    }

    pub fn row_count(&self) -> usize {
        if self.col_count == 0 {
            0
        } else {
            self.values.len() / self.col_count
        }
    }

    #[inline]
    pub fn get(&self, row: usize, col: usize) -> &CompactValue {
        &self.values[row * self.col_count + col]
    }

    fn finish_row(&mut self) {
        self.values.append(&mut self.current_row);
    }

    #[inline]
    fn push(&mut self, val: CompactValue) {
        self.current_row.push(val);
    }
}

/// Multi-result-set writer: accumulates complete result sets via on_metadata/on_row_done.
/// Used with tabby's batch_into() which keeps a single writer across all result sets.
pub struct MultiSetWriter {
    pub completed: Vec<(Vec<crate::cursor::ColumnInfo>, PyRowWriter)>,
    pub messages: Vec<(String, String)>,
    current_cols: Option<Vec<crate::cursor::ColumnInfo>>,
    current: Option<PyRowWriter>,
}

impl MultiSetWriter {
    pub fn new() -> Self {
        Self {
            completed: Vec::new(),
            messages: Vec::new(),
            current_cols: None,
            current: None,
        }
    }

    pub fn finalize(mut self) -> Vec<(Vec<crate::cursor::ColumnInfo>, PyRowWriter)> {
        if let (Some(cols), Some(writer)) = (self.current_cols.take(), self.current.take()) {
            self.completed.push((cols, writer));
        }
        self.completed
    }
}

impl RowWriter for MultiSetWriter {
    fn on_metadata(&mut self, columns: &[tabby::Column]) {
        // Stash previous result set
        if let (Some(cols), Some(writer)) = (self.current_cols.take(), self.current.take()) {
            self.completed.push((cols, writer));
        }
        let col_infos: Vec<crate::cursor::ColumnInfo> = columns
            .iter()
            .map(crate::cursor::TdsCursor::column_to_info)
            .collect();
        let col_count = columns.len();
        self.current_cols = Some(col_infos);
        self.current = Some(PyRowWriter::new(col_count));
    }

    fn on_row_done(&mut self) {
        if let Some(ref mut w) = self.current {
            w.finish_row();
        }
    }

    fn on_info(&mut self, number: u32, message: &str) {
        let header = format!("[01000] ({})", number);
        self.messages.push((header, message.to_owned()));
    }

    #[inline]
    fn write_null(&mut self, _col: usize) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Null);
        }
    }
    #[inline]
    fn write_bool(&mut self, _col: usize, val: bool) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Bool(val));
        }
    }
    #[inline]
    fn write_u8(&mut self, _col: usize, val: u8) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::I64(val as i64));
        }
    }
    #[inline]
    fn write_i16(&mut self, _col: usize, val: i16) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::I64(val as i64));
        }
    }
    #[inline]
    fn write_i32(&mut self, _col: usize, val: i32) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::I64(val as i64));
        }
    }
    #[inline]
    fn write_i64(&mut self, _col: usize, val: i64) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::I64(val));
        }
    }
    #[inline]
    fn write_f32(&mut self, _col: usize, val: f32) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::F64(val as f64));
        }
    }
    #[inline]
    fn write_f64(&mut self, _col: usize, val: f64) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::F64(val));
        }
    }
    #[inline]
    fn write_str(&mut self, _col: usize, val: &str) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Str(val.to_owned()));
        }
    }
    #[inline]
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Bytes(val.to_owned()));
        }
    }
    #[inline]
    fn write_date(&mut self, _col: usize, days: i32) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Date(days));
        }
    }
    #[inline]
    fn write_time(&mut self, _col: usize, nanos: i64) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Time(nanos));
        }
    }
    #[inline]
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::DateTime(micros));
        }
    }
    #[inline]
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::DateTimeOffset(micros, offset_minutes));
        }
    }
    #[inline]
    fn write_decimal(&mut self, _col: usize, value: i128, precision: u8, scale: u8) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Decimal(value, precision, scale));
        }
    }
    #[inline]
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        if let Some(ref mut w) = self.current {
            w.push(CompactValue::Guid(*bytes));
        }
    }
}
