use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyString};
use std::cell::RefCell;
use tabby::SqlValue;

use crate::row_writer::CompactValue;

// Cached Python module/class references — avoids repeated py.import() per value.
// Like pyodbc's static caches but thread-local for safety.
thread_local! {
    static DATETIME_CACHE: RefCell<Option<DateTimeCache>> = const { RefCell::new(None) };
    static UUID_CACHE: RefCell<Option<PyObject>> = const { RefCell::new(None) };
    static DECIMAL_CACHE: RefCell<Option<PyObject>> = const { RefCell::new(None) };
}

struct DateTimeCache {
    datetime_cls: PyObject,
    date_cls: PyObject,
    time_cls: PyObject,
    timedelta_cls: PyObject,
    timezone_cls: PyObject,
}

fn get_datetime_cache(py: Python<'_>) -> PyResult<DateTimeCache> {
    let m = py.import("datetime")?;
    Ok(DateTimeCache {
        datetime_cls: m.getattr("datetime")?.unbind(),
        date_cls: m.getattr("date")?.unbind(),
        time_cls: m.getattr("time")?.unbind(),
        timedelta_cls: m.getattr("timedelta")?.unbind(),
        timezone_cls: m.getattr("timezone")?.unbind(),
    })
}

fn with_datetime<F, R>(py: Python<'_>, f: F) -> PyResult<R>
where
    F: FnOnce(Python<'_>, &DateTimeCache) -> PyResult<R>,
{
    DATETIME_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            *opt = Some(get_datetime_cache(py)?);
        }
        f(py, opt.as_ref().unwrap())
    })
}

fn with_uuid_cls<F, R>(py: Python<'_>, f: F) -> PyResult<R>
where
    F: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R>,
{
    UUID_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            *opt = Some(py.import("uuid")?.getattr("UUID")?.unbind());
        }
        let bound = opt.as_ref().unwrap().bind(py);
        f(py, bound)
    })
}

fn with_decimal_cls<F, R>(py: Python<'_>, f: F) -> PyResult<R>
where
    F: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R>,
{
    DECIMAL_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            *opt = Some(py.import("decimal")?.getattr("Decimal")?.unbind());
        }
        let bound = opt.as_ref().unwrap().bind(py);
        f(py, bound)
    })
}

/// Convert a tabby SqlValue to a Python object.
/// Uses cached module references (pyodbc technique) to avoid repeated imports.
#[inline]
#[allow(dead_code)]
pub fn sql_value_to_py(py: Python<'_>, data: &SqlValue<'static>) -> PyResult<PyObject> {
    match data {
        // Fast path: primitives — direct PyObject creation, no module imports
        SqlValue::Bit(Some(v)) => Ok(PyBool::new(py, *v).to_owned().into_any().unbind()),
        SqlValue::Bit(None)
        | SqlValue::U8(None)
        | SqlValue::I16(None)
        | SqlValue::I32(None)
        | SqlValue::I64(None)
        | SqlValue::F32(None)
        | SqlValue::F64(None)
        | SqlValue::String(None)
        | SqlValue::Binary(None)
        | SqlValue::Guid(None)
        | SqlValue::Numeric(None)
        | SqlValue::DateTime(None)
        | SqlValue::SmallDateTime(None)
        | SqlValue::DateTime2(None)
        | SqlValue::Date(None)
        | SqlValue::Time(None)
        | SqlValue::DateTimeOffset(None)
        | SqlValue::Xml(None) => Ok(py.None()),

        SqlValue::U8(Some(v)) => Ok((*v as i64).into_pyobject(py)?.into_any().unbind()),
        SqlValue::I16(Some(v)) => Ok((*v as i64).into_pyobject(py)?.into_any().unbind()),
        SqlValue::I32(Some(v)) => Ok((*v as i64).into_pyobject(py)?.into_any().unbind()),
        SqlValue::I64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::F32(Some(v)) => Ok((*v as f64).into_pyobject(py)?.into_any().unbind()),
        SqlValue::F64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),

        SqlValue::String(Some(v)) => Ok(PyString::new(py, v.as_ref()).into_any().unbind()),
        SqlValue::Binary(Some(v)) => Ok(PyBytes::new(py, v.as_ref()).into_any().unbind()),

        SqlValue::Guid(Some(v)) => {
            let s = v.to_string();
            with_uuid_cls(py, |_py, cls| Ok(cls.call1((s,))?.unbind()))
        }

        SqlValue::Numeric(Some(v)) => {
            let s = v.to_string();
            with_decimal_cls(py, |_py, cls| Ok(cls.call1((s,))?.unbind()))
        }

        SqlValue::DateTime(Some(dt)) => {
            let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            let days = dt.days() as i64;
            let ticks = dt.seconds_fragments() as i64;
            let total_ms = ticks * 1000 / 300;
            let secs = (total_ms / 1000) as u32;
            let micros = ((total_ms % 1000) * 1000) as u32;
            let date = base + chrono::Duration::days(days);
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, micros * 1000)
                .unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((
                        ndt.year(),
                        ndt.month(),
                        ndt.day(),
                        ndt.hour(),
                        ndt.minute(),
                        ndt.second(),
                        micros,
                    ))?
                    .unbind())
            })
        }

        SqlValue::SmallDateTime(Some(dt)) => {
            let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            let days = dt.days() as i64;
            let mins = dt.seconds_fragments() as i64;
            let date = base + chrono::Duration::days(days);
            let time = NaiveTime::from_num_seconds_from_midnight_opt((mins * 60) as u32, 0)
                .unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((
                        ndt.year(),
                        ndt.month(),
                        ndt.day(),
                        ndt.hour(),
                        ndt.minute(),
                        ndt.second(),
                        0u32,
                    ))?
                    .unbind())
            })
        }

        SqlValue::DateTime2(Some(dt)) => {
            let d = dt.date();
            let t = dt.time();
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let remaining_nanos = (nanos % 1_000_000_000) as u32;
            let micros = remaining_nanos / 1000;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, remaining_nanos)
                .unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((
                        ndt.year(),
                        ndt.month(),
                        ndt.day(),
                        ndt.hour(),
                        ndt.minute(),
                        ndt.second(),
                        micros,
                    ))?
                    .unbind())
            })
        }

        SqlValue::Date(Some(d)) => {
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .date_cls
                    .bind(py)
                    .call1((date.year(), date.month(), date.day()))?
                    .unbind())
            })
        }

        SqlValue::Time(Some(t)) => {
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let remaining_nanos = (nanos % 1_000_000_000) as u32;
            let micros = remaining_nanos / 1000;
            let hour = secs / 3600;
            let minute = (secs % 3600) / 60;
            let second = secs % 60;
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .time_cls
                    .bind(py)
                    .call1((hour, minute, second, micros))?
                    .unbind())
            })
        }

        SqlValue::DateTimeOffset(Some(dto)) => {
            let d = dto.datetime2().date();
            let t = dto.datetime2().time();
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let remaining_nanos = (nanos % 1_000_000_000) as u32;
            let micros = remaining_nanos / 1000;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, remaining_nanos)
                .unwrap_or_default();
            let utc_ndt = NaiveDateTime::new(date, time);
            let offset_mins = dto.offset() as i32;
            let local_ndt = utc_ndt + chrono::Duration::minutes(offset_mins as i64);
            with_datetime(py, |_py, cache| {
                let td = cache.timedelta_cls.bind(py).call1((0, offset_mins * 60))?;
                let tz = cache.timezone_cls.bind(py).call1((td,))?;
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((
                        local_ndt.year(),
                        local_ndt.month(),
                        local_ndt.day(),
                        local_ndt.hour(),
                        local_ndt.minute(),
                        local_ndt.second(),
                        micros,
                        tz,
                    ))?
                    .unbind())
            })
        }

        SqlValue::Xml(Some(x)) => {
            let s = format!("{}", x);
            Ok(PyString::new(py, &s).into_any().unbind())
        }
    }
}

/// Convert a CompactValue (from PyRowWriter / direct decode) to a Python object.
/// This is the fast path — no SqlValue enum, pre-normalized temporal values.
#[inline]
pub fn compact_value_to_py(py: Python<'_>, val: &CompactValue) -> PyResult<PyObject> {
    match val {
        CompactValue::Null => Ok(py.None()),
        CompactValue::Bool(v) => Ok(PyBool::new(py, *v).to_owned().into_any().unbind()),
        CompactValue::I64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        CompactValue::F64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        CompactValue::Str(v) => Ok(PyString::new(py, v).into_any().unbind()),
        CompactValue::Bytes(v) => Ok(PyBytes::new(py, v).into_any().unbind()),
        CompactValue::Guid(bytes) => {
            let u = uuid::Uuid::from_bytes(*bytes);
            let s = u.to_string();
            with_uuid_cls(py, |_py, cls| Ok(cls.call1((s,))?.unbind()))
        }
        CompactValue::Decimal(value, _precision, scale) => {
            // Convert i128 + scale to string like "123.45"
            let s = decimal_i128_to_string(*value, *scale);
            with_decimal_cls(py, |_py, cls| Ok(cls.call1((s,))?.unbind()))
        }
        CompactValue::Date(unix_days) => {
            // unix_days = days since Unix epoch (1970-01-01)
            // Use same civil calendar algorithm
            let days = *unix_days + 719468i32; // shift to 0000-03-01 epoch
            let era = if days >= 0 { days } else { days - 146096 } / 146097;
            let doe = (days - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i32 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let year = if m <= 2 { y + 1 } else { y };
            with_datetime(py, |_py, cache| {
                Ok(cache.date_cls.bind(py).call1((year, m, d))?.unbind())
            })
        }
        CompactValue::Time(nanos) => {
            let total_secs = (*nanos / 1_000_000_000) as u32;
            let remaining_nanos = (*nanos % 1_000_000_000) as u32;
            let micros = remaining_nanos / 1000;
            let hour = total_secs / 3600;
            let minute = (total_secs % 3600) / 60;
            let second = total_secs % 60;
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .time_cls
                    .bind(py)
                    .call1((hour, minute, second, micros))?
                    .unbind())
            })
        }
        CompactValue::DateTime(micros) => {
            let (year, month, day, hour, minute, second, remaining_micros) =
                micros_to_components(*micros);
            with_datetime(py, |_py, cache| {
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((year, month, day, hour, minute, second, remaining_micros))?
                    .unbind())
            })
        }
        CompactValue::DateTimeOffset(micros, offset_minutes) => {
            let offset_micros = (*offset_minutes as i64) * 60 * 1_000_000;
            let local_micros = micros + offset_micros;
            let (year, month, day, hour, minute, second, remaining_micros) =
                micros_to_components(local_micros);
            with_datetime(py, |_py, cache| {
                let td = cache
                    .timedelta_cls
                    .bind(py)
                    .call1((0, *offset_minutes as i32 * 60))?;
                let tz = cache.timezone_cls.bind(py).call1((td,))?;
                Ok(cache
                    .datetime_cls
                    .bind(py)
                    .call1((year, month, day, hour, minute, second, remaining_micros, tz))?
                    .unbind())
            })
        }
    }
}

/// Decompose microseconds since Unix epoch into (year, month, day, hour, min, sec, micros).
/// Pure arithmetic — no chrono allocations.
#[inline]
fn micros_to_components(micros: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let total_secs = micros.div_euclid(1_000_000);
    let remaining_micros = micros.rem_euclid(1_000_000) as u32;

    let time_of_day = total_secs.rem_euclid(86400) as u32;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;

    // Days since Unix epoch
    let mut days = total_secs.div_euclid(86400) as i32;
    // Shift to March 1, year 0 epoch for easier calendar math
    days += 719468; // days from 0000-03-01 to 1970-01-01
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    (year, m, d, hour, minute, second, remaining_micros)
}

/// Convert i128 + scale to decimal string like "-123.45"
fn decimal_i128_to_string(value: i128, scale: u8) -> String {
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let s = abs.to_string();
    let scale = scale as usize;
    let result = if scale == 0 {
        s
    } else if s.len() <= scale {
        let padding = "0".repeat(scale - s.len());
        format!("0.{}{}", padding, s)
    } else {
        let (int_part, frac_part) = s.split_at(s.len() - scale);
        format!("{}.{}", int_part, frac_part)
    };
    if negative {
        format!("-{}", result)
    } else {
        result
    }
}

/// Convert a Python parameter to a SQL literal string for substitution.
/// Caches module lookups via thread-local storage.
pub fn py_to_sql_literal(py: Python<'_>, param: &Bound<'_, PyAny>) -> PyResult<String> {
    if param.is_none() {
        return Ok("NULL".to_string());
    }

    // Check bool before int (bool is subclass of int in Python)
    if param.is_instance_of::<PyBool>() {
        let v: bool = param.extract()?;
        return Ok(if v { "1".to_string() } else { "0".to_string() });
    }

    if param.is_instance_of::<PyInt>() {
        let v: i64 = param.extract()?;
        return Ok(v.to_string());
    }

    if param.is_instance_of::<PyFloat>() {
        let v: f64 = param.extract()?;
        return Ok(format!("CAST({} AS FLOAT)", v));
    }

    // Check for decimal.Decimal
    let is_decimal = with_decimal_cls(py, |_py, cls| param.is_instance(cls))?;
    if is_decimal {
        let s = param.str()?.to_string();
        return Ok(s);
    }

    // Check for datetime types (datetime before date since datetime is subclass of date)
    let is_datetime = with_datetime(py, |_py, cache| {
        param.is_instance(cache.datetime_cls.bind(py))
    })?;
    if is_datetime {
        return datetime_to_literal(py, param);
    }

    let is_date = with_datetime(py, |_py, cache| param.is_instance(cache.date_cls.bind(py)))?;
    if is_date {
        let s = param.call_method0("isoformat")?.str()?.to_string();
        return Ok(format!("'{}'", s));
    }

    let is_time = with_datetime(py, |_py, cache| param.is_instance(cache.time_cls.bind(py)))?;
    if is_time {
        let s = param.call_method0("isoformat")?.str()?.to_string();
        return Ok(format!("'{}'", s));
    }

    // uuid.UUID
    let is_uuid = with_uuid_cls(py, |_py, cls| param.is_instance(cls))?;
    if is_uuid {
        let s = param.str()?.to_string();
        return Ok(format!("'{}'", s));
    }

    // bytes/bytearray
    if param.is_instance_of::<PyBytes>() {
        let v: Vec<u8> = param.extract()?;
        let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
        return Ok(format!("0x{}", hex));
    }

    if let Ok(v) = param.extract::<Vec<u8>>() {
        let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
        return Ok(format!("0x{}", hex));
    }

    // String (check last since many types convert to string)
    if param.is_instance_of::<PyString>() {
        let v: String = param.extract()?;
        let escaped = v.replace('\'', "''");
        return Ok(format!("N'{}'", escaped));
    }

    // NumericData struct
    if let Ok(nd) = param.extract::<crate::NumericData>() {
        let val = nd.val;
        let scale = nd.scale;
        let sign = if nd.sign == 0 { "-" } else { "" };
        if scale > 0 {
            let s = format!("{}", val);
            let len = s.len();
            if len > scale as usize {
                let (int_part, frac_part) = s.split_at(len - scale as usize);
                return Ok(format!("{}{}.{}", sign, int_part, frac_part));
            } else {
                let padding = "0".repeat(scale as usize - len);
                return Ok(format!("{}0.{}{}", sign, padding, s));
            }
        } else {
            return Ok(format!("{}{}", sign, val));
        }
    }

    // Fallback
    let s = param.str()?.to_string();
    let escaped = s.replace('\'', "''");
    Ok(format!("N'{}'", escaped))
}

fn datetime_to_literal(_py: Python<'_>, param: &Bound<'_, PyAny>) -> PyResult<String> {
    let year: i32 = param.getattr("year")?.extract()?;
    let month: u32 = param.getattr("month")?.extract()?;
    let day: u32 = param.getattr("day")?.extract()?;
    let hour: u32 = param.getattr("hour")?.extract()?;
    let minute: u32 = param.getattr("minute")?.extract()?;
    let second: u32 = param.getattr("second")?.extract()?;
    let microsecond: u32 = param.getattr("microsecond")?.extract()?;

    let tzinfo = param.getattr("tzinfo")?;
    if !tzinfo.is_none() {
        let utcoffset = param.call_method0("utcoffset")?;
        let total_seconds: f64 = utcoffset.call_method0("total_seconds")?.extract()?;
        let total_seconds_i = total_seconds as i64;
        let offset_hours = total_seconds_i / 3600;
        let offset_minutes = (total_seconds_i.abs() % 3600) / 60;
        let sign = if total_seconds_i >= 0 { "+" } else { "-" };
        if microsecond > 0 {
            return Ok(format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:07}{}{:02}:{:02}'",
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond * 10,
                sign,
                offset_hours.abs(),
                offset_minutes
            ));
        } else {
            return Ok(format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}{}{:02}:{:02}'",
                year,
                month,
                day,
                hour,
                minute,
                second,
                sign,
                offset_hours.abs(),
                offset_minutes
            ));
        }
    }

    if microsecond > 0 {
        if microsecond % 1000 == 0 {
            let millis = microsecond / 1000;
            return Ok(format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}'",
                year, month, day, hour, minute, second, millis
            ));
        } else {
            return Ok(format!(
                "CAST('{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:07}' AS DATETIME2(7))",
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond * 10
            ));
        }
    } else {
        Ok(format!(
            "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
            year, month, day, hour, minute, second
        ))
    }
}

/// Get the SQL type code for a tabby column type
pub fn column_type_to_sql_type(type_name: &str) -> i32 {
    match type_name {
        "Int4" => 4,
        "Int2" => 5,
        "Int1" => -6,
        "Int8" | "Intn" => -5,
        "Float8" | "Floatn" => 8,
        "Float4" => 7,
        "Bit" | "Bitn" => -7,
        "BigVarChar" | "NVarchar" => -9,
        "BigChar" | "NChar" => -8,
        "Text" => -1,
        "NText" => -10,
        "BigBinary" => -2,
        "BigVarBin" => -3,
        "Image" => -4,
        "Decimaln" | "Numericn" | "Money" | "Money4" => 3,
        "Datetime" | "Datetimen" | "Datetime4" | "Datetime2" => 93,
        "Daten" => 91,
        "Timen" => 92,
        "DatetimeOffsetn" => -155,
        "Guid" => -11,
        "Xml" => -152,
        "SSVariant" => -150,
        _ => 12, // SQL_VARCHAR default
    }
}
