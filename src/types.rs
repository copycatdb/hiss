use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyString};
use std::cell::RefCell;
use crate::row_writer::CompactValue;

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
where F: FnOnce(Python<'_>, &DateTimeCache) -> PyResult<R> {
    DATETIME_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() { *opt = Some(get_datetime_cache(py)?); }
        f(py, opt.as_ref().unwrap())
    })
}

fn with_uuid_cls<F, R>(py: Python<'_>, f: F) -> PyResult<R>
where F: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R> {
    UUID_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() { *opt = Some(py.import("uuid")?.getattr("UUID")?.unbind()); }
        f(py, opt.as_ref().unwrap().bind(py))
    })
}

fn with_decimal_cls<F, R>(py: Python<'_>, f: F) -> PyResult<R>
where F: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R> {
    DECIMAL_CACHE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() { *opt = Some(py.import("decimal")?.getattr("Decimal")?.unbind()); }
        f(py, opt.as_ref().unwrap().bind(py))
    })
}

#[inline]
fn micros_to_components(micros: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let total_secs = micros.div_euclid(1_000_000);
    let remaining_micros = micros.rem_euclid(1_000_000) as u32;
    let time_of_day = total_secs.rem_euclid(86400) as u32;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let mut days = total_secs.div_euclid(86400) as i32;
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, hour, minute, second, remaining_micros)
}

fn decimal_i128_to_string(value: i128, scale: u8) -> String {
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let s = abs.to_string();
    let scale = scale as usize;
    let result = if scale == 0 { s }
    else if s.len() <= scale { format!("0.{}{}", "0".repeat(scale - s.len()), s) }
    else { let (i, f) = s.split_at(s.len() - scale); format!("{}.{}", i, f) };
    if negative { format!("-{}", result) } else { result }
}

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
            with_uuid_cls(py, |_py, cls| Ok(cls.call1((u.to_string(),))?.unbind()))
        }
        CompactValue::Decimal(value, _precision, scale) => {
            let s = decimal_i128_to_string(*value, *scale);
            with_decimal_cls(py, |_py, cls| Ok(cls.call1((s,))?.unbind()))
        }
        CompactValue::Date(unix_days) => {
            let days = *unix_days + 719468i32;
            let era = if days >= 0 { days } else { days - 146096 } / 146097;
            let doe = (days - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i32 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let year = if m <= 2 { y + 1 } else { y };
            with_datetime(py, |_py, cache| Ok(cache.date_cls.bind(py).call1((year, m, d))?.unbind()))
        }
        CompactValue::Time(nanos) => {
            let total_secs = (*nanos / 1_000_000_000) as u32;
            let micros = ((*nanos % 1_000_000_000) / 1000) as u32;
            let hour = total_secs / 3600;
            let minute = (total_secs % 3600) / 60;
            let second = total_secs % 60;
            with_datetime(py, |_py, cache| Ok(cache.time_cls.bind(py).call1((hour, minute, second, micros))?.unbind()))
        }
        CompactValue::DateTime(micros) => {
            let (year, month, day, hour, minute, second, remaining_micros) = micros_to_components(*micros);
            with_datetime(py, |_py, cache| Ok(cache.datetime_cls.bind(py).call1((year, month, day, hour, minute, second, remaining_micros))?.unbind()))
        }
        CompactValue::DateTimeOffset(micros, offset_minutes) => {
            let offset_micros = (*offset_minutes as i64) * 60 * 1_000_000;
            let local_micros = micros + offset_micros;
            let (year, month, day, hour, minute, second, remaining_micros) = micros_to_components(local_micros);
            with_datetime(py, |_py, cache| {
                let td = cache.timedelta_cls.bind(py).call1((0, *offset_minutes as i32 * 60))?;
                let tz = cache.timezone_cls.bind(py).call1((td,))?;
                Ok(cache.datetime_cls.bind(py).call1((year, month, day, hour, minute, second, remaining_micros, tz))?.unbind())
            })
        }
    }
}

/// Convert a Python parameter to a SQL literal string for substitution.
pub fn py_to_sql_literal(py: Python<'_>, param: &Bound<'_, PyAny>) -> PyResult<String> {
    if param.is_none() { return Ok("NULL".to_string()); }
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
    let is_decimal = with_decimal_cls(py, |_py, cls| param.is_instance(cls))?;
    if is_decimal {
        return Ok(param.str()?.to_string());
    }
    let is_datetime = with_datetime(py, |_py, cache| param.is_instance(cache.datetime_cls.bind(py)))?;
    if is_datetime {
        let year: i32 = param.getattr("year")?.extract()?;
        let month: u32 = param.getattr("month")?.extract()?;
        let day: u32 = param.getattr("day")?.extract()?;
        let hour: u32 = param.getattr("hour")?.extract()?;
        let minute: u32 = param.getattr("minute")?.extract()?;
        let second: u32 = param.getattr("second")?.extract()?;
        let microsecond: u32 = param.getattr("microsecond")?.extract()?;
        if microsecond > 0 {
            return Ok(format!("CAST('{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:07}' AS DATETIME2(7))", year, month, day, hour, minute, second, microsecond * 10));
        }
        return Ok(format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'", year, month, day, hour, minute, second));
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
    let is_uuid = with_uuid_cls(py, |_py, cls| param.is_instance(cls))?;
    if is_uuid {
        return Ok(format!("'{}'", param.str()?));
    }
    if param.is_instance_of::<PyBytes>() {
        let v: Vec<u8> = param.extract()?;
        let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
        return Ok(format!("0x{}", hex));
    }
    if param.is_instance_of::<PyString>() {
        let v: String = param.extract()?;
        return Ok(format!("N'{}'", v.replace('\'', "''")));
    }
    let s = param.str()?.to_string();
    Ok(format!("N'{}'", s.replace('\'', "''")))
}
