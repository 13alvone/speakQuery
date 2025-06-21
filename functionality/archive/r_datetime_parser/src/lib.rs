use chrono::{NaiveDate, NaiveDateTime, Utc, DateTime, TimeZone};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

#[pyfunction]
fn parse_dates_to_epoch(dates: Vec<String>) -> PyResult<Vec<i64>> {
    let formats = vec![
        "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S%.f",
        "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S%.f", "%m-%d-%Y %H:%M:%S",
        "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y",
        "%d-%m-%Y %H:%M:%S%.f", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S%.f",
        "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S%.f", "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d", // Date only
        "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S",
        "%B %d, %Y %H:%M:%S%.f", "%B %d, %Y %H:%M:%S",
        "%d %B %Y %H:%M:%S%.f", "%d %B %Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p", "%m-%d-%Y %I:%M:%S %p",
        "%Y%m%d%H%M%S", "%Y-W%W-%w %H:%M:%S%.f", "%Y-W%U-%w %H:%M:%S%.f"
    ];

    Ok(dates.iter().map(|date_str| {
        for fmt in &formats {
            if fmt.contains("H") || fmt.contains("M") || fmt.contains("S") || fmt.contains("p") {
                // Attempt datetime parsing
                if let Ok(naive_dt) = NaiveDateTime::parse_from_str(date_str, fmt) {
                    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive_dt);
                    return utc_dt.timestamp();
                }
            } else {
                // Attempt date parsing
                if let Ok(naive_date) = NaiveDate::parse_from_str(date_str, fmt) {
                    let naive_dt = naive_date.and_hms(0, 0, 0);
                    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive_dt);
                    return utc_dt.timestamp();
                }
            }
        }
        println!("Failed to parse: '{}', attempted formats: {:?}", date_str, formats);
        0 // Return 0 if all parsing attempts fail
    }).collect())
}

#[pymodule]
fn r_datetime_parser(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_dates_to_epoch, m)?)?;
    Ok(())
}
