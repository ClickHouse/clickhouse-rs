//! Writes the values that clickhouse-rs decoded as the text that the server
//! writes for the `TabSeparated` format. The upstream `.reference` files hold
//! the server's text, so a test passes only when the client decoded every
//! value correctly.
//!
//! The rendering is type-directed: it follows the [`DataTypeNode`] of the
//! column, which the client parsed from the block header. The value alone is
//! not sufficient (for example, a `Date` is a `UInt16` on the wire).
//!
//! ClickHouse uses two text forms:
//! - top level (`serializeTextEscaped`): strings are escaped, not quoted, and
//!   NULL is `\N`;
//! - nested in `Array`, `Tuple` and `Map` (`serializeTextQuoted`): strings
//!   are quoted, and NULL is `NULL`.

use crate::value::Value;
use chrono::TimeZone;
use clickhouse::native::DataTypeNode;
use std::fmt::Write as _;
use std::net::Ipv6Addr;

/// `write!` into a `Vec<u8>`; formatting into a `String` cannot fail.
macro_rules! put {
    ($out:expr, $($arg:tt)*) => {{
        let mut s = String::new();
        let _ = write!(s, $($arg)*);
        $out.extend_from_slice(s.as_bytes());
    }};
}

/// What the renderer needs besides the type and the value.
#[derive(Debug, Clone, Default)]
pub(crate) struct Context {
    /// The time zone of a `DateTime` or `DateTime64` without a time zone in
    /// its type. The server uses its own time zone (or `session_timezone`),
    /// but clickhouse-rs does not tell it (the HTTP response has it in the
    /// `X-ClickHouse-Timezone` header). The shim uses UTC, the time zone of
    /// the CI server.
    pub(crate) default_timezone: Option<chrono_tz::Tz>,
}

/// Renders one top-level value. With `raw` (the `TSVRaw` formats), a
/// top-level string is written without escapes.
pub(crate) fn render_cell(
    out: &mut Vec<u8>,
    ty: &DataTypeNode,
    v: &Value<'_>,
    raw: bool,
    ctx: &Context,
) -> Result<(), String> {
    if raw
        && let Value::Bytes(s) = v
        && is_string_type(ty)
    {
        out.extend_from_slice(s);
        return Ok(());
    }
    render_value(out, ty, v, false, ctx)
}

fn is_string_type(ty: &DataTypeNode) -> bool {
    matches!(
        crate::value::unwrap_value_type(ty),
        DataTypeNode::String | DataTypeNode::FixedString(_)
    )
}

fn mismatch(ty: &DataTypeNode, v: &Value<'_>) -> String {
    format!("clickhouse-test-runner: unexpected value {v:?} for the data type {ty}")
}

pub(crate) fn render_value(
    out: &mut Vec<u8>,
    ty: &DataTypeNode,
    v: &Value<'_>,
    nested: bool,
    ctx: &Context,
) -> Result<(), String> {
    use DataTypeNode as T;
    if let Value::Null = v {
        out.extend_from_slice(if nested { b"NULL" } else { b"\\N" });
        return Ok(());
    }
    match (ty, v) {
        (T::Nullable(inner) | T::LowCardinality(inner), _) => {
            return render_value(out, inner, v, nested, ctx);
        }
        (T::SimpleAggregateFunction(_, inner), _) => {
            return render_value(out, inner, v, nested, ctx);
        }
        (T::Bool, Value::Bool(b)) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        (T::Int8 | T::Int16 | T::Int32 | T::Int64 | T::Int128 | T::Interval(_), Value::Int(n)) => {
            put!(out, "{n}")
        }
        (T::UInt8 | T::UInt16 | T::UInt32 | T::UInt64 | T::UInt128, Value::UInt(n)) => {
            put!(out, "{n}")
        }
        (T::Int256, Value::Wide(bytes)) => {
            out.extend_from_slice(wide_to_string(bytes, true).as_bytes())
        }
        (T::UInt256, Value::Wide(bytes)) => {
            out.extend_from_slice(wide_to_string(bytes, false).as_bytes())
        }
        (T::Float32, Value::Float32(f)) => out.extend_from_slice(format_float32(*f).as_bytes()),
        (T::Float64, Value::Float64(f)) => out.extend_from_slice(format_float64(*f).as_bytes()),
        (T::BFloat16, Value::BFloat16(bits)) => {
            let f = f32::from_bits(u32::from(*bits) << 16);
            out.extend_from_slice(format_float32(f).as_bytes())
        }
        (T::Decimal(_, scale, _), Value::Int(n)) => {
            let text = decimal_text(*n < 0, n.unsigned_abs().to_string(), *scale);
            out.extend_from_slice(text.as_bytes())
        }
        (T::Decimal(_, scale, _), Value::Wide(bytes)) => {
            let (neg, digits) = wide_digits(bytes, true);
            out.extend_from_slice(decimal_text(neg, digits, *scale).as_bytes())
        }
        (T::String | T::FixedString(_), Value::Bytes(s)) => write_string(out, s, nested),
        (T::Enum(_, names), Value::Int(n)) => {
            let name = i16::try_from(*n)
                .ok()
                .and_then(|n| names.get(&n))
                .ok_or_else(|| {
                    format!("clickhouse-test-runner: no name for the value {n} of {ty}")
                })?;
            write_string(out, name.as_bytes(), nested)
        }
        (T::UUID, Value::Uuid(u)) => write_quoted_if(out, &u.to_string(), nested),
        (T::IPv4, Value::Ipv4(ip)) => write_quoted_if(out, &ip.to_string(), nested),
        (T::IPv6, Value::Ipv6(ip)) => write_quoted_if(out, &format_ipv6(ip), nested),
        (T::Date, Value::UInt(days)) => write_quoted_if(out, &format_date(*days as i64), nested),
        (T::Date32, Value::Int(days)) => write_quoted_if(out, &format_date(*days as i64), nested),
        (T::DateTime(tz), Value::UInt(secs)) => {
            let text = format_datetime(*secs as i64, 0, 0, tz.as_deref(), ctx)?;
            write_quoted_if(out, &text, nested)
        }
        (T::DateTime64(precision, tz), Value::Int(ticks)) => {
            let p = precision_digits(precision);
            let scale = 10i64.pow(p);
            let ticks = *ticks as i64;
            let text = format_datetime(
                ticks.div_euclid(scale),
                ticks.rem_euclid(scale),
                p,
                tz.as_deref(),
                ctx,
            )?;
            write_quoted_if(out, &text, nested)
        }
        (T::Time, Value::Int(secs)) => write_quoted_if(out, &format_time(*secs as i64, 0), nested),
        (T::Time64(precision), Value::Int(ticks)) => write_quoted_if(
            out,
            &format_time(*ticks as i64, precision_digits(precision)),
            nested,
        ),
        (T::Point, Value::Point(x, y)) => {
            put!(out, "({},{})", format_float64(*x), format_float64(*y))
        }
        (T::Array(inner), Value::Array(items)) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                render_value(out, inner, item, true, ctx)?;
            }
            out.push(b']');
        }
        (T::Tuple(types), Value::Tuple(items)) => {
            if types.len() != items.len() {
                return Err(mismatch(ty, v));
            }
            out.push(b'(');
            for (i, (t, item)) in types.iter().zip(items).enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                render_value(out, t, item, true, ctx)?;
            }
            out.push(b')');
        }
        (T::Map([kt, vt]), Value::Map(entries)) => {
            out.push(b'{');
            for (i, (k, val)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                render_value(out, kt, k, true, ctx)?;
                out.push(b':');
                render_value(out, vt, val, true, ctx)?;
            }
            out.push(b'}');
        }
        _ => return Err(mismatch(ty, v)),
    }
    Ok(())
}

/// The number of fractional digits of a `DateTime64` or `Time64`.
/// (`clickhouse` does not re-export `DateTimePrecision`; its `Display`
/// writes the digit.)
fn precision_digits(p: &impl std::fmt::Display) -> u32 {
    p.to_string().parse().unwrap_or(0)
}

/// Writes `s` with the escapes of ClickHouse's `writeAnyEscapedString` (with
/// the single quote as the quote character), quoted when nested.
pub(crate) fn write_string(out: &mut Vec<u8>, s: &[u8], nested: bool) {
    if nested {
        out.push(b'\'');
    }
    for &c in s {
        match c {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\'' => out.extend_from_slice(b"\\'"),
            b'\x08' => out.extend_from_slice(b"\\b"),
            b'\x0c' => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0 => out.extend_from_slice(b"\\0"),
            _ => out.push(c),
        }
    }
    if nested {
        out.push(b'\'');
    }
}

/// Writes text that has no characters to escape (dates, UUIDs, addresses),
/// quoted when nested.
fn write_quoted_if(out: &mut Vec<u8>, s: &str, nested: bool) {
    if nested {
        out.push(b'\'');
    }
    out.extend_from_slice(s.as_bytes());
    if nested {
        out.push(b'\'');
    }
}

/// Writes a float as ClickHouse does: the shortest text that reads back as
/// the same value, in positional notation when the decimal exponent is in
/// [-6, 21), else in scientific notation without a `+` sign or exponent
/// padding (`1e21`, `1.5e-7`). This is the layout of JavaScript's
/// `Number.prototype.toString`.
pub(crate) fn format_float64(f: f64) -> String {
    // The server writes `nan` also for a NaN with the sign bit set (the
    // result of `0/0`, for example).
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.into();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0" } else { "0" }.into();
    }
    // `{:e}` writes the shortest digits that read back as the same value.
    layout_float(&format!("{f:e}"))
}

pub(crate) fn format_float32(f: f32) -> String {
    if f.is_nan() || f.is_infinite() || f == 0.0 {
        return format_float64(f64::from(f));
    }
    layout_float(&format!("{f:e}"))
}

/// Lays out `d.ddddde-X` (the output of `{:e}`) as JavaScript does.
fn layout_float(sci: &str) -> String {
    let (neg, s) = match sci.strip_prefix('-') {
        Some(s) => (true, s),
        None => (false, sci),
    };
    let (mantissa, exp) = s.split_once('e').unwrap_or((s, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|&c| c != '.').collect();
    // The value is 0.digits × 10^n.
    let n = exp + 1;
    let k = digits.len() as i32;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let mut t = digits[..1].to_owned();
        if k > 1 {
            t.push('.');
            t.push_str(&digits[1..]);
        }
        format!("{t}e{}", n - 1)
    };
    if neg { format!("-{body}") } else { body }
}

/// Writes a decimal from its sign, the digits of its absolute unscaled value
/// and its scale, without trailing zeros (`1.5`, not `1.50`).
pub(crate) fn decimal_text(neg: bool, digits: String, scale: u8) -> String {
    let scale = usize::from(scale);
    let digits = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits
    };
    let (int, frac) = digits.split_at(digits.len() - scale);
    let frac = frac.trim_end_matches('0');
    let is_zero = int.bytes().all(|c| c == b'0') && frac.is_empty();
    let mut out = String::new();
    if neg && !is_zero {
        out.push('-');
    }
    out.push_str(int);
    if !frac.is_empty() {
        out.push('.');
        out.push_str(frac);
    }
    out
}

/// Returns the sign and the decimal digits of the absolute value of a 256-bit
/// little-endian integer.
fn wide_digits(bytes: &[u8; 32], signed: bool) -> (bool, String) {
    let mut limbs = [0u64; 4];
    for (i, limb) in limbs.iter_mut().enumerate() {
        *limb = u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().expect("8 bytes"));
    }
    let neg = signed && limbs[3] >> 63 == 1;
    if neg {
        // Two's complement: invert and add one.
        let mut carry = true;
        for limb in &mut limbs {
            *limb = !*limb;
            if carry {
                let (v, c) = limb.overflowing_add(1);
                *limb = v;
                carry = c;
            }
        }
    }
    const CHUNK: u64 = 10_000_000_000_000_000_000; // 10^19
    let mut parts = Vec::new();
    while limbs.iter().any(|&l| l != 0) {
        let mut rem: u128 = 0;
        for limb in limbs.iter_mut().rev() {
            let cur = (rem << 64) | u128::from(*limb);
            *limb = (cur / u128::from(CHUNK)) as u64;
            rem = cur % u128::from(CHUNK);
        }
        parts.push(rem as u64);
    }
    let mut s = String::new();
    match parts.split_last() {
        None => s.push('0'),
        Some((first, rest)) => {
            let _ = write!(s, "{first}");
            for p in rest.iter().rev() {
                let _ = write!(s, "{p:019}");
            }
        }
    }
    (neg, s)
}

fn wide_to_string(bytes: &[u8; 32], signed: bool) -> String {
    let (neg, digits) = wide_digits(bytes, signed);
    if neg { format!("-{digits}") } else { digits }
}

/// Converts days since 1970-01-01 to a civil date (Howard Hinnant's
/// `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn format_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Writes `secs` (seconds since the epoch) in the time zone `tz` (or the
/// default time zone), with `precision` digits of `frac`.
fn format_datetime(
    secs: i64,
    frac: i64,
    precision: u32,
    tz: Option<&str>,
    ctx: &Context,
) -> Result<String, String> {
    let zone = match tz {
        Some(name) => Some(parse_timezone(name)?),
        None => ctx.default_timezone.map(Zone::Named),
    };
    // The server writes a value outside 0000-01-01 00:00:00 ..
    // 9999-12-31 23:59:59 (in the time zone of the type) as the nearest end
    // of that range, with the fraction of the value.
    const MIN_SECS: i64 = -62_167_219_200;
    const MAX_SECS: i64 = 253_402_300_799;
    const MARGIN: i64 = 2 * 86_400;
    let clamped = secs.clamp(MIN_SECS - MARGIN, MAX_SECS + MARGIN);
    let naive = match zone {
        None => chrono::DateTime::from_timestamp(clamped, 0).map(|t| t.naive_utc()),
        Some(Zone::Named(zone)) => named_offset(zone, clamped)
            .and_then(|offset| chrono::DateTime::from_timestamp(clamped + i64::from(offset), 0))
            .map(|t| t.naive_utc()),
        Some(Zone::Fixed(offset)) => offset
            .timestamp_opt(clamped, 0)
            .single()
            .map(|t| t.naive_local()),
    }
    .ok_or_else(|| format!("clickhouse-test-runner: timestamp {secs} is out of range"))?;
    let range = |s: i64| chrono::DateTime::from_timestamp(s, 0).map(|t| t.naive_utc());
    let (Some(min), Some(max)) = (range(MIN_SECS), range(MAX_SECS)) else {
        unreachable!("the range ends are valid timestamps");
    };
    let naive = naive.clamp(min, max);
    let mut s = naive.format("%Y-%m-%d %H:%M:%S").to_string();
    if precision > 0 {
        let _ = write!(s, ".{frac:0width$}", width = precision as usize);
    }
    Ok(s)
}

/// 2100-01-01 00:00:00 UTC. chrono-tz has the transitions of a time zone up
/// to the end of 2099 and uses the last offset after that. The server
/// (`cctz`) applies the rule of the zone (the POSIX TZ string of the tzfile)
/// to every later year, for example the daylight saving time of
/// `Europe/Amsterdam` in July 2105.
const END_OF_TRANSITIONS: i64 = 4_102_444_800;

/// Returns the UTC offset in seconds of `zone` at `secs`. From 2100, it is
/// the offset at the same time of a year in 2072..=2099 with the same
/// calendar (leap year, and weekday of January 1): the rule of the zone gives
/// the same transitions in both years.
fn named_offset(zone: chrono_tz::Tz, secs: i64) -> Option<i32> {
    use chrono::{Datelike, Offset};
    let mut probe = secs;
    if secs >= END_OF_TRANSITIONS {
        let is_leap = |y: i32| y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let jan1 = |y: i32| chrono::NaiveDate::from_ymd_opt(y, 1, 1);
        let year = chrono::DateTime::from_timestamp(secs, 0)?.year();
        let target = jan1(year)?;
        let same = (2072..=2099)
            .filter_map(jan1)
            .find(|d| is_leap(d.year()) == is_leap(year) && d.weekday() == target.weekday())?;
        probe -= (target - same).num_seconds();
    }
    let utc = chrono::DateTime::from_timestamp(probe, 0)?.naive_utc();
    Some(zone.offset_from_utc_datetime(&utc).fix().local_minus_utc())
}

/// The time zone of a `DateTime` or `DateTime64` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Zone {
    /// A name of the time-zone database.
    Named(chrono_tz::Tz),
    /// A fixed offset from UTC: `Fixed/UTC±HH:MM:SS`.
    Fixed(chrono::FixedOffset),
}

/// The prefix of the names of fixed-offset time zones.
const FIXED_ZONE_PREFIX: &str = "Fixed/UTC";

/// Parses the time-zone name of a `DateTime` or `DateTime64` type.
///
/// The server (`cctz`, `FixedOffsetFromName`) also accepts names of the
/// form `Fixed/UTC±HH:MM:SS`, which are not in the time-zone database. `-`
/// is west of UTC. The server writes these names, for example, for Arrow and
/// ORC timestamps with a fixed offset.
fn parse_timezone(name: &str) -> Result<Zone, String> {
    if let Some(offset) = name.strip_prefix(FIXED_ZONE_PREFIX) {
        return parse_fixed_offset(offset)
            .map(Zone::Fixed)
            .ok_or_else(|| format!("clickhouse-test-runner: unknown time zone {name:?}"));
    }
    name.parse::<chrono_tz::Tz>()
        .map(Zone::Named)
        .map_err(|e| format!("clickhouse-test-runner: unknown time zone {name:?}: {e}"))
}

/// Parses `±HH:MM:SS` (exactly two digits each).
fn parse_fixed_offset(s: &str) -> Option<chrono::FixedOffset> {
    let b = s.as_bytes();
    if b.len() != 9 || b[3] != b':' || b[6] != b':' {
        return None;
    }
    let two_digits = |i: usize| -> Option<i32> {
        let (h, l) = (b[i], b[i + 1]);
        (h.is_ascii_digit() && l.is_ascii_digit())
            .then(|| i32::from(h - b'0') * 10 + i32::from(l - b'0'))
    };
    let secs = (two_digits(1)? * 60 + two_digits(4)?) * 60 + two_digits(7)?;
    match b[0] {
        b'+' => chrono::FixedOffset::east_opt(secs),
        b'-' => chrono::FixedOffset::west_opt(secs),
        _ => None,
    }
}

const MAX_TIME_SECONDS: i64 = 999 * 3600 + 59 * 60 + 59;

/// Writes a `Time` / `Time64` value: `[-]HH:MM:SS[.fff]`. The hours can be
/// more than 24. The server writes a value above 999:59:59 as 999:59:59, with
/// a zero fraction; the stored value is not changed.
pub(crate) fn format_time(ticks: i64, precision: u32) -> String {
    let scale = 10i128.pow(precision);
    let ticks = i128::from(ticks);
    let mut s = String::new();
    if ticks < 0 {
        s.push('-');
    }
    let abs = ticks.abs();
    let (mut total, mut frac) = ((abs / scale) as i64, (abs % scale) as i64);
    if total > MAX_TIME_SECONDS {
        total = MAX_TIME_SECONDS;
        frac = 0;
    }
    let _ = write!(
        s,
        "{:02}:{:02}:{:02}",
        total / 3600,
        total / 60 % 60,
        total % 60
    );
    if precision > 0 {
        let _ = write!(s, ".{frac:0width$}", width = precision as usize);
    }
    s
}

/// Follows ClickHouse's `formatIPv6`: the longest run of two or more zero
/// words becomes `::`, and an IPv4-mapped (`::ffff:a.b.c.d`) or
/// IPv4-compatible (`::a.b.c.d`) address ends in dotted form.
pub(crate) fn format_ipv6(ip: &Ipv6Addr) -> String {
    let words = ip.segments();
    let octets = ip.octets();
    let (mut best_base, mut best_len) = (None, 0);
    let mut i = 0;
    while i < 8 {
        if words[i] != 0 {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < 8 && words[j] == 0 {
            j += 1;
        }
        if j - i > best_len {
            best_base = Some(i);
            best_len = j - i;
        }
        i = j;
    }
    if best_len < 2 {
        best_base = None;
    }
    let ipv4_compatible = best_base == Some(0) && best_len == 6;
    let ipv4_mapped = best_base == Some(0) && best_len == 5 && words[5] == 0xffff;

    let mut s = String::new();
    for (i, word) in words.iter().enumerate() {
        if let Some(base) = best_base
            && i >= base
            && i < base + best_len
        {
            if i == base {
                s.push(':');
            }
            continue;
        }
        if i != 0 {
            s.push(':');
        }
        if i == 6 && (ipv4_compatible || ipv4_mapped) {
            let _ = write!(
                s,
                "{}.{}.{}.{}",
                octets[12], octets[13], octets[14], octets[15]
            );
            return s;
        }
        let _ = write!(s, "{word:x}");
    }
    if let Some(base) = best_base
        && base + best_len == 8
    {
        s.push(':');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(name: &str) -> DataTypeNode {
        DataTypeNode::new(name).unwrap()
    }

    fn render(name: &str, v: Value<'_>) -> String {
        let mut out = Vec::new();
        render_value(&mut out, &ty(name), &v, false, &Context::default()).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn floats() {
        // Variables, so that the compiler does not fold 0.1 + 0.2 exactly.
        let (a, b) = (std::hint::black_box(0.1f64), std::hint::black_box(0.2f64));
        // The expected text is what ClickHouse 26.10 writes for FORMAT TSV.
        let cases: &[(f64, &str)] = &[
            (1e15, "1000000000000000"),
            (1e20, "100000000000000000000"),
            (1e21, "1e21"),
            (1.2345e21, "1.2345e21"),
            (1.2345e20, "123450000000000000000"),
            (0.0001, "0.0001"),
            (1e-6, "0.000001"),
            (1.5e-6, "0.0000015"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (1.5e300, "1.5e300"),
            (a + b, "0.30000000000000004"),
            (5e-324, "5e-324"),
            (-12345678.9, "-12345678.9"),
            (100.0, "100"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (-0.0, "-0"),
            (0.0, "0"),
        ];
        for (f, want) in cases {
            assert_eq!(format_float64(*f), *want, "{f:e}");
        }
        assert_eq!(format_float64(f64::NAN), "nan");
        // The server writes `nan` also for a NaN with the sign bit set.
        assert_eq!(format_float64(-f64::NAN), "nan");
        assert_eq!(format_float32(-f32::NAN), "nan");
        assert_eq!(format_float32(0.1), "0.1");
        assert_eq!(format_float32(3.402_823_5e38), "3.4028235e38");
        assert_eq!(format_float32(16_777_216.0), "16777216");
    }

    #[test]
    fn ipv6() {
        for (input, want) in [
            ("::ffff:1.2.3.4", "::ffff:1.2.3.4"),
            ("::1.2.3.4", "::1.2.3.4"),
            ("::", "::"),
            ("::1", "::1"),
            ("2001:db8::1", "2001:db8::1"),
            ("1::", "1::"),
            ("2001:db8:0:1::1", "2001:db8:0:1::1"),
            ("1:0:0:2:0:0:0:3", "1:0:0:2::3"),
            ("1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8"),
        ] {
            assert_eq!(format_ipv6(&input.parse().unwrap()), want, "{input}");
        }
    }

    #[test]
    fn decimals() {
        assert_eq!(decimal_text(false, "150".into(), 2), "1.5");
        assert_eq!(decimal_text(false, "0".into(), 2), "0");
        assert_eq!(decimal_text(true, "1".into(), 5), "-0.00001");
        assert_eq!(decimal_text(false, "100000".into(), 3), "100");
        assert_eq!(decimal_text(true, "0".into(), 3), "0");
        assert_eq!(render("Decimal(9, 2)", Value::Int(150)), "1.5");
        assert_eq!(render("Decimal(38, 0)", Value::Int(-7)), "-7");
    }

    #[test]
    fn wide_integers() {
        let mut max = [0xffu8; 32];
        assert_eq!(wide_to_string(&max, true), "-1");
        assert_eq!(
            wide_to_string(&max, false),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        max[31] = 0x80;
        max[..31].fill(0);
        assert_eq!(
            wide_to_string(&max, true),
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
        );
        let mut one = [0u8; 32];
        one[0] = 1;
        assert_eq!(wide_to_string(&one, true), "1");
        assert_eq!(wide_to_string(&[0; 32], false), "0");
        let mut ten19 = [0u8; 32];
        ten19[..8].copy_from_slice(&10_000_000_000_000_000_000u64.to_le_bytes());
        assert_eq!(wide_to_string(&ten19, false), "10000000000000000000");
    }

    #[test]
    fn values() {
        assert_eq!(
            render("String", Value::Bytes(b"a\tb\\c\nd'e\x00\x08\x0c\x01")),
            "a\\tb\\\\c\\nd\\'e\\0\\b\\f\x01"
        );
        assert_eq!(render("Nullable(String)", Value::Null), "\\N");
        assert_eq!(render("Nullable(String)", Value::Bytes(b"x")), "x");
        assert_eq!(
            render(
                "Array(Nullable(String))",
                Value::Array(vec![Value::Bytes(b"a'b"), Value::Null])
            ),
            "['a\\'b',NULL]"
        );
        assert_eq!(
            render(
                "Tuple(UInt8, String, Nullable(UInt8))",
                Value::Tuple(vec![Value::UInt(1), Value::Bytes(b"x"), Value::Null])
            ),
            "(1,'x',NULL)"
        );
        assert_eq!(
            render(
                "Map(String, UInt8)",
                Value::Map(vec![
                    (Value::Bytes(b"k"), Value::UInt(1)),
                    (Value::Bytes(b"a"), Value::UInt(2))
                ])
            ),
            "{'k':1,'a':2}"
        );
        assert_eq!(render("Int128", Value::Int(-5)), "-5");
        assert_eq!(render("Date", Value::UInt(18263)), "2020-01-02");
        assert_eq!(render("Date32", Value::Int(-25567)), "1900-01-01");
        assert_eq!(
            render("Array(Date)", Value::Array(vec![Value::UInt(18263)])),
            "['2020-01-02']"
        );
        assert_eq!(
            render("DateTime", Value::UInt(1_577_872_800)),
            "2020-01-01 10:00:00"
        );
        assert_eq!(
            render("DateTime('Asia/Tokyo')", Value::UInt(1_577_836_800)),
            "2020-01-01 09:00:00"
        );
        assert_eq!(
            render("DateTime64(3)", Value::Int(1_577_836_800_120)),
            "2020-01-01 00:00:00.120"
        );
        assert_eq!(
            render("DateTime64(2)", Value::Int(-1)),
            "1969-12-31 23:59:59.99"
        );
        // The server writes the ends of its range for a value outside it.
        assert_eq!(
            render(
                "DateTime64(3, 'UTC')",
                Value::Int(9_223_372_036_854_775_806)
            ),
            "9999-12-31 23:59:59.806"
        );
        assert_eq!(
            render("DateTime64(0, 'Asia/Tokyo')", Value::Int(-62_167_219_201)),
            "0000-01-01 09:18:58"
        );
        assert_eq!(
            render(
                "DateTime64(0, 'Asia/Tokyo')",
                Value::Int(-9_223_372_036_854)
            ),
            "0000-01-01 00:00:00"
        );
        assert_eq!(
            render("DateTime64(0)", Value::Int(16_725_225_600)),
            "2500-01-01 00:00:00"
        );
        assert_eq!(render("Time", Value::Int(-3723)), "-01:02:03");
        assert_eq!(
            render("Time64(3)", Value::Int(360_000_500)),
            "100:00:00.500"
        );
        // The server writes a value above 999:59:59 as 999:59:59.
        assert_eq!(render("Time64(2)", Value::Int(777_601_200)), "999:59:59.00");
        assert_eq!(render("Time", Value::Int(-7_200_000)), "-999:59:59");
        assert_eq!(
            render(
                "UUID",
                Value::Uuid("61f0c404-5cb3-11e7-907b-a6006ad3dba0".parse().unwrap())
            ),
            "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
        );
        assert_eq!(
            render("IPv4", Value::Ipv4("1.2.3.4".parse().unwrap())),
            "1.2.3.4"
        );
        assert_eq!(
            render(
                "Array(IPv6)",
                Value::Array(vec![Value::Ipv6("::ffff:1.2.3.4".parse().unwrap())])
            ),
            "['::ffff:1.2.3.4']"
        );
        assert_eq!(render("Bool", Value::Bool(true)), "true");
        assert_eq!(
            render(
                "Array(Float64)",
                Value::Array(vec![
                    Value::Float64(f64::NAN),
                    Value::Float64(f64::INFINITY)
                ])
            ),
            "[nan,inf]"
        );
        assert_eq!(render("IntervalDay", Value::Int(3)), "3");
        assert_eq!(render("LowCardinality(String)", Value::Bytes(b"a")), "a");
        assert_eq!(
            render("LowCardinality(Nullable(String))", Value::Null),
            "\\N"
        );
        assert_eq!(
            render("SimpleAggregateFunction(sum, UInt64)", Value::UInt(7)),
            "7"
        );
        assert_eq!(
            render(
                "SimpleAggregateFunction(sumMap, Tuple(Array(UInt8), Array(String)))",
                Value::Tuple(vec![
                    Value::Array(vec![Value::UInt(1)]),
                    Value::Array(vec![Value::Bytes(b"a")])
                ])
            ),
            "([1],['a'])"
        );
        assert_eq!(
            render("Enum8('a' = 1, 'b\\'c' = 2)", Value::Int(2)),
            "b\\'c"
        );
        assert_eq!(
            render("Array(Enum8('a' = 1))", Value::Array(vec![Value::Int(1)])),
            "['a']"
        );
        assert_eq!(render("Point", Value::Point(1.5, -2.0)), "(1.5,-2)");
        assert_eq!(render("BFloat16", Value::BFloat16(0x3dcc)), "0.099609375");
    }

    /// `Fixed/UTC±HH:MM:SS` zones. The expected text is the server's
    /// (`05098_time_zone_fixed_offset_granularity.reference`).
    #[test]
    fn named_time_zones_after_2099() {
        // The values and the text of the server (26.10), `toUnixTimestamp` and
        // `toString`: the zone rules apply after 2099 too.
        for (name, v, want) in [
            (
                "DateTime('Europe/Amsterdam')",
                4_088_887_680,
                "2099-07-28 04:08:00",
            ),
            (
                "DateTime('Europe/Amsterdam')",
                4_120_423_680,
                "2100-07-28 04:08:00",
            ),
            (
                "DateTime('Europe/Amsterdam')",
                4_139_089_680,
                "2101-03-01 04:08:00",
            ),
            (
                "DateTime('Europe/Amsterdam')",
                4_278_190_080,
                "2105-07-28 04:08:00",
            ),
            (
                "DateTime('Europe/Amsterdam')",
                4_294_967_295,
                "2106-02-07 07:28:15",
            ),
            (
                "DateTime('America/New_York')",
                4_278_233_280,
                "2105-07-28 10:08:00",
            ),
            (
                "DateTime('America/New_York')",
                4_255_047_000,
                "2104-11-02 01:30:00",
            ),
        ] {
            assert_eq!(render(name, Value::UInt(v)), want, "{name} {v}");
        }
        for (v, want) in [
            (7_276_097_280, "2200-07-28 04:08:00"),
            (10_407_918_600, "2299-10-25 02:30:00"),
        ] {
            assert_eq!(
                render("DateTime64(0, 'Europe/Amsterdam')", Value::Int(v)),
                want,
                "{v}"
            );
        }
    }

    #[test]
    fn fixed_offset_time_zones() {
        for (name, v, want) in [
            ("DateTime('Fixed/UTC+00:00:00')", 0, "1970-01-01 00:00:00"),
            ("DateTime('Fixed/UTC+05:30:00')", 0, "1970-01-01 05:30:00"),
            ("DateTime('Fixed/UTC-08:00:00')", 0, "1969-12-31 16:00:00"),
        ] {
            assert_eq!(render(name, Value::UInt(v)), want, "{name}");
        }
        for (name, want) in [
            (
                "DateTime64(3, 'Fixed/UTC+14:00:00')",
                "1970-01-01 14:00:00.000",
            ),
            (
                "DateTime64(3, 'Fixed/UTC-14:00:00')",
                "1969-12-31 10:00:00.000",
            ),
        ] {
            assert_eq!(render(name, Value::Int(0)), want, "{name}");
        }
        // Same offset as Asia/Kathmandu.
        assert_eq!(
            render("DateTime('Fixed/UTC+05:45:00')", Value::UInt(1_700_000_000)),
            render("DateTime('Asia/Kathmandu')", Value::UInt(1_700_000_000)),
        );
        for name in [
            "Fixed/UTC+1:00:00",
            "Fixed/UTC+01:00",
            "Fixed/UTC*01:00:00",
            "Fixed/UTC+0a:00:00",
            "Fixed/UTC+05:30:000",
            "Fixed/Europe/Amsterdam",
        ] {
            assert!(parse_timezone(name).is_err(), "{name}");
        }
        assert_eq!(
            parse_timezone("Europe/Amsterdam"),
            Ok(Zone::Named(chrono_tz::Europe::Amsterdam))
        );
    }

    #[test]
    fn mismatch_is_an_error() {
        let ctx = Context::default();
        let mut out = Vec::new();
        assert!(render_value(&mut out, &ty("UInt8"), &Value::Bytes(b"x"), false, &ctx).is_err());
        // An enum value without a name.
        assert!(
            render_value(&mut out, &ty("Enum8('a' = 1)"), &Value::Int(5), false, &ctx).is_err()
        );
    }
}
