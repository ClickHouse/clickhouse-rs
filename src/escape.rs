use std::fmt;

// See https://clickhouse.tech/docs/en/sql-reference/syntax/#syntax-string-literal.
pub fn string(src: &str, mut dst: impl fmt::Write) -> fmt::Result {
    dst.write_char('\'')?;

    for (idx, part) in src.split('\'').enumerate() {
        if idx > 0 {
            dst.write_str("\\'")?;
        }

        for (idx, part) in part.split('\\').enumerate() {
            if idx > 0 {
                dst.write_str("\\\\")?;
            }

            dst.write_str(part)?;
        }
    }

    dst.write_char('\'')
}

#[test]
fn it_escapes_string() {
    let mut actual = String::new();
    string(r#"f\o'o '' b\'ar'"#, &mut actual).unwrap();
    assert_eq!(actual, r#"'f\\o\'o \'\' b\\\'ar\''"#);
}
