/// Unqualifies a fully qualified table name into its database and table components.
/// If the input does not contain a database, the database component will be `None`.
///
/// # Examples
///
///  ```
/// let (db, table) = unqualify_table_name("my_database.my_table");
/// assert_eq!(db, Some("my_database".to_string()));
/// assert_eq!(table, "my_table".to_string());
///
/// let (db, table) = unqualify_table_name("my_table");
/// assert_eq!(db, None);
/// assert_eq!(table, "my_table".to_string());
///
/// let (db, table) = unqualify_table_name("`db.schema`.table");
/// assert_eq!(db, Some("db.schema".to_string()));
/// assert_eq!(table, "table".to_string());
///
/// let (db, table) = unqualify_table_name("db.`table.name`");
/// assert_eq!(db, Some("db".to_string()));
/// assert_eq!(table, "table.name".to_string());
///
/// let (db, table) = unqualify_table_name("`my_db.my_table`");
/// assert_eq!(db, None);
/// assert_eq!(table, "my_db.my_table".to_string());
/// ```
pub(crate) fn unqualify(table_name_with_db: &str) -> (Option<String>, String) {
    todo!()
}
