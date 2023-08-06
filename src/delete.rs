use crate::{error::Result, query::Query, sql::Bind, Client};

#[must_use]
#[derive(Clone)]
pub struct Delete {
    query: Query,
}

impl Delete {
    pub(crate) fn new(client: &Client, table_name: &str, pk_names: Vec<String>) -> Self {
        let mut out = format!("ALTER TABLE `{table_name}` DELETE WHERE");
        for (idx, pk_name) in pk_names.iter().enumerate() {
            if idx > 0 {
                out.push_str(" AND ");
            }
            out.push_str(&format!(" `{pk_name}` = ? "));
        }
        let query = Query::new(client, &out);
        Self { query }
    }
    pub async fn delete(mut self, delete_pk: Vec<impl Bind>) -> Result<()> {
        for i in delete_pk {
            self.query.bind_ref(i);
        }
        self.query.execute().await
    }
}
