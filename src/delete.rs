use crate::{error::Result, query::Query, sql::Bind, Client};

#[must_use]
#[derive(Clone)]
pub struct Delete {
    query: Query,
}

impl Delete {
    pub(crate) fn new(client: &Client, table_name: &str, pk_name: &str, pk_len: usize) -> Self {
        let mut out = format!("ALTER TABLE {table_name} DELETE WHERE {pk_name} in (");
        for idx in 0..pk_len {
            if idx > 0 {
                out.push(',');
            }
            out.push_str("?");
        }
        out.push_str(")");
        let query = Query::new(client, &out);
        Self { query }
    }
    pub async fn delete(mut self, delete_pk: Vec<impl Bind>) -> Result<()> {
        delete_pk.into_iter().for_each(|a| self.query.bind_ref(a));
        self.query.execute().await
    }
}
