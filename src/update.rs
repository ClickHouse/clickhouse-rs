use crate::{error::Result, query::Query, sql::Bind, Client};

#[must_use]
#[derive(Clone)]
pub struct Update {
    query: Query,
}

impl Update {
    pub(crate) fn new(
        client: &Client,
        table_name: &str,
        pk_name: &str,
        flieds_names: Vec<String>,
    ) -> Self {
        let mut out: String = flieds_names.iter().enumerate().fold(
            format!("ALTER TABLE {table_name} UPDATE"),
            |mut res, (idx, key)| {
                if idx > 0 {
                    res.push(',');
                }
                res.push_str(&format!(" {key} = ?"));
                res
            },
        );
        out.push_str(&format!(" where {pk_name} = ?"));
        let query = Query::new(client, &out);
        Self { query }
    }
    pub async fn update_fields(mut self, fields: Vec<impl Bind>, pk: impl Bind) -> Result<()> {
        fields.into_iter().for_each(|a| {
            self.query.bind_ref(a);
        });
        self.query.bind_ref(pk);
        self.query.execute().await
    }
}
