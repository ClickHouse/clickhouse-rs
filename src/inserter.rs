use std::{future::Future, mem};

use serde::Serialize;

use crate::{error::Result, insert::Insert, introspection::Reflection, Client};

pub struct Inserter<T> {
    client: Client,
    table: String,
    insert: Insert<T>,
    quantities: Quantities,
}

//#[derive(Debug, Clone, Add, AddAssign, Sub, SubAssign, PartialEq, Eq, Serialize)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Quantities {
    pub entries: u64,
    pub transactions: u64,
}

impl Quantities {
    pub const ZERO: Quantities = Quantities {
        entries: 0,
        transactions: 0,
    };
}

impl<T> Inserter<T>
where
    T: Reflection,
{
    pub fn new(client: &Client, table: &str) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            table: table.into(),
            insert: client.insert(table)?,
            quantities: Quantities::ZERO,
        })
    }

    #[inline]
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        self.quantities.entries += 1;
        let fut = self.insert.write(row);
        async move { fut.await }
    }

    pub async fn commit(&mut self) -> Result<Quantities> {
        self.quantities.transactions += 1;

        Ok(if self.is_threshold_reached() {
            let new_insert = self.client.insert(&self.table)?; // Actually it mustn't fail.
            let insert = mem::replace(&mut self.insert, new_insert);
            insert.end().await?;
            mem::replace(&mut self.quantities, Quantities::ZERO)
        } else {
            Quantities::ZERO
        })
    }

    pub async fn end(self) -> Result<Quantities> {
        self.insert.end().await?;
        Ok(self.quantities)
    }

    fn is_threshold_reached(&self) -> bool {
        self.quantities.entries > 10
    }
}
