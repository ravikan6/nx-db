pub use core::*;
pub use database_cache as cache;
pub use database_core as core;

#[cfg(feature = "postgres")]
pub mod postgres {
    pub use driver_postgres::PostgresAdapter;
    pub mod migration {
        pub use driver_postgres::migration::*;
    }
}

pub mod prelude {
    pub use crate::core::{Context, Database, Filter, Key, Model, QuerySpec, Repository, Role};
    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresAdapter;
}

#[cfg(feature = "postgres")]
#[macro_export]
macro_rules! db_connect {
    ($url:expr) => {
        sqlx::PgPool::connect($url)
    };
}
