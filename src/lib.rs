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

#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use driver_sqlite::SqliteAdapter;
    pub mod migration {
        pub use driver_sqlite::migration::*;
    }
}

pub mod prelude {
    pub use crate::core::{Context, Database, Filter, Key, Model, QuerySpec, Repository, Role};
    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresAdapter;
    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqliteAdapter;
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[macro_export]
macro_rules! db_connect {
    (postgres, $url:expr) => {
        sqlx::PgPool::connect($url)
    };
    (sqlite, $url:expr) => {
        sqlx::SqlitePool::connect($url)
    };
    ($url:expr) => {
        sqlx::AnyPool::connect($url)
    };
}
