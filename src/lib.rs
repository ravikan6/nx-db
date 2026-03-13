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
    pub use crate::core::{Context, Database, Key, Model, QuerySpec, Repository, Role};
    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresAdapter;
}
