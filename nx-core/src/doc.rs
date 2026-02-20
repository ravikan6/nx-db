pub use crate::traits::document::Document;

#[derive(Clone, Default)]
pub struct Doc {
    metadata: Metadata,
}

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    pub id: Option<String>,
    pub collection: Option<String>,
}

impl Document for Doc {
    fn get_id(&self) -> Option<&str> {
        self.metadata.id.as_deref()
    }

    fn set_id(&mut self, value: &str) -> () {
        self.metadata.id = Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Doc, Document};
    #[test]
    fn main() {
        let mut a = Doc::default();
        a.set_id("hello");

        println!("{}", &a.get_id().unwrap())
    }
}
