pub trait Document {
    fn get_id(&self) -> Option<&str>;
    fn set_id(&mut self, value: &str) -> ();
}
