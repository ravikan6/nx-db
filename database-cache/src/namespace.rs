use crate::CacheError;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(Box<str>);

impl Namespace {
    pub fn root() -> Self {
        Self("root".into())
    }

    pub fn new(value: impl Into<String>) -> Result<Self, CacheError> {
        let value = value.into();
        validate_namespace(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    pub fn from_segments<I, S>(segments: I) -> Result<Self, CacheError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut value = String::new();
        for segment in segments {
            let segment = segment.as_ref();
            validate_segment("namespace", segment)?;
            if !value.is_empty() {
                value.push(':');
            }
            value.push_str(segment);
        }

        if value.is_empty() {
            Ok(Self::root())
        } else {
            Ok(Self(value.into_boxed_str()))
        }
    }

    pub fn child(&self, segment: impl AsRef<str>) -> Result<Self, CacheError> {
        let segment = segment.as_ref();
        validate_segment("namespace", segment)?;

        if self.is_root() {
            return Self::new(segment);
        }

        let mut value = String::with_capacity(self.0.len() + segment.len() + 1);
        value.push_str(&self.0);
        value.push(':');
        value.push_str(segment);
        Self::new(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.as_ref() == "root"
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Self::root()
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_namespace(value: &str) -> Result<(), CacheError> {
    if value.is_empty() {
        return Err(CacheError::InvalidNamespace(
            "namespace cannot be empty".into(),
        ));
    }

    for segment in value.split(':') {
        validate_segment("namespace", segment)?;
    }

    Ok(())
}

pub(crate) fn validate_segment(kind: &str, segment: &str) -> Result<(), CacheError> {
    if segment.is_empty() {
        return Err(CacheError::InvalidNamespace(format!(
            "{kind} segment cannot be empty"
        )));
    }

    if !segment
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(CacheError::InvalidNamespace(format!(
            "{kind} segment '{segment}' contains unsupported characters"
        )));
    }

    Ok(())
}
