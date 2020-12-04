/// Trait to convert any type to a "String" primary key.
///
/// Implemented by default for any T that can be converted to a string
/// using [`std::string::ToString`].
pub trait ToPrimaryKey {
    /// Converts the struct to a string for usage as a primary key in a data store.
    fn to_primary_key(&self) -> String;
}

impl<T> ToPrimaryKey for T
where
    T: std::string::ToString,
{
    fn to_primary_key(&self) -> String {
        self.to_string()
    }
}
