#[cfg(any(feature = "cbor", feature = "json"))]
use serde::{Deserialize, Serialize};

/// This trait is responsible for serializing and de-serializing data for the upstream, when
/// the upstream expects data to be stored as byte buffer (Vec<u8>).
///
/// For example, an implementation of this exists, `CborSerializer` that will serialize
/// the data for storage using the consistent binary object representation.
pub trait UpstreamSerializer<V>: Clone + Send + 'static {
    type Error: std::error::Error + Send + 'static;

    fn serialize(&self, value: &V) -> Result<Vec<u8>, Self::Error>;
    fn deserialize(&self, buf: &[u8]) -> Result<V, Self::Error>;
}

#[cfg(feature = "cbor")]
/// Serializes and de-serializes data to/from storage on the upstream using
/// `serde_cbor`. Must implement serde's `Serialize` and `Deserialize` trait
/// in order for this to work.
#[derive(Clone)]
pub struct CborSerializer;

#[cfg(feature = "cbor")]
impl<V> UpstreamSerializer<V> for CborSerializer
where
    V: Serialize,
    V: for<'de> Deserialize<'de>,
{
    type Error = serde_cbor::error::Error;

    fn serialize(&self, value: &V) -> Result<Vec<u8>, Self::Error> {
        serde_cbor::to_vec(value)
    }
    fn deserialize(&self, buf: &[u8]) -> Result<V, Self::Error> {
        serde_cbor::from_reader(buf)
    }
}

#[cfg(feature = "json")]
/// Serializes and de-serializes data to/from storage on the upstream using
/// `serde_json`. Must implement serde's `Serialize` and `Deserialize` trait
/// in order for this to work.
#[derive(Clone)]
pub struct JsonSerializer;

#[cfg(feature = "json")]
impl<V> UpstreamSerializer<V> for JsonSerializer
where
    V: Serialize,
    V: for<'de> Deserialize<'de>,
{
    type Error = serde_json::error::Error;

    fn serialize(&self, value: &V) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(value)
    }
    fn deserialize(&self, buf: &[u8]) -> Result<V, Self::Error> {
        serde_json::from_reader(buf)
    }
}
