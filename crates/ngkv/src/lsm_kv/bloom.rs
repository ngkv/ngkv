use std::todo;

use serde::{de::Visitor, Deserialize, Serialize};

pub(crate) struct BloomFilter {}

impl Serialize for BloomFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&[])
    }
}
struct BufVisitor;

impl<'de> Visitor<'de> for BufVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: std::error::Error,
    {
        Ok(v.to_owned())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: std::error::Error,
    {
        Ok(v)
    }
}

impl<'de> Deserialize<'de> for BloomFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = deserializer.deserialize_byte_buf(BufVisitor);
        // TODO: init bloom filter
        todo!()
    }
}

impl BloomFilter {
    pub fn likely_contains(&self, key: &[u8]) -> bool {
        // TODO
        todo!()
    }
}

pub(crate) struct BloomFilterBuilder {

}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        todo!()
    }

    pub fn add_key(&mut self, key: &[u8]) -> &mut Self {
        // TODO
        todo!()
    }

    pub fn build(&self) -> BloomFilter {
        todo!()
    }
}
