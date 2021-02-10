use std::io::{Read, Write};

use crate::{Error, Result};

use bincode::Options as BincodeOptions;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

fn bincode_options() -> impl BincodeOptions {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_varint_encoding()
}

pub fn serialize(t: &impl Serialize) -> Result<Vec<u8>> {
    // Currently there is no reason for serialization to fail.
    let res = bincode_options().serialize(t).unwrap();
    Ok(res)
}

pub fn serialize_into(w: impl Write, t: &impl Serialize) -> Result<()> {
    // Currently there is no reason for serialization to fail.
    bincode_options().serialize_into(w, t).unwrap();
    Ok(())
}

pub fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    bincode_options()
        .deserialize(bytes)
        .map_err(|_| Error::Corrupted("deserialization failed".into()))
}

pub fn deserialize_from<T: DeserializeOwned>(r: impl Read) -> Result<T> {
    bincode_options()
        .deserialize_from(r)
        .map_err(|_| Error::Corrupted("deserialization failed".into()))
}

pub trait FixedSizeSerializable: Sized {
    fn serialized_size() -> usize;
    fn serialize_into(&self, w: impl Write) -> Result<()>;
    fn deserialize_from(r: impl Read) -> Result<Self>;
}
