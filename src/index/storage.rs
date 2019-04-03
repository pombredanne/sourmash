use std::collections::HashMap;
use std::fs::{DirBuilder, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use derive_builder::Builder;
use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Fail)]
pub enum StorageError {
    #[fail(display = "Path can't be empty")]
    EmptyPathError,
}

#[derive(Debug, Fail)]
pub enum ReadDataError {
    #[fail(display = "Could not load data")]
    LoadError,
}

/// Implemented by anything that wants to read specific data from a storage.
pub trait ReadData<D> {
    fn data(&self) -> Result<&D, Error>;
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StorageInfo {
    pub(crate) backend: String,
    pub(crate) args: HashMap<String, String>,
}

/// An abstraction for any place where we can store data.
pub trait Storage {
    /// Save bytes into path
    fn save(&self, path: &str, content: &[u8]) -> Result<String, Error>;

    /// Load bytes from path
    fn load(&self, path: &str) -> Result<Vec<u8>, Error>;
}

/// Store files locally into a directory
#[derive(Builder, Debug, Clone, Default)]
pub struct FSStorage {
    /// absolute path for the directory where data is saved.
    pub(crate) basepath: PathBuf,
}

impl FSStorage {
    pub fn builder() -> FSStorageBuilder {
        FSStorageBuilder::default()
    }
}

impl Storage for FSStorage {
    fn save(&self, path: &str, content: &[u8]) -> Result<String, Error> {
        if path.is_empty() {
            return Err(StorageError::EmptyPathError.into());
        }

        let path = self.basepath.join(path);
        DirBuilder::new()
            .recursive(true)
            .create(path.parent().unwrap())?;

        let file = File::create(&path)?;
        let mut buf_writer = BufWriter::new(file);
        buf_writer.write(content)?;
        Ok(path.to_str().unwrap().into())
    }

    fn load(&self, path: &str) -> Result<Vec<u8>, Error> {
        let path = self.basepath.join(path);
        let file = File::open(path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = Vec::new();
        buf_reader.read_to_end(&mut contents)?;
        Ok(contents)
    }
}

pub trait ToWriter {
    fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write;
}
