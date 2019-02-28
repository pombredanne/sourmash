use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use derive_builder::Builder;
use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};

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
    fn save(&mut self, path: &str, content: &[u8]) -> Result<(), Error>;

    /// Load bytes from path
    fn load(&self, path: &str) -> Result<Vec<u8>, Error>;
}

/// Store files locally into a directory
#[derive(Builder, Debug, Clone, Default)]
pub struct FSStorage {
    /// absolute path for the directory where data is saved.
    pub(crate) basepath: PathBuf,
}

impl Storage for FSStorage {
    fn save(&mut self, path: &str, content: &[u8]) -> Result<(), Error> {
        Ok(())
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
