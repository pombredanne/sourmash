use std::collections::HashMap;
use std::fs::{DirBuilder, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

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
    pub(crate) args: StorageArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StorageArgs {
    FSStorage { path: String },
}

impl From<&StorageArgs> for FSStorage {
    fn from(other: &StorageArgs) -> FSStorage {
        if let StorageArgs::FSStorage { path } = other {
            let mut fullpath = PathBuf::new();
            fullpath.push(".");
            fullpath.push(path);

            FSStorage {
                fullpath: fullpath,
                subdir: path.clone(),
            }
        } else {
            unimplemented!()
        }
    }
}

/// An abstraction for any place where we can store data.
pub trait Storage {
    /// Save bytes into path
    fn save(&self, path: &str, content: &[u8]) -> Result<String, Error>;

    /// Load bytes from path
    fn load(&self, path: &str) -> Result<Vec<u8>, Error>;

    /// Args for initializing a new Storage
    fn args(&self) -> StorageArgs;
}

/// Store files locally into a directory
#[derive(TypedBuilder, Debug, Clone, Default)]
pub struct FSStorage {
    /// absolute path for the directory where data is saved.
    pub(crate) fullpath: PathBuf,
    pub(crate) subdir: String,
}

impl FSStorage {
    pub fn new(location: &str, subdir: &str) -> FSStorage {
        let mut fullpath = PathBuf::new();
        fullpath.push(location);
        fullpath.push(subdir);

        FSStorage {
            fullpath,
            subdir: subdir.into(),
        }
    }

    pub fn set_base(&mut self, location: &str) {
        let mut fullpath = PathBuf::new();
        fullpath.push(location);
        fullpath.push(&self.subdir);
        self.fullpath = fullpath;
    }
}

impl Storage for FSStorage {
    fn save(&self, path: &str, content: &[u8]) -> Result<String, Error> {
        if path.is_empty() {
            return Err(StorageError::EmptyPathError.into());
        }

        let path = self.fullpath.join(path);
        DirBuilder::new()
            .recursive(true)
            .create(path.parent().unwrap())?;

        let file = File::create(&path)?;
        let mut buf_writer = BufWriter::new(file);
        buf_writer.write(content)?;
        Ok(path.to_str().unwrap().into())
    }

    fn load(&self, path: &str) -> Result<Vec<u8>, Error> {
        let path = self.fullpath.join(path);
        let file = File::open(path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = Vec::new();
        buf_reader.read_to_end(&mut contents)?;
        Ok(contents)
    }

    fn args(&self) -> StorageArgs {
        StorageArgs::FSStorage {
            path: self.subdir.clone(),
        }
    }
}

pub trait ToWriter {
    fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write;
}
