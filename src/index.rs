pub mod bigsi;
pub mod linear;
pub mod sbt;

pub mod storage;

pub mod nodegraph;

pub mod search;

use std::path::Path;
use std::rc::Rc;

use serde_derive::{Deserialize, Serialize};

use derive_builder::Builder;
use failure::Error;
use lazy_init::Lazy;

use crate::index::search::{search_minhashes, search_minhashes_containment};
use crate::index::storage::{ReadData, ReadDataError, Storage};
use crate::Signature;

pub trait Index {
    type Item;

    fn find<F>(
        &self,
        search_fn: F,
        sig: &Self::Item,
        threshold: f64,
    ) -> Result<Vec<&Self::Item>, Error>
    where
        F: Fn(&dyn Comparable<Self::Item>, &Self::Item, f64) -> bool;

    fn search(
        &self,
        sig: &Self::Item,
        threshold: f64,
        containment: bool,
    ) -> Result<Vec<&Self::Item>, Error> {
        if containment {
            self.find(search_minhashes_containment, sig, threshold)
        } else {
            self.find(search_minhashes, sig, threshold)
        }
    }

    //fn gather(&self, sig: &Self::Item, threshold: f64) -> Result<Vec<&Self::Item>, Error>;

    fn insert(&mut self, node: &Self::Item);

    fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), Error>;

    fn load<P: AsRef<Path>>(path: P) -> Result<(), Error>;

    fn datasets(&self) -> Vec<Self::Item>;
}

// TODO: split into two traits, Similarity and Containment?
pub trait Comparable<O> {
    fn similarity(&self, other: &O) -> f64;
    fn containment(&self, other: &O) -> f64;
}

impl<'a, N, L> Comparable<L> for &'a N
where
    N: Comparable<L>,
{
    fn similarity(&self, other: &L) -> f64 {
        (*self).similarity(&other)
    }

    fn containment(&self, other: &L) -> f64 {
        (*self).containment(&other)
    }
}

#[derive(Serialize, Deserialize)]
pub struct DatasetInfo {
    pub filename: String,
    pub name: String,
    pub metadata: String,
}

#[derive(Builder, Default, Clone)]
pub struct Dataset<T>
where
    T: std::marker::Sync,
{
    pub(crate) filename: String,
    pub(crate) name: String,
    pub(crate) metadata: String,

    pub(crate) storage: Option<Rc<dyn Storage>>,

    pub(crate) data: Rc<Lazy<T>>,
}

impl<T> std::fmt::Debug for Dataset<T>
where
    T: std::marker::Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset [filename: {}, name: {}, metadata: {}]",
            self.filename, self.name, self.metadata
        )
    }
}

impl ReadData<Signature> for Dataset<Signature> {
    fn data(&self) -> Result<&Signature, Error> {
        if let Some(storage) = &self.storage {
            let sig = self.data.get_or_create(|| {
                let raw = storage.load(&self.filename).unwrap();
                let sigs: Vec<Signature> = serde_json::from_reader(&mut &raw[..]).unwrap();
                // TODO: select the right sig?
                sigs[0].to_owned()
            });

            Ok(sig)
        } else {
            Err(ReadDataError::LoadError.into())
        }
    }
}

impl Dataset<Signature> {
    pub fn count_common(&self, other: &Dataset<Signature>) -> u64 {
        let ng: &Signature = self.data().unwrap();
        let ong: &Signature = other.data().unwrap();

        // TODO: select the right signatures...
        ng.signatures[0].count_common(&ong.signatures[0]).unwrap() as u64
    }

    pub fn mins(&self) -> Vec<u64> {
        let ng: &Signature = self.data().unwrap();
        ng.signatures[0].mins.to_vec()
    }
}

impl Comparable<Dataset<Signature>> for Dataset<Signature> {
    fn similarity(&self, other: &Dataset<Signature>) -> f64 {
        let ng: &Signature = self.data().unwrap();
        let ong: &Signature = other.data().unwrap();

        // TODO: select the right signatures...
        ng.signatures[0].compare(&ong.signatures[0]).unwrap()
    }

    fn containment(&self, other: &Dataset<Signature>) -> f64 {
        let ng: &Signature = self.data().unwrap();
        let ong: &Signature = other.data().unwrap();

        // TODO: select the right signatures...
        let common = ng.signatures[0].count_common(&ong.signatures[0]).unwrap();
        let size = ng.signatures[0].mins.len();
        common as f64 / size as f64
    }
}
