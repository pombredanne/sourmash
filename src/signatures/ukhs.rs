use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use failure::Error;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_derive::{Deserialize, Serialize};
use ukhs;

use crate::errors::SourmashError;
use crate::index::nodegraph::Nodegraph;

pub struct UKHS<T> {
    ukhs: ukhs::UKHS,
    buckets: Vec<T>,
}

pub type FullUKHS = UKHS<Nodegraph>;
pub type FlatUKHS = UKHS<u64>;

pub trait UKHSTrait {
    type Storage;

    fn new(ksize: usize, wsize: usize) -> Result<UKHS<Self::Storage>, Error>;

    fn reset(&mut self);

    fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write;

    fn save<P: AsRef<Path>>(&self, path: P, _name: &str) -> Result<(), Error> {
        let file = File::open(&path)?;
        let mut writer = BufWriter::new(file);

        self.to_writer(&mut writer)
    }

    fn add_sequence(&mut self, seq: &[u8], _force: bool) -> Result<(), Error>;

    fn to_vec(&self) -> Vec<u64>;

    fn load<P: AsRef<Path>>(path: P) -> Result<FlatUKHS, Error> {
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let ukhs = FlatUKHS::from_reader(reader)?;
        Ok(ukhs)
    }

    fn from_reader<R>(rdr: R) -> Result<FlatUKHS, Error>
    where
        R: Read,
    {
        let ukhs = serde_json::from_reader(rdr)?;
        Ok(ukhs)
    }
}

impl UKHSTrait for UKHS<u64> {
    type Storage = u64;

    fn new(ksize: usize, wsize: usize) -> Result<UKHS<u64>, Error> {
        let wk_ukhs = ukhs::UKHS::new(ksize, wsize)?;
        let len = wk_ukhs.len();

        Ok(UKHS {
            ukhs: wk_ukhs,
            buckets: vec![0; len],
        })
    }

    fn reset(&mut self) {
        self.buckets = vec![0; self.ukhs.len()];
    }

    fn add_sequence(&mut self, seq: &[u8], _force: bool) -> Result<(), Error> {
        let it: Vec<(u64, u64)> = self.ukhs.hash_iter_sequence(seq)?.collect();

        it.into_iter()
            .map(|(_, k_hash)| {
                self.buckets[self.ukhs.query_bucket(k_hash).unwrap()] += 1;
            })
            .count();

        Ok(())
    }

    fn to_vec(&self) -> Vec<u64> {
        self.buckets.clone()
    }

    fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        match serde_json::to_writer(writer, &self) {
            Ok(_) => Ok(()),
            Err(_) => Err(SourmashError::SerdeError.into()),
        }
    }
}

impl UKHSTrait for UKHS<Nodegraph> {
    type Storage = Nodegraph;

    fn new(ksize: usize, wsize: usize) -> Result<Self, Error> {
        let wk_ukhs = ukhs::UKHS::new(ksize, wsize)?;
        let len = wk_ukhs.len();

        Ok(UKHS {
            ukhs: wk_ukhs,
            buckets: vec![Nodegraph::with_tables(100000, 4, wsize); len],
        })
    }

    fn reset(&mut self) {
        self.buckets = vec![Nodegraph::with_tables(100000, 4, self.ukhs.W()); self.ukhs.len()];
    }

    fn add_sequence(&mut self, seq: &[u8], _force: bool) -> Result<(), Error> {
        let it: Vec<(u64, u64)> = self.ukhs.hash_iter_sequence(seq)?.collect();

        it.into_iter()
            .map(|(w_hash, k_hash)| {
                self.buckets[self.ukhs.query_bucket(k_hash).unwrap()].count(w_hash);
            })
            .count();

        Ok(())
    }

    fn to_vec(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|b| b.unique_kmers() as u64)
            .collect()
    }

    fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        // TODO: avoid cloning?
        let flat: FlatUKHS = self.into();

        match serde_json::to_writer(writer, &flat) {
            Ok(_) => Ok(()),
            Err(_) => Err(SourmashError::SerdeError.into()),
        }
    }
}

impl From<FullUKHS> for FlatUKHS {
    fn from(other: FullUKHS) -> Self {
        let buckets = other.to_vec(); // TODO: implement into_vec?
        let ukhs = other.ukhs;

        FlatUKHS { ukhs, buckets }
    }
}

impl From<&FullUKHS> for FlatUKHS {
    fn from(other: &FullUKHS) -> Self {
        // TODO: implement clone for ukhs::UKHS?
        let wk_ukhs = ukhs::UKHS::new(other.ukhs.K(), other.ukhs.W()).unwrap();

        FlatUKHS {
            ukhs: wk_ukhs,
            buckets: other.to_vec(), // TODO: also implement into_vec?
        }
    }
}

impl Serialize for UKHS<u64> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let n_fields = 5;

        let buckets = self.buckets.to_vec();

        let mut partial = serializer.serialize_struct("UKHS", n_fields)?;
        partial.serialize_field("signature", &buckets)?;
        partial.serialize_field("W", &self.ukhs.W())?;
        partial.serialize_field("K", &self.ukhs.K())?;
        partial.serialize_field("size", &self.buckets.len())?;
        partial.serialize_field("name", "".into())?;

        // TODO: properly set name
        //partial.serialize_field("name", &self.name)?;

        partial.end()
    }
}

impl<'de> Deserialize<'de> for UKHS<u64> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TempUKHS {
            signature: Vec<u64>,
            K: usize,
            W: usize,
            size: usize,
            name: String,
        }

        let tmpukhs = TempUKHS::deserialize(deserializer)?;

        //TODO: remove this unwrap, need to map Failure error to serde error?
        let mut u = UKHS::<u64>::new(tmpukhs.K, tmpukhs.W).unwrap();

        u.buckets = tmpukhs.signature;

        //TODO: what to do with name?

        Ok(u)
    }
}

impl<T> PartialEq for UKHS<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &UKHS<T>) -> bool {
        self.buckets
            .iter()
            .zip(other.buckets.iter())
            .all(|(b1, b2)| b1 == b2)
            && self.ukhs.W() == other.ukhs.W()
            && self.ukhs.K() == self.ukhs.K()
    }
}

#[cfg(test)]
mod test {
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;

    use bio::io::fasta::{Reader, Record};
    use ocf::get_input;

    use super::*;

    #[test]
    fn ukhs_add_sequence() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/ecoli.genes.fna");

        let mut ukhs = FullUKHS::new(9, 21).unwrap();

        let (mut input, _) = get_input(filename.to_str().unwrap()).unwrap();
        let reader = Reader::new(input);

        for record in reader.records() {
            let record = record.unwrap();
            ukhs.add_sequence(record.seq(), false);
        }

        // TODO: find test case...
        //assert_eq!(ukhs.to_vec(), [1, 2, 3]);
    }

    #[test]
    fn ukhs_writer_reader() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/ecoli.genes.fna");

        let mut ukhs = FlatUKHS::new(9, 21).unwrap();

        let (mut input, _) = get_input(filename.to_str().unwrap()).unwrap();
        let reader = Reader::new(input);

        for record in reader.records() {
            let record = record.unwrap();
            ukhs.add_sequence(record.seq(), false);
        }

        let mut buffer = Vec::new();
        ukhs.to_writer(&mut buffer);

        match FlatUKHS::from_reader(&buffer[..]) {
            Ok(new_ukhs) => {
                assert_eq!(ukhs.buckets, new_ukhs.buckets);
            }
            Err(e) => {
                dbg!(e);
                assert_eq!(1, 0);
            }
        }
    }
}
