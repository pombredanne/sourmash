use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use failure::Error;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_derive::{Deserialize, Serialize};
use ukhs;

use crate::errors::SourmashError;

pub struct UKHS {
    ukhs: ukhs::UKHS,
    buckets: Vec<u64>,
}

impl UKHS {
    pub fn new(ksize: usize, wsize: usize) -> Result<UKHS, Error> {
        let wk_ukhs = ukhs::UKHS::new(ksize, wsize)?;
        let len = wk_ukhs.len();

        Ok(UKHS {
            ukhs: wk_ukhs,
            buckets: vec![0; len],
        })
    }

    pub fn reset(&mut self) {
        self.buckets = vec![0; self.ukhs.len()];
    }

    pub fn add_sequence(&mut self, seq: &[u8], _force: bool) -> Result<(), Error> {
        let it: Vec<(u64, u64)> = self.ukhs.hash_iter_sequence(seq)?.collect();

        it.into_iter()
            .map(|(_, k_hash)| {
                self.buckets[self.ukhs.query_bucket(k_hash).unwrap()] += 1;
            })
            .count();

        Ok(())
    }

    pub fn to_vec(&self) -> Vec<u64> {
        self.buckets.clone()
    }

    pub fn save<P: AsRef<Path>>(&self, path: P, _name: &str) -> Result<(), Error> {
        let file = File::open(&path)?;
        let mut writer = BufWriter::new(file);

        self.to_writer(&mut writer)
    }

    pub fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        if let Ok(_) = serde_json::to_writer(writer, &self) {
            Ok(())
        } else {
            let st = serde_json::to_string(&self);
            //dbg!(st);
            Err(SourmashError::SerdeError.into())
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<UKHS, Error> {
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let ukhs = UKHS::from_reader(reader)?;
        Ok(ukhs)
    }

    pub fn from_reader<R>(rdr: R) -> Result<UKHS, Error>
    where
        R: Read,
    {
        let ukhs: UKHS = serde_json::from_reader(rdr)?;
        Ok(ukhs)
    }
}

impl Serialize for UKHS {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let n_fields = 5;

        let mut partial = serializer.serialize_struct("UKHS", n_fields)?;
        partial.serialize_field("signature", &self.buckets)?;
        partial.serialize_field("W", &self.ukhs.W())?;
        partial.serialize_field("K", &self.ukhs.K())?;
        partial.serialize_field("size", &self.buckets.len())?;
        partial.serialize_field("name", "".into())?;

        // TODO: properly set name
        //partial.serialize_field("name", &self.name)?;

        partial.end()
    }
}

impl<'de> Deserialize<'de> for UKHS {
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
        let mut u = UKHS::new(tmpukhs.K, tmpukhs.W).unwrap();

        u.buckets = tmpukhs.signature;

        //TODO: what to do with name?

        Ok(u)
    }
}

impl PartialEq for UKHS {
    fn eq(&self, other: &UKHS) -> bool {
        self.buckets == other.buckets
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

        let mut ukhs = UKHS::new(9, 21).unwrap();

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

        let mut ukhs = UKHS::new(9, 21).unwrap();

        let (mut input, _) = get_input(filename.to_str().unwrap()).unwrap();
        let reader = Reader::new(input);

        for record in reader.records() {
            let record = record.unwrap();
            ukhs.add_sequence(record.seq(), false);
        }

        let mut buffer = Vec::new();
        ukhs.to_writer(&mut buffer);

        match UKHS::from_reader(&buffer[..]) {
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
