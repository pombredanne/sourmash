//! # Compressed representations of genomic data
//!
//! A signature is a sketch of a genomic dataset.

pub mod minhash;
pub mod ukhs;

use std::fs::File;
use std::io;
use std::iter::Iterator;
use std::path::Path;
use std::str;

use derive_builder::Builder;
use failure::Error;
use serde_derive::{Deserialize, Serialize};

use crate::errors::SourmashError;
use crate::signatures::minhash::KmerMinHash;
use crate::signatures::ukhs::FlatUKHS;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Signatures {
    MinHash(KmerMinHash),
    UKHS(FlatUKHS),
}

pub trait SigsTrait {
    fn size(&self) -> usize;
    fn to_vec(&self) -> Vec<u64>;
    fn check_compatible(&self, other: &Self) -> Result<(), Error>;
    fn add_sequence(&mut self, seq: &[u8], _force: bool) -> Result<(), Error>;
    fn ksize(&self) -> usize;
}

impl SigsTrait for Signatures {
    fn size(&self) -> usize {
        match *self {
            Signatures::UKHS(ref ukhs) => ukhs.size(),
            Signatures::MinHash(ref mh) => mh.size(),
        }
    }

    fn to_vec(&self) -> Vec<u64> {
        match *self {
            Signatures::UKHS(ref ukhs) => ukhs.to_vec(),
            Signatures::MinHash(ref mh) => mh.to_vec(),
        }
    }

    fn ksize(&self) -> usize {
        match *self {
            Signatures::UKHS(ref ukhs) => ukhs.ksize(),
            Signatures::MinHash(ref mh) => mh.ksize(),
        }
    }

    fn check_compatible(&self, other: &Self) -> Result<(), Error> {
        match *self {
            Signatures::UKHS(ref ukhs) => match other {
                Signatures::UKHS(ref ot) => ukhs.check_compatible(ot),
                _ => Err(SourmashError::MismatchSignatureType.into()),
            },
            Signatures::MinHash(ref mh) => match other {
                Signatures::MinHash(ref ot) => mh.check_compatible(ot),
                _ => Err(SourmashError::MismatchSignatureType.into()),
            },
        }
    }

    fn add_sequence(&mut self, seq: &[u8], force: bool) -> Result<(), Error> {
        match *self {
            Signatures::UKHS(ref mut ukhs) => ukhs.add_sequence(seq, force),
            Signatures::MinHash(ref mut mh) => mh.add_sequence(seq, force),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder)]
pub struct Signature {
    #[serde(default = "default_class")]
    #[builder(default = "default_class()")]
    pub class: String,

    #[serde(default)]
    #[builder(setter(skip))]
    pub email: String,

    pub hash_function: String,

    #[builder(setter(skip))]
    pub filename: Option<String>,

    pub name: Option<String>,

    #[serde(default = "default_license")]
    #[builder(default = "default_license()")]
    pub license: String,

    pub signatures: Vec<Signatures>,

    #[serde(default = "default_version")]
    #[builder(default = "default_version()")]
    pub version: f64,
}

fn default_license() -> String {
    "CC0".to_string()
}

fn default_class() -> String {
    "sourmash_signature".to_string()
}

fn default_version() -> f64 {
    0.4
}

impl Signature {
    pub fn builder() -> SignatureBuilder {
        SignatureBuilder::default()
    }

    pub fn name(&self) -> String {
        if let Some(name) = &self.name {
            name.clone()
        } else if let Some(filename) = &self.filename {
            filename.clone()
        } else {
            // TODO md5sum case
            unimplemented!()
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Vec<Signature>, Error> {
        let mut reader = io::BufReader::new(File::open(path)?);
        Ok(Signature::from_reader(&mut reader)?)
    }

    pub fn from_reader<R>(rdr: &mut R) -> Result<Vec<Signature>, Error>
    where
        R: io::Read,
    {
        let sigs: Vec<Signature> = serde_json::from_reader(rdr)?;
        Ok(sigs)
    }

    pub fn load_signatures<R>(
        buf: &mut R,
        ksize: usize,
        moltype: Option<&str>,
        _scaled: Option<u64>,
    ) -> Result<Vec<Signature>, Error>
    where
        R: io::Read,
    {
        let orig_sigs = Signature::from_reader(buf)?;

        let flat_sigs = orig_sigs.into_iter().flat_map(|s| {
            s.signatures
                .iter()
                .map(|mh| {
                    let mut new_s = s.clone();
                    new_s.signatures = vec![mh.clone()];
                    new_s
                })
                .collect::<Vec<Signature>>()
        });

        let filtered_sigs = flat_sigs.filter_map(|mut sig| {
            let good_mhs: Vec<Signatures> = sig
                .signatures
                .into_iter()
                .filter(|sig| {
                    if let Signatures::MinHash(mh) = sig {
                        if ksize == 0 || ksize == mh.ksize() as usize {
                            match moltype {
                                Some(x) => {
                                    if (x.to_lowercase() == "dna" && !mh.is_protein())
                                        || (x.to_lowercase() == "protein" && mh.is_protein())
                                    {
                                        return true;
                                    }
                                }
                                None => return true,
                            };
                        };
                    } else {
                        // TODO: what if it is not a minhash?
                        unimplemented!();
                    }
                    false
                })
                .collect();

            if good_mhs.is_empty() {
                return None;
            };

            sig.signatures = good_mhs;
            Some(sig)
        });

        Ok(filtered_sigs.collect())
    }
}

impl Default for Signature {
    fn default() -> Signature {
        Signature {
            class: default_class(),
            email: "".to_string(),
            hash_function: "0.murmur64".to_string(),
            license: default_license(),
            filename: None,
            name: None,
            signatures: Vec::<Signatures>::new(),
            version: default_version(),
        }
    }
}

impl PartialEq for Signature {
    fn eq(&self, other: &Signature) -> bool {
        let metadata = self.class == other.class
            && self.email == other.email
            && self.hash_function == other.hash_function
            && self.filename == other.filename
            && self.name == other.name;

        // TODO: find the right signature
        // as long as we have a matching
        if let Signatures::MinHash(mh) = &self.signatures[0] {
            if let Signatures::MinHash(other_mh) = &other.signatures[0] {
                return metadata && (mh == other_mh);
            }
        }
        metadata
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;
    use std::path::PathBuf;

    use super::Signature;

    #[test]
    fn load_sig() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/.sbt.v3/60f7e23c24a8d94791cc7a8680c493f9");

        let mut reader = BufReader::new(File::open(filename).unwrap());
        let sigs = Signature::load_signatures(&mut reader, 31, Some("DNA".into()), None).unwrap();
        let _sig_data = sigs[0].clone();
        // TODO: check sig_data
    }
}
