pub mod minhash;
pub mod ukhs;

use serde_derive::{Deserialize, Serialize};

use std::fs::File;
use std::io;
use std::iter::Iterator;
use std::path::Path;
use std::str;

use failure::Error;

use crate::signatures::minhash::KmerMinHash;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Signature {
    #[serde(default = "default_class")]
    pub class: String,

    #[serde(default)]
    pub email: String,
    pub hash_function: String,

    pub filename: Option<String>,
    pub name: Option<String>,

    #[serde(default = "default_license")]
    pub license: String,

    pub signatures: Vec<KmerMinHash>,

    #[serde(default = "default_version")]
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
    pub fn name(&self) -> String {
        if let Some(name) = &self.name {
            name.clone()
        } else if let Some(filename) = &self.filename {
            filename.clone()
        } else {
            //TODO md5sum case
            "".into()
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
            let good_mhs: Vec<KmerMinHash> = sig
                .signatures
                .into_iter()
                .filter(|mh| {
                    if ksize == 0 || ksize == mh.ksize as usize {
                        match moltype {
                            Some(x) => {
                                if (x.to_lowercase() == "dna" && !mh.is_protein)
                                    || (x.to_lowercase() == "protein" && mh.is_protein)
                                {
                                    return true;
                                }
                            }
                            None => return true,
                        };
                    };
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
            signatures: Vec::<KmerMinHash>::new(),
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

        let mh = &self.signatures[0];
        let other_mh = &other.signatures[0];
        metadata && (mh == other_mh)
    }
}
