use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::rc::Rc;

use derive_builder::Builder;
use failure::{Error, Fail};
use fixedbitset::FixedBitSet;

use crate::index::nodegraph::Nodegraph;
use crate::index::storage::Storage;
use crate::index::{Comparable, Dataset, Index};
use crate::{HashIntoType, Signature};

#[derive(Clone, Builder)]
pub struct BIGSI<L> {
    matrix: Vec<FixedBitSet>,
    ksize: usize,
    datasets: Vec<L>,
    //#[builder(setter(skip))]
    //storage: Rc<dyn Storage>,
}

#[derive(Debug, Fail)]
pub enum BIGSIError {
    #[fail(display = "BIGSI doesn't support this method")]
    MethodDisabled,
}

impl<L> BIGSI<L> {
    pub fn new(bf_size: usize, ksize: usize) -> BIGSI<L> {
        let mut matrix = Vec::with_capacity(bf_size);
        for _ in 0..bf_size {
            // TODO: figure initial capacity for each row
            matrix.push(FixedBitSet::with_capacity(100));
        }

        BIGSI {
            matrix,
            ksize,
            datasets: Vec::new(),
        }
    }
}

impl BIGSI<Signature> {
    pub fn add(&mut self, dataset: Signature) {
        let mut ng = Nodegraph::new(&[self.matrix.len()], self.ksize);

        // TODO: select correct minhash
        let mh = &dataset.signatures[0];
        for h in &mh.mins {
            ng.count(*h);
        }

        self.datasets.push(dataset);
        let col = self.datasets.len() - 1;

        for pos in ng.bs[0].ones() {
            let bs = &mut self.matrix[pos];
            if bs.len() == col {
                bs.grow(col + col / 2);
            }
            bs.insert(col);
        }
    }

    pub fn query(&self, hash: HashIntoType) -> impl Iterator<Item = usize> + '_ {
        let pos = hash as usize % self.matrix.len();
        let bs = &self.matrix[pos];
        bs.ones()
    }

    pub fn query_datasets(&self, hash: HashIntoType) -> impl Iterator<Item = Signature> + '_ {
        self.query(hash)
            .into_iter()
            .map(move |pos| self.datasets[pos].clone())
    }
}

impl Index for BIGSI<Signature> {
    type Item = Signature;

    fn find<F>(
        &self,
        search_fn: F,
        sig: &Self::Item,
        threshold: f64,
    ) -> Result<Vec<&Self::Item>, Error>
    where
        F: Fn(&dyn Comparable<Self::Item>, &Self::Item, f64) -> bool,
    {
        // TODO: is there a better way than making this a runtime check?
        Err(BIGSIError::MethodDisabled.into())
    }

    fn search(
        &self,
        sig: &Self::Item,
        threshold: f64,
        containment: bool,
    ) -> Result<Vec<&Self::Item>, Error> {
        let mut results = Vec::new();

        //TODO: still assuming one mh in the signature!
        let hashes = &sig.signatures[0];

        let mut counter: HashMap<usize, usize> = HashMap::with_capacity(hashes.size());

        for hash in &hashes.mins {
            self.query(*hash)
                .into_iter()
                .map(|dataset_idx| {
                    let idx = counter.entry(dataset_idx).or_insert(0);
                    *idx += 1;
                })
                .count();
        }

        for (idx, count) in counter {
            let match_sig = &self.datasets[idx];
            //TODO: still assuming one mh in the signature!
            let match_mh = match_sig.signatures[0].size();

            let score = if containment {
                count as f64 / hashes.size() as f64
            } else {
                count as f64 / (hashes.size() + match_mh - count) as f64
            };

            if score >= threshold {
                results.push(match_sig)
            }
        }

        Ok(results)
    }

    fn insert(&mut self, node: &Self::Item) {
        self.add(node.clone());
    }

    fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        Ok(())
    }

    fn load<P: AsRef<Path>>(path: P) -> Result<(), Error> {
        Ok(())
    }

    fn datasets(&self) -> Vec<Self::Item> {
        self.datasets.to_vec()
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    use crate::index::sbt::MHBT;
    use crate::index::storage::ReadData;

    #[test]
    fn bigsi_sbt_oracle() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/v5.sbt.json");

        let sbt = MHBT::from_path(filename).expect("Loading error");

        let mut bigsi = BIGSI::new(10000, 10);
        let datasets = sbt.datasets();
        let leaf = &datasets[0];

        for l in &datasets {
            let data = l.data().unwrap();
            bigsi.insert(data);
        }

        let results_sbt = sbt.search(&leaf, 0.5, false).unwrap();
        assert_eq!(results_sbt.len(), 1);

        let data = (*leaf.data).get().unwrap();
        let results_bigsi = bigsi.search(&data, 0.5, false).unwrap();
        assert_eq!(results_bigsi.len(), 1);

        assert_eq!(results_sbt.len(), results_bigsi.len());

        let results_sbt = sbt.search(&leaf, 0.1, false).unwrap();
        assert_eq!(results_sbt.len(), 2);

        let data = (*leaf.data).get().unwrap();
        let results_bigsi = bigsi.search(&data, 0.1, false).unwrap();
        assert_eq!(results_bigsi.len(), 2);

        assert_eq!(results_sbt.len(), results_bigsi.len());
    }
}
