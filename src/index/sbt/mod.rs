pub mod mhbt;
pub mod ukhs;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{BufReader, Read};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use derive_builder::Builder;
use failure::Error;
use lazy_init::Lazy;
use serde_derive::{Deserialize, Serialize};

use crate::index::storage::{FSStorage, Storage, StorageInfo, ToWriter};
use crate::index::{Comparable, Dataset, DatasetInfo, Index};
use crate::signatures::Signature;

use crate::signatures::ukhs::{FlatUKHS, UKHSTrait};

pub trait Update<O> {
    fn update(&self, other: &mut O) -> Result<(), Error>;
}

pub trait FromFactory<N> {
    fn factory(&self, name: &str) -> Result<N, Error>;
}

#[derive(Builder)]
pub struct SBT<N, L> {
    #[builder(default = "2")]
    d: u32,

    storage: Rc<dyn Storage>,

    #[builder(setter(skip))]
    factory: Factory,

    #[builder(default = "HashMap::default()")]
    nodes: HashMap<u64, N>,

    #[builder(default = "HashMap::default()")]
    leaves: HashMap<u64, L>,
}

const fn parent(pos: u64, d: u64) -> u64 {
    ((pos - 1) / d) as u64
}

const fn child(parent: u64, pos: u64, d: u64) -> u64 {
    d * parent + pos + 1
}

impl<N, L> SBT<N, L>
where
    L: std::clone::Clone + Default,
    N: Default,
{
    pub fn builder() -> SBTBuilder<N, L> {
        SBTBuilder::default()
    }

    #[inline(always)]
    fn parent(&self, pos: u64) -> Option<u64> {
        if pos == 0 {
            None
        } else {
            Some(parent(pos, u64::from(self.d)))
        }
    }

    #[inline(always)]
    fn child(&self, parent: u64, pos: u64) -> u64 {
        child(parent, pos, u64::from(self.d))
    }

    #[inline(always)]
    fn children(&self, pos: u64) -> Vec<u64> {
        (0..u64::from(self.d)).map(|c| self.child(pos, c)).collect()
    }

    pub fn storage(&self) -> Rc<dyn Storage> {
        Rc::clone(&self.storage)
    }

    // combine
}

impl<T, U> SBT<Node<U>, Dataset<T>>
where
    T: std::marker::Sync + ToWriter,
    U: std::marker::Sync + ToWriter,
{
    pub fn from_reader<R, P>(rdr: &mut R, path: P) -> Result<SBT<Node<U>, Dataset<T>>, Error>
    where
        R: Read,
        P: AsRef<Path>,
    {
        // TODO: check https://serde.rs/enum-representations.html for a
        // solution for loading v4 and v5
        let sbt: SBTInfo<NodeInfo, DatasetInfo> = serde_json::from_reader(rdr)?;

        // TODO: match with available Storage while we don't
        // add a function to build a Storage from a StorageInfo
        let mut basepath = PathBuf::new();
        basepath.push(path);
        basepath.push(&sbt.storage.args["path"]);

        let storage: Rc<dyn Storage> = Rc::new(FSStorage { basepath });

        Ok(SBT {
            d: sbt.d,
            factory: sbt.factory,
            storage: Rc::clone(&storage),
            nodes: sbt
                .nodes
                .into_iter()
                .map(|(n, l)| {
                    let new_node = Node {
                        filename: l.filename,
                        name: l.name,
                        metadata: l.metadata,
                        storage: Some(Rc::clone(&storage)),
                        data: Rc::new(Lazy::new()),
                    };
                    (n, new_node)
                })
                .collect(),
            leaves: sbt
                .leaves
                .into_iter()
                .map(|(n, l)| {
                    let new_node = Dataset {
                        filename: l.filename,
                        name: l.name,
                        metadata: l.metadata,
                        storage: Some(Rc::clone(&storage)),
                        data: Rc::new(Lazy::new()),
                    };
                    (n, new_node)
                })
                .collect(),
        })
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<SBT<Node<U>, Dataset<T>>, Error> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        // TODO: match with available Storage while we don't
        // add a function to build a Storage from a StorageInfo
        let mut basepath = PathBuf::new();
        basepath.push(path);
        basepath.canonicalize()?;

        let sbt =
            SBT::<Node<U>, Dataset<T>>::from_reader(&mut reader, &basepath.parent().unwrap())?;
        Ok(sbt)
    }

    pub fn save_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        let mut args: HashMap<String, String> = HashMap::default();
        //TODO: read this from storage
        args.insert("path".into(), ".sbt".into());

        let storage = StorageInfo {
            backend: "FSStorage".into(),
            args,
        };

        //TODO: still need to trigger saving each node under storage!

        let info: SBTInfo<NodeInfo, DatasetInfo> = SBTInfo {
            d: self.d,
            factory: self.factory.clone(),
            storage,
            version: 5,
            nodes: self
                .nodes
                .iter()
                .map(|(n, l)| {
                    // TODO: set storage to new one?
                    let filename = (*l).save(&l.filename).unwrap();
                    let new_node = NodeInfo {
                        filename: filename,
                        name: l.name.clone(),
                        metadata: l.metadata.clone(),
                    };
                    (*n, new_node)
                })
                .collect(),
            leaves: self
                .leaves
                .iter()
                .map(|(n, l)| {
                    // TODO: set storage to new one?
                    let filename = (*l).save(&l.filename).unwrap();
                    let new_node = DatasetInfo {
                        filename: filename,
                        name: l.name.clone(),
                        metadata: l.metadata.clone(),
                    };
                    (*n, new_node)
                })
                .collect(),
        };

        let file = File::create(path)?;
        serde_json::to_writer(file, &info)?;

        Ok(())
    }
}

impl<N, L> Index for SBT<N, L>
where
    N: Comparable<N> + Comparable<L> + Update<N> + Debug + Default,
    L: Comparable<L> + Update<N> + Clone + Debug + Default,
    SBT<N, L>: FromFactory<N>,
{
    type Item = L;

    fn find<F>(&self, search_fn: F, sig: &L, threshold: f64) -> Result<Vec<&L>, Error>
    where
        F: Fn(&dyn Comparable<Self::Item>, &Self::Item, f64) -> bool,
    {
        let mut matches = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = vec![0u64];

        while !queue.is_empty() {
            let pos = queue.pop().unwrap();
            if !visited.contains(&pos) {
                visited.insert(pos);

                if let Some(node) = self.nodes.get(&pos) {
                    dbg!((node, sig, node.similarity(sig)));
                    if search_fn(&node, sig, threshold) {
                        for c in self.children(pos) {
                            queue.push(c);
                        }
                    }
                } else if let Some(leaf) = self.leaves.get(&pos) {
                    dbg!((leaf, sig, leaf.similarity(sig)));
                    if search_fn(leaf, sig, threshold) {
                        matches.push(leaf);
                    }
                }
            }
        }

        Ok(matches)
    }

    fn insert(&mut self, dataset: &L) -> Result<(), Error> {
        if self.leaves.is_empty() {
            // in this case the tree is empty,
            // just add the dataset to the first available leaf
            self.leaves.entry(0).or_insert(dataset.clone());
            return Ok(());
        }

        // we can unwrap here because the root node case
        // only happens on an empty tree, and if we got
        // to this point we have at least one leaf already.
        // TODO: find position by similarity search
        let pos = self.leaves.keys().max().unwrap() + 1;
        let parent_pos = self.parent(pos).unwrap();

        if let Entry::Occupied(pnode) = self.leaves.entry(parent_pos) {
            // Case 1: parent is a Leaf
            // create a new internal node, add it to self.nodes[parent_pos]

            let (_, leaf) = pnode.remove_entry();

            let mut new_node = self.factory(&format!("internal.{}", parent_pos))?;

            // for each children update the parent node
            // TODO: write the update method
            leaf.update(&mut new_node)?;
            dataset.update(&mut new_node)?;

            // node and parent are children of new internal node
            let mut c_pos = self.children(parent_pos).into_iter().take(2);
            let c1_pos = c_pos.next().unwrap();
            let c2_pos = c_pos.next().unwrap();

            self.leaves.entry(c1_pos).or_insert(leaf);
            self.leaves.entry(c2_pos).or_insert(dataset.clone());

            // add the new internal node to self.nodes[parent_pos)
            // TODO check if it is really empty?
            self.nodes.entry(parent_pos).or_insert(new_node);
        } else {
            // TODO: moved these two lines here to avoid borrow checker
            // error E0502 in the Vacant case, but would love to avoid it!
            let mut new_node = self.factory(&format!("internal.{}", parent_pos))?;
            let c_pos = self.children(parent_pos)[0];

            match self.nodes.entry(parent_pos) {
                // Case 2: parent is a node and has an empty child spot available
                // (if there isn't an empty spot, it was already covered by case 1)
                Entry::Occupied(mut pnode) => {
                    dataset.update(&mut pnode.get_mut())?;
                    self.leaves.entry(pos).or_insert(dataset.clone());
                }

                // Case 3: parent is None/empty
                // this can happen with d != 2, need to create parent node
                Entry::Vacant(pnode) => {
                    self.leaves.entry(c_pos).or_insert(dataset.clone());
                    dataset.update(&mut new_node)?;
                    pnode.insert(new_node);
                }
            }
        }

        let mut parent_pos = parent_pos;
        while let Some(ppos) = self.parent(parent_pos) {
            if let Entry::Occupied(mut pnode) = self.nodes.entry(parent_pos) {
                //TODO: use children for this node to update, instead of dragging
                // dataset up to the root? It would be more generic, but this
                // works for minhash, draff signatures and nodegraphs...
                dataset.update(&mut pnode.get_mut())?;
            }
            parent_pos = ppos;
        }

        Ok(())
    }

    fn save<P: AsRef<Path>>(&self, _path: P) -> Result<(), Error> {
        unimplemented!()
    }

    fn load<P: AsRef<Path>>(_path: P) -> Result<(), Error> {
        unimplemented!()
    }

    fn datasets(&self) -> Vec<Self::Item> {
        self.leaves.values().cloned().collect()
    }
}

#[derive(Builder, Clone, Default, Serialize, Deserialize)]
pub struct Factory {
    class: String,
    args: Vec<u64>,
}

#[derive(Builder, Default, Clone)]
pub struct Node<T>
where
    T: std::marker::Sync,
{
    filename: String,
    name: String,
    metadata: HashMap<String, u64>,
    storage: Option<Rc<dyn Storage>>,
    #[builder(setter(skip))]
    pub(crate) data: Rc<Lazy<T>>,
}

impl<T> Node<T>
where
    T: Sync + ToWriter,
{
    pub fn save(&self, path: &str) -> Result<String, Error> {
        if let Some(storage) = &self.storage {
            if let Some(data) = self.data.get() {
                let mut buffer = Vec::new();
                data.to_writer(&mut buffer)?;

                Ok(storage.save(path, &buffer)?)
            } else {
                unimplemented!()
            }
        } else {
            unimplemented!()
        }
    }
}

impl<T> Dataset<T>
where
    T: Sync + ToWriter,
{
    pub fn save(&self, path: &str) -> Result<String, Error> {
        if let Some(storage) = &self.storage {
            if let Some(data) = self.data.get() {
                let mut buffer = Vec::new();
                data.to_writer(&mut buffer)?;

                Ok(storage.save(path, &buffer)?)
            } else {
                unimplemented!()
            }
        } else {
            unimplemented!()
        }
    }
}

impl<T> std::fmt::Debug for Node<T>
where
    T: std::marker::Sync + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node [name={}, filename={}, metadata: {:?}, data: {:?}]",
            self.name,
            self.filename,
            self.metadata,
            self.data.get().is_some()
        )
    }
}

#[derive(Serialize, Deserialize)]
struct NodeInfo {
    filename: String,
    name: String,
    metadata: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
struct SBTInfo<N, L> {
    d: u32,
    version: u32,
    storage: StorageInfo,
    factory: Factory,
    nodes: HashMap<u64, N>,
    leaves: HashMap<u64, L>,
}

// This comes from finch
pub struct NoHashHasher(u64);

impl Default for NoHashHasher {
    #[inline]
    fn default() -> NoHashHasher {
        NoHashHasher(0x0)
    }
}

impl Hasher for NoHashHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        *self = NoHashHasher(
            (u64::from(bytes[0]) << 24)
                + (u64::from(bytes[1]) << 16)
                + (u64::from(bytes[2]) << 8)
                + u64::from(bytes[3]),
        );
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

type HashIntersection = HashSet<u64, BuildHasherDefault<NoHashHasher>>;

enum BinaryTree {
    Empty,
    Internal(Box<TreeNode<HashIntersection>>),
    Dataset(Box<TreeNode<Dataset<Signature>>>),
}

struct TreeNode<T> {
    element: T,
    left: BinaryTree,
    right: BinaryTree,
}

pub fn scaffold<N>(mut datasets: Vec<Dataset<Signature>>) -> SBT<Node<N>, Dataset<Signature>>
where
    N: std::marker::Sync + std::clone::Clone + std::default::Default,
{
    let mut leaves: HashMap<u64, Dataset<Signature>> = HashMap::with_capacity(datasets.len());

    let mut next_round = Vec::new();

    // generate two bottom levels:
    // - datasets
    // - first level of internal nodes
    eprintln!("Start processing leaves");
    while !datasets.is_empty() {
        let next_leaf = datasets.pop().unwrap();

        let (simleaf_tree, in_common) = if datasets.is_empty() {
            (
                BinaryTree::Empty,
                HashIntersection::from_iter(next_leaf.mins().into_iter()),
            )
        } else {
            let mut similar_leaf_pos = 0;
            let mut current_max = 0;
            for (pos, leaf) in datasets.iter().enumerate() {
                let common = next_leaf.count_common(leaf);
                if common > current_max {
                    current_max = common;
                    similar_leaf_pos = pos;
                }
            }

            let similar_leaf = datasets.remove(similar_leaf_pos);

            let in_common = HashIntersection::from_iter(next_leaf.mins().into_iter())
                .union(&HashIntersection::from_iter(
                    similar_leaf.mins().into_iter(),
                ))
                .cloned()
                .collect();

            let simleaf_tree = BinaryTree::Dataset(Box::new(TreeNode {
                element: similar_leaf,
                left: BinaryTree::Empty,
                right: BinaryTree::Empty,
            }));
            (simleaf_tree, in_common)
        };

        let leaf_tree = BinaryTree::Dataset(Box::new(TreeNode {
            element: next_leaf,
            left: BinaryTree::Empty,
            right: BinaryTree::Empty,
        }));

        let tree = BinaryTree::Internal(Box::new(TreeNode {
            element: in_common,
            left: leaf_tree,
            right: simleaf_tree,
        }));

        next_round.push(tree);

        if next_round.len() % 100 == 0 {
            eprintln!("Processed {} leaves", next_round.len() * 2);
        }
    }
    eprintln!("Finished processing leaves");

    // while we don't get to the root, generate intermediary levels
    while next_round.len() != 1 {
        next_round = BinaryTree::process_internal_level(next_round);
        eprintln!("Finished processing round {}", next_round.len());
    }

    // Convert from binary tree to nodes/leaves
    let root = next_round.pop().unwrap();
    let mut visited = HashSet::new();
    let mut queue = vec![(0u64, root)];

    while !queue.is_empty() {
        let (pos, cnode) = queue.pop().unwrap();
        if !visited.contains(&pos) {
            visited.insert(pos);

            match cnode {
                BinaryTree::Dataset(leaf) => {
                    leaves.insert(pos, leaf.element);
                }
                BinaryTree::Internal(mut node) => {
                    let left = std::mem::replace(&mut node.left, BinaryTree::Empty);
                    let right = std::mem::replace(&mut node.right, BinaryTree::Empty);
                    queue.push((2 * pos + 1, left));
                    queue.push((2 * pos + 2, right));
                }
                BinaryTree::Empty => (),
            }
        }
    }

    // save the new tree
    // TODO: make a proper basepath here!
    let storage: Rc<dyn Storage> = Rc::new(FSStorage {
        basepath: ".sbt".into(),
    });

    SBTBuilder::default()
        .storage(storage)
        .nodes(HashMap::default())
        .leaves(leaves)
        .build()
        .unwrap()
}

impl BinaryTree {
    fn process_internal_level(mut current_round: Vec<BinaryTree>) -> Vec<BinaryTree> {
        let mut next_round = Vec::with_capacity(current_round.len() + 1);

        while !current_round.is_empty() {
            let next_node = current_round.pop().unwrap();

            let similar_node = if current_round.is_empty() {
                BinaryTree::Empty
            } else {
                let mut similar_node_pos = 0;
                let mut current_max = 0;
                for (pos, cmpe) in current_round.iter().enumerate() {
                    let common = BinaryTree::intersection_size(&next_node, &cmpe);
                    if common > current_max {
                        current_max = common;
                        similar_node_pos = pos;
                    }
                }
                current_round.remove(similar_node_pos)
            };

            let tree = BinaryTree::new_tree(next_node, similar_node);

            next_round.push(tree);
        }
        next_round
    }

    fn new_tree(mut left: BinaryTree, mut right: BinaryTree) -> BinaryTree {
        let in_common = if let BinaryTree::Internal(ref mut el1) = left {
            match right {
                BinaryTree::Internal(ref mut el2) => {
                    let c1 = std::mem::replace(&mut el1.element, HashIntersection::default());
                    let c2 = std::mem::replace(&mut el2.element, HashIntersection::default());
                    c1.union(&c2).cloned().collect()
                }
                BinaryTree::Empty => {
                    std::mem::replace(&mut el1.element, HashIntersection::default())
                }
                _ => panic!("Should not see a Dataset at this level"),
            }
        } else {
            HashIntersection::default()
        };

        BinaryTree::Internal(Box::new(TreeNode {
            element: in_common,
            left,
            right,
        }))
    }

    fn intersection_size(n1: &BinaryTree, n2: &BinaryTree) -> usize {
        if let BinaryTree::Internal(ref el1) = n1 {
            if let BinaryTree::Internal(ref el2) = n2 {
                return el1.element.intersection(&el2.element).count();
            }
        };
        0
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;
    use std::io::{Seek, SeekFrom};
    use std::path::PathBuf;
    use std::rc::Rc;
    use tempfile;

    use lazy_init::Lazy;

    use super::scaffold;

    use crate::index::linear::LinearIndexBuilder;
    use crate::index::search::{search_minhashes, search_minhashes_containment};
    use crate::index::storage::Storage;
    use crate::index::{DatasetBuilder, Index, MHBT};
    use crate::signatures::Signature;

    #[test]
    fn save_sbt() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/v5.sbt.json");

        let sbt = MHBT::from_path(filename).expect("Loading error");

        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        sbt.save_file(tmpfile.path()).unwrap();

        tmpfile.seek(SeekFrom::Start(0)).unwrap();

        let _sbt = MHBT::from_path(tmpfile.path()).expect("Loading error");
        // TODO: check values?
    }

    #[test]
    fn load_sbt() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/v5.sbt.json");

        let sbt = MHBT::from_path(filename).expect("Loading error");

        assert_eq!(sbt.d, 2);
        //assert_eq!(sbt.storage.backend, "FSStorage");
        //assert_eq!(sbt.storage.args["path"], ".sbt.v5");
        assert_eq!(sbt.factory.class, "GraphFactory");
        assert_eq!(sbt.factory.args, [1, 100000, 4]);

        println!("sbt leaves {:?} {:?}", sbt.leaves.len(), sbt.leaves);

        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/.sbt.v3/60f7e23c24a8d94791cc7a8680c493f9");

        let mut reader = BufReader::new(File::open(filename).unwrap());
        let sigs = Signature::load_signatures(&mut reader, 31, Some("DNA".into()), None).unwrap();
        let sig_data = sigs[0].clone();

        let data = Lazy::new();
        data.get_or_create(|| sig_data);

        let leaf = DatasetBuilder::default()
            .data(Rc::new(data))
            .filename("".into())
            .name("".into())
            .metadata("".into())
            .storage(None)
            .build()
            .unwrap();

        let results = sbt.find(search_minhashes, &leaf, 0.5).unwrap();
        assert_eq!(results.len(), 1);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);

        let results = sbt.find(search_minhashes, &leaf, 0.1).unwrap();
        assert_eq!(results.len(), 2);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);

        let mut linear = LinearIndexBuilder::default()
            .storage(Rc::clone(&sbt.storage) as Rc<dyn Storage>)
            .build()
            .unwrap();
        for l in &sbt.leaves {
            linear.insert(l.1);
        }

        println!(
            "linear leaves {:?} {:?}",
            linear.datasets.len(),
            linear.datasets
        );

        let results = linear.find(search_minhashes, &leaf, 0.5).unwrap();
        assert_eq!(results.len(), 1);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);

        let results = linear.find(search_minhashes, &leaf, 0.1).unwrap();
        assert_eq!(results.len(), 2);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);

        let results = linear
            .find(search_minhashes_containment, &leaf, 0.5)
            .unwrap();
        assert_eq!(results.len(), 2);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);

        let results = linear
            .find(search_minhashes_containment, &leaf, 0.1)
            .unwrap();
        assert_eq!(results.len(), 4);
        println!("results: {:?}", results);
        println!("leaf: {:?}", leaf);
    }

    #[test]
    fn scaffold_sbt() {
        let mut filename = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        filename.push("tests/test-data/v5.sbt.json");

        let sbt = MHBT::from_path(filename).expect("Loading error");

        let new_sbt: MHBT = scaffold(sbt.datasets());

        assert_eq!(new_sbt.datasets().len(), 7);
    }
}
