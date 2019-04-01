use crate::index::sbt::{Node, Update};
use crate::index::{Comparable, Dataset};
use crate::signatures::ukhs::FlatUKHS;
use crate::signatures::Signature;

impl Update<Node<FlatUKHS>> for Node<FlatUKHS> {
    fn update(&self, _other: &mut Node<FlatUKHS>) {
        unimplemented!();
    }
}

impl Update<Node<FlatUKHS>> for Dataset<Signature> {
    fn update(&self, other: &mut Node<FlatUKHS>) {
        unimplemented!();
    }
}

impl Comparable<Node<FlatUKHS>> for Node<FlatUKHS> {
    fn similarity(&self, _other: &Node<FlatUKHS>) -> f64 {
        unimplemented!();
    }

    fn containment(&self, _other: &Node<FlatUKHS>) -> f64 {
        unimplemented!();
    }
}

impl Comparable<Dataset<Signature>> for Node<FlatUKHS> {
    fn similarity(&self, _other: &Dataset<Signature>) -> f64 {
        unimplemented!();
    }

    fn containment(&self, _other: &Dataset<Signature>) -> f64 {
        unimplemented!();
    }
}
