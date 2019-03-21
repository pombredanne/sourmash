use crate::index::Comparable;

pub fn search_minhashes<L>(node: &dyn Comparable<L>, query: &L, threshold: f64) -> bool {
    node.similarity(query) > threshold
}

pub fn search_minhashes_containment<L>(
    node: &dyn Comparable<L>,
    query: &L,
    threshold: f64,
) -> bool {
    node.containment(query) > threshold
}

pub fn search_minhashes_find_best<L>() -> fn(&dyn Comparable<L>, &L, f64) -> bool {
    let mut _best_so_far = 0.;

    // TODO: implement the proper function, as a closure that modifies
    // `best_so_far`
    search_minhashes
}
