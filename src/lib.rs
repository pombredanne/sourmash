pub mod errors;

#[macro_use]
pub mod utils;

pub mod index;

pub mod signatures;

#[cfg(feature = "from-finch")]
pub mod from;

use cfg_if::cfg_if;
use murmurhash3::murmurhash3_x64_128;

cfg_if! {
    if #[cfg(target_arch = "wasm32")] {
        pub mod wasm;
    } else {
        pub mod ffi;
    }
}

type HashIntoType = u64;

pub fn _hash_murmur(kmer: &[u8], seed: u64) -> u64 {
    murmurhash3_x64_128(kmer, seed).0
}
