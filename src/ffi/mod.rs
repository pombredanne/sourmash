pub mod minhash;
pub mod signature;

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::_hash_murmur;

#[no_mangle]
pub extern "C" fn hash_murmur(kmer: *const c_char, seed: u64) -> u64 {
    let c_str = unsafe {
        assert!(!kmer.is_null());

        CStr::from_ptr(kmer)
    };

    _hash_murmur(c_str.to_bytes(), seed)
}
