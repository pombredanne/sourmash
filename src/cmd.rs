use std::path::PathBuf;
use std::rc::Rc;

use bio::io::fasta;
use failure::Error;
use log::info;
use ocf::{get_input, get_output, CompressionFormat};

use crate::index::storage::{FSStorage, Storage};
use crate::index::{Dataset, Index, UKHSTree};
use crate::signatures::ukhs::{FlatUKHS, UKHSTrait, UniqueUKHS};
use crate::signatures::{Signature, SigsTrait};

pub fn draff_index(sig_files: Vec<&str>, outfile: &str) -> Result<(), Error> {
    let storage: Rc<dyn Storage> = Rc::new(
        FSStorage::builder()
            .basepath(".draff".into())
            .build()
            .unwrap(),
    );

    let mut index = UKHSTree::builder().storage(storage).build().unwrap();

    for filename in sig_files {
        // TODO: check for stdin? can also use get_input()?

        let sig = FlatUKHS::load(&filename)?;

        let dataset: Dataset<Signature> = sig.into();
        // TODO: properly set name, filename for the dataset

        index.insert(&dataset);
    }

    // TODO: implement to_writer and use this
    //let mut output = get_output(outfile, CompressionFormat::No)?;
    //index.to_writer(&mut output)?

    index.save_file(outfile)
}

pub fn draff_signature(files: Vec<&str>, k: usize, w: usize) -> Result<(), Error> {
    for filename in files {
        // TODO: check for stdin?

        let mut ukhs = UniqueUKHS::new(k, w)?;

        info!("Build signature for {} with W={}, K={}...", filename, w, k);

        let (input, _) = get_input(filename)?;
        let reader = fasta::Reader::new(input);

        for record in reader.records() {
            // TODO: N in sequence?
            ukhs.add_sequence(record?.seq(), false)?;
        }

        let mut outfile = PathBuf::from(filename);
        outfile.set_extension("sig");

        let mut output = get_output(outfile.to_str().unwrap(), CompressionFormat::No)?;

        let flat: FlatUKHS = ukhs.into();
        flat.to_writer(&mut output)?
    }
    info!("Done.");

    Ok(())
}
