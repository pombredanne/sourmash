use std::fs::File;
use std::io;
use std::path::Path;
use std::rc::Rc;

use clap::{load_yaml, App};
use exitfailure::ExitFailure;
use failure::Error;
use lazy_init::Lazy;
use log::{info, LevelFilter};

use sourmash::cmd::{draff_compare, draff_index, draff_search, draff_signature};
use sourmash::index::sbt::scaffold;
use sourmash::index::search::{
    search_minhashes, search_minhashes_containment, search_minhashes_find_best,
};
use sourmash::index::{Comparable, Dataset, DatasetBuilder, Index, MHBT};
use sourmash::signatures::{Signature, Signatures, SigsTrait};

struct Query<T> {
    data: T,
}

impl Query<Signature> {
    fn ksize(&self) -> u64 {
        // TODO: select the correct signature
        self.data.signatures[0].ksize() as u64
    }

    fn moltype(&self) -> String {
        // TODO: this might panic
        if let Signatures::MinHash(mh) = &self.data.signatures[0] {
            if mh.is_protein() {
                "protein".into()
            } else {
                "DNA".into()
            }
        } else {
            // TODO what if this is not a minhash?
            unimplemented!();
        }
    }

    fn name(&self) -> String {
        self.data.name().clone()
    }
}

impl From<Query<Signature>> for Dataset<Signature> {
    fn from(other: Query<Signature>) -> Dataset<Signature> {
        let data = Lazy::new();
        data.get_or_create(|| other.data);

        DatasetBuilder::default()
            .data(Rc::new(data))
            .filename("".into())
            .name("".into())
            .metadata("".into())
            .storage(None)
            .build()
            .unwrap()
    }
}

fn load_query_signature(
    query: &str,
    ksize: usize,
    moltype: Option<&str>,
    scaled: Option<u64>,
) -> Result<Query<Signature>, Error> {
    let mut reader = io::BufReader::new(File::open(query)?);
    let sigs = Signature::load_signatures(&mut reader, ksize, moltype, scaled)?;

    //dbg!(&sigs);
    // TODO: what if we have more than one left?
    let data = sigs[0].clone();

    Ok(Query { data })
}

struct Database {
    data: MHBT,
    path: String,
}

fn load_sbts_and_sigs(
    filenames: &[&str],
    query: &Query<Signature>,
    _containment: bool,
    traverse: bool,
) -> Result<Vec<Database>, Error> {
    let mut dbs = Vec::default();

    let _ksize = query.ksize();
    let _moltype = query.moltype();

    let n_signatures = 0;
    let mut n_databases = 0;

    for path in filenames {
        if traverse && Path::new(path).is_dir() {
            continue;
        }

        if let Ok(data) = MHBT::from_path(path) {
            // TODO: check compatible
            dbs.push(Database {
                data,
                path: String::from(*path),
            });
            info!("loaded SBT {}", path);
            n_databases += 1;
            continue;
        }

        // TODO: load sig, need to change Database
        // IDEA: put sig into a LinearIndex, and replace Database with a Box<dyn Index>?
    }

    if n_signatures > 0 && n_databases > 0 {
        info!(
            "loaded {} signatures and {} databases total.",
            n_signatures, n_databases
        );
    } else if n_signatures > 0 {
        info!("loaded {} signatures.", n_signatures);
    } else if n_databases > 0 {
        info!("loaded {} databases.", n_databases);
    } else {
        return Err(failure::err_msg("Couldn't load any databases"));
    }

    Ok(dbs)
}

struct Results {
    similarity: f64,
    match_sig: Signature,
}

fn search_databases(
    query: Query<Signature>,
    databases: &[Database],
    threshold: f64,
    containment: bool,
    best_only: bool,
    _ignore_abundance: bool,
) -> Result<Vec<Results>, Error> {
    let mut results = Vec::default();

    let search_fn = if best_only {
        search_minhashes_find_best()
    } else if containment {
        search_minhashes_containment
    } else {
        search_minhashes
    };
    let query_leaf = query.into();

    // TODO: set up scaled for DB and query

    for db in databases {
        let matches = db.data.find(search_fn, &query_leaf, threshold).unwrap();
        for dataset in matches.into_iter() {
            let similarity = query_leaf.similarity(dataset);

            // should always be true, but... better safe than sorry.
            if similarity >= threshold {
                results.push(Results {
                    similarity,
                    match_sig: dataset.clone().into(),
                })
            }
        }
    }

    results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap());
    Ok(results)
}

fn main() -> Result<(), ExitFailure> {
    //setup_panic!();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let yml = load_yaml!("sourmash.yml");
    let m = App::from_yaml(yml).get_matches();

    match m.subcommand_name() {
        Some("draff") => {
            let cmd = m.subcommand_matches("draff").unwrap();
            let inputs = cmd
                .values_of("inputs")
                .map(|vals| vals.collect::<Vec<_>>())
                .unwrap();

            let ksize: usize = cmd.value_of("ksize").unwrap().parse().unwrap();
            let wsize: usize = cmd.value_of("wsize").unwrap().parse().unwrap();

            draff_signature(inputs, ksize, wsize)?;
        }
        Some("draff_search") => {
            let cmd = m.subcommand_matches("draff_search").unwrap();

            let index: &str = cmd.value_of("index").unwrap();
            let query: &str = cmd.value_of("query").unwrap();

            draff_search(index, query)?;
        }
        Some("draff_compare") => {
            let cmd = m.subcommand_matches("draff_compare").unwrap();
            let inputs = cmd
                .values_of("inputs")
                .map(|vals| vals.collect::<Vec<_>>())
                .unwrap();

            draff_compare(inputs)?;
        }
        Some("index") => {
            let cmd = m.subcommand_matches("index").unwrap();
            let inputs = cmd
                .values_of("inputs")
                .map(|vals| vals.collect::<Vec<_>>())
                .unwrap();

            let output: &str = cmd.value_of("output").unwrap();

            draff_index(inputs, output)?;
        }
        Some("scaffold") => {
            let cmd = m.subcommand_matches("scaffold").unwrap();
            let sbt_file = cmd.value_of("current_sbt").unwrap();

            let sbt = MHBT::from_path(sbt_file)?;
            let mut new_sbt: MHBT = scaffold(sbt.datasets(), Rc::clone(&sbt.storage()));

            new_sbt.save_file("test", None)?;

            assert_eq!(new_sbt.datasets().len(), sbt.datasets().len());
        }
        Some("search") => {
            let cmd = m.subcommand_matches("search").unwrap();

            if cmd.is_present("quiet") {
                log::set_max_level(LevelFilter::Warn);
            }

            let query = load_query_signature(
                cmd.value_of("query").unwrap(),
                if cmd.is_present("ksize") {
                    cmd.value_of("ksize").unwrap().parse().unwrap()
                } else {
                    0
                },
                Some("dna"), // TODO: select moltype,
                if cmd.is_present("scaled") {
                    Some(cmd.value_of("scaled").unwrap().parse().unwrap())
                } else {
                    None
                },
            )?;

            info!(
                "loaded query: {}... (k={}, {})",
                query.name(),
                query.ksize(),
                query.moltype()
            );

            let containment = cmd.is_present("containment");
            let traverse_directory = cmd.is_present("traverse-directory");
            let databases = load_sbts_and_sigs(
                &cmd.values_of("databases")
                    .map(|vals| vals.collect::<Vec<_>>())
                    .unwrap(),
                &query,
                containment,
                traverse_directory,
            )?;

            if databases.is_empty() {
                return Err(failure::err_msg("Nothing found to search!").into());
            }

            let best_only = cmd.is_present("best-only");
            let threshold = cmd.value_of("threshold").unwrap().parse().unwrap();
            let ignore_abundance = cmd.is_present("ignore-abundance");
            let results = search_databases(
                query,
                &databases,
                threshold,
                containment,
                best_only,
                ignore_abundance,
            )?;

            let num_results = if best_only {
                1
            } else {
                cmd.value_of("num-results").unwrap().parse().unwrap()
            };

            let n_matches = if num_results == 0 || results.len() <= num_results {
                println!("{} matches:", results.len());
                results.len()
            } else {
                println!("{} matches; showing first {}:", results.len(), num_results);
                num_results
            };

            println!("similarity   match");
            println!("----------   -----");
            for sr in &results[..n_matches] {
                println!(
                    "{:>5.1}%       {:60}",
                    sr.similarity * 100.,
                    sr.match_sig.name()
                );
            }

            if best_only {
                info!("** reporting only one match because --best-only was set")
            }

            /*
            if args.output:
                fieldnames = ['similarity', 'name', 'filename', 'md5']
                w = csv.DictWriter(args.output, fieldnames=fieldnames)
                w.writeheader()

                for sr in &results:
                    d = dict(sr._asdict())
                    del d['match_sig']
                    w.writerow(d)

            if args.save_matches:
                outname = args.save_matches.name
                info!("saving all matched signatures to \"{}\"", outname)
                Signature::save_signatures([sr.match_sig for sr in results], args.save_matches)
            */
        }
        _ => {
            println!("{:?}", m);
        }
    }
    Ok(())
}
