extern crate clap;
extern crate kvs;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::env;
use std::process::exit;

fn main() -> Result<()> {
  let matches = App::new(env!("CARGO_PKG_NAME"))
    .version(env!("CARGO_PKG_VERSION"))
    .author(env!("CARGO_PKG_AUTHORS"))
    .about(env!("CARGO_PKG_DESCRIPTION"))
    .setting(AppSettings::DisableHelpSubcommand)
    .setting(AppSettings::SubcommandRequiredElseHelp)
    .setting(AppSettings::VersionlessSubcommands)
    .subcommands(vec![
      SubCommand::with_name("get").about("Get the value").arg(
        Arg::with_name("key")
          .takes_value(true)
          .value_name("KEY")
          .required(true)
          .index(1),
      ),
      SubCommand::with_name("set")
        .about("set the value")
        .arg(
          Arg::with_name("key")
            .takes_value(true)
            .value_name("KEY")
            .required(true)
            .index(1),
        )
        .arg(
          Arg::with_name("value")
            .takes_value(true)
            .value_name("VALUE")
            .required(true)
            .index(2),
        ),
      SubCommand::with_name("rm").about("Remove a value").arg(
        Arg::with_name("key")
          .takes_value(true)
          .value_name("KEY")
          .required(true)
          .index(1),
      ),
    ])
    .get_matches();

  match matches.subcommand() {
    ("get", Some(get_matches)) => {
      let key = get_matches.value_of("key").unwrap();
      let mut store = KvStore::open(env::current_dir()?)?;
      match store.get(key.to_string())? {
        Some(x) => println!("{}", x),
        None => println!("Key not found"),
      };
    }
    ("set", Some(set_matches)) => {
      let key = set_matches.value_of("key").unwrap();
      let value = set_matches.value_of("value").unwrap();

      let mut store = KvStore::open(env::current_dir()?)?;
      store.set(key.to_string(), value.to_string())?;
    }
    ("rm", Some(rm_matches)) => {
      let key = rm_matches.value_of("key").unwrap();
      let mut store = KvStore::open(env::current_dir()?)?;

      if store.remove(key.to_string()).is_err() {
        println!("Key not found");
        exit(1);
      };
    }
    ("", None) => panic!("No subcommand was used"), // If no subcommand was usd it'll match the tuple ("", None)
    _ => unreachable!(),
  }

  Ok(())
}