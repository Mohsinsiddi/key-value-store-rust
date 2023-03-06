use chrono::prelude::Utc;
use failure::{format_err, Error};
use std::collections::HashMap;
use std::fs::{create_dir, read_dir, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path:: {PathBuf};

use crate::log_helpers::{LogReader, LogWriter};
use serde::Deserialize;
use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Set { key: String, value: String },
  Rm { key: String },
}

#[derive(Debug)]
struct LogReference {
  filename: String,
  pos: u64,
  size: u64,
  timestamp: String,
}

impl LogReference {
  pub fn new(filename: String, pos: u64, size: u64) -> Self {
    LogReference {
      filename,
      pos,
      size,
      timestamp: Utc::now().to_rfc3339(),
    }
  }
}

#[derive(Debug)]
pub struct KvStore {
  store: HashMap<String, LogReference>,
  writer: LogWriter<File>,
  readers: HashMap<String, LogReader<File>>,
}

impl KvStore {
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    let log = Log::Set { key, value };
    self.write(log)
  }

  pub fn get(&mut self, key: String) -> Result<Option<String>> {
    if let Some(log_ref) = self.store.get(&key) {
      let reader = self.readers.get_mut(&log_ref.filename).unwrap();
      let mut buffer = vec![0; log_ref.size as usize];

      reader.seek(SeekFrom::Start(log_ref.pos as u64))?;
      reader.read_exact(&mut buffer)?;

      match serde_json::from_slice(&buffer)? {
        Log::Set { value, .. } => Ok(Some(value)),
        Log::Rm { .. } => Ok(None),
      }
    } else {
      Ok(None)
    }
  }

  pub fn remove(&mut self, key: String) -> Result<()> {
    if self.store.contains_key(&key) {
      self.store.remove(&key);
      let log = Log::Rm { key };

      self.write(log)
    } else {
      Err(format_err!("Key not found"))
    }
  }

  fn write(&mut self, log: Log) -> Result<()> {
    let serialized = serde_json::to_vec(&log).unwrap();
    let file_name = self.writer.filename.clone();
    let start_pos = self.writer.pos;
    let bytes_writen = self.writer.write(&serialized)? as u64;
    self.writer.flush()?;

    match log {
      Log::Rm { key } => {
        let log_ref = LogReference::new(file_name, start_pos as u64, bytes_writen);
        self.store.insert(key, log_ref);
      }
      Log::Set { key, .. } => {
        let log_ref = LogReference::new(file_name, start_pos as u64, bytes_writen);
        self.store.insert(key, log_ref);
      }
    }

    Ok(())
  }

  fn process_file(
    log_reader: &mut LogReader<File>,
    store: &mut HashMap<String, LogReference>,
    filename: &str,
  ) -> Result<()> {
    let mut pos = log_reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(log_reader).into_iter::<Log>();

    while let Some(log) = stream.next() {
      let new_pos = stream.byte_offset() as u64;
      match log? {
        Log::Rm { key } => {
          store.insert(
            key.clone(),
            LogReference::new(String::from(filename), pos, new_pos - pos),
          );
        }
        Log::Set { key, .. } => {
          store.insert(
            key.clone(),
            LogReference::new(String::from(filename), pos, new_pos - pos),
          );
        }
      }
      pos = new_pos;
    }

    Ok(())
  }

  pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
    let mut current_dir = path.into();
    current_dir.push("data");
    let mut readers: HashMap<String, LogReader<File>> = HashMap::new();
    let mut store: HashMap<String, LogReference> = HashMap::new();
    let entries = fetch_entries(&current_dir)?;

    // Insert each file to the reader hashmap
    for entry in &entries {
      let file = File::open(entry)?;
      let file_name = entry.file_name().unwrap().to_str().unwrap();
      let mut log_reader = LogReader::new(file)?;
      KvStore::process_file(&mut log_reader, &mut store, &file_name)?;

      readers.insert(String::from(file_name), log_reader);
    }

    let (writer_path_buf, writer_filename) = match entries.last() {
      Some(v) => {
        // @TODO: If the size of the file is too large, make a new file
        (v, String::from(v.file_name().unwrap().to_str().unwrap()))
      }
      None => {
        let filename = format!("{}.txt", Utc::now());
        current_dir.push(&filename);
        (&current_dir, filename)
      }
    };

    let file = OpenOptions::new()
      .read(true)
      .append(true)
      .create(true)
      .open(writer_path_buf)?;
    // @TODO move this somewhere else
    let file_clone = file.try_clone()?;
    let writer_filename_clone = writer_filename.clone();

    let writer = LogWriter::new(file, writer_filename)?;
    readers.insert(writer_filename_clone, LogReader::new(file_clone)?);

    Ok(KvStore {
      readers,
      writer,
      store,
    })
  }
}

fn fetch_entries(current_dir: &PathBuf) -> Result<Vec<PathBuf>> {
  let mut entries: Vec<PathBuf> = Vec::new();
  let data_folder = &current_dir.as_path();

  if data_folder.is_dir() {
    entries = read_dir(data_folder)?
      .filter_map(std::result::Result::ok)
      .map(|res| res.path())
      .collect::<Vec<_>>();

    entries.sort();
  } else {
    create_dir(data_folder)?;
  }

  Ok(entries)
}