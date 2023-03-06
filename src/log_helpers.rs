use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use std::fmt;

use crate::kv::Result;

#[derive(Debug)]
pub struct LogReader<T: Read> {
  pub reader: BufReader<T>,
  pub pos: u64, // the position of the log
}

impl<T: Read + Seek> LogReader<T> {
  pub fn new(mut reader: T) -> Result<Self> {
    let pos = reader.seek(SeekFrom::Current(0))?;
    Ok(LogReader {
      reader: BufReader::new(reader),
      pos,
    })
  }
}

impl<T: Read + Seek> Read for LogReader<T> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let len = self.reader.read(buf)?;
    self.pos += len as u64;
    Ok(len)
  }
}

impl<T: Read + Seek> Seek for LogReader<T> {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    self.pos = self.reader.seek(pos)?;
    Ok(self.pos)
  }
}

impl<T: Read + Seek> BufRead for LogReader<T> {
  fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
    self.reader.fill_buf()
  }

  fn consume(&mut self, amt: usize) {
    // @TODO: This might be incorrect
    self.pos += amt as u64;
    self.reader.consume(amt);
  }
}

pub struct LogWriter<T: Write> {
  pub writer: BufWriter<T>,
  pub filename: String,
  pub pos: u64,
}

impl<T: Write + Seek> LogWriter<T> {
  pub fn new(mut writer: T, filename: String) -> Result<Self> {
    let pos = writer.seek(SeekFrom::End(0))?;
    Ok(LogWriter {
      writer: BufWriter::new(writer),
      pos,
      filename,
    })
  }
}

impl<T: Write + Seek> Write for LogWriter<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    let bytes_written = self.writer.write(buf)?;
    self.pos += bytes_written as u64;
    Ok(bytes_written)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.writer.flush()
  }
}

impl<T: Write> fmt::Debug for LogWriter<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Point")
      .field("writer", &"writer")
      .field("pos", &self.pos)
      .finish()
  }
}

impl<T: Write + Seek> Seek for LogWriter<T> {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    self.pos = self.writer.seek(pos)?;
    Ok(self.pos)
  }
}