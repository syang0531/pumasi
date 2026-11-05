//! A minimal ZIP writer (stored entries, no compression) for "download
//! all outputs": every OS opens it without extra tools, and job outputs
//! are often compressed already. Limited to 4 GiB (no ZIP64).

use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::{Result, ensure};

pub struct ZipWriter<W: Write + Seek> {
    out: W,
    entries: Vec<(String, u32, u32, u32)>,
    dos_time: u16,
    dos_date: u16,
}

const LIMIT: u64 = u32::MAX as u64;

impl<W: Write + Seek> ZipWriter<W> {
    pub fn new(out: W, unix: u64) -> Self {
        let iso = crate::fmt::iso_utc(unix.max(315_532_800));
        let n = |a: usize, b: usize| iso[a..b].parse::<u16>().unwrap_or(0);
        Self {
            out,
            entries: Vec::new(),
            dos_time: (n(11, 13) << 11) | (n(14, 16) << 5) | (n(17, 19) / 2),
            dos_date: ((n(0, 4).saturating_sub(1980)) << 9) | (n(5, 7) << 5) | n(8, 10),
        }
    }

    /// Adds a file at `name` (`/`-separated) with what `data` reads.
    pub fn add(&mut self, name: &str, data: &mut dyn Read) -> Result<()> {
        let offset = self.out.stream_position()?;
        ensure!(offset < LIMIT, "the archive would be larger than 4 GiB");
        self.local_header(name, 0, 0)?;
        let mut crc = crc32fast::Hasher::new();
        let mut size: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = data.read(&mut buf)?;
            if n == 0 {
                break;
            }
            crc.update(&buf[..n]);
            self.out.write_all(&buf[..n])?;
            size += n as u64;
        }
        ensure!(size < LIMIT, "{name} is larger than 4 GiB");
        let crc = crc.finalize();
        let end = self.out.stream_position()?;
        self.out.seek(SeekFrom::Start(offset))?;
        self.local_header(name, crc, size as u32)?;
        self.out.seek(SeekFrom::Start(end))?;
        self.entries
            .push((name.to_string(), crc, size as u32, offset as u32));
        Ok(())
    }

    fn local_header(&mut self, name: &str, crc: u32, size: u32) -> io::Result<()> {
        let o = &mut self.out;
        o.write_all(&0x0403_4b50u32.to_le_bytes())?;
        o.write_all(&20u16.to_le_bytes())?; // version needed
        o.write_all(&0x0800u16.to_le_bytes())?; // UTF-8 names
        o.write_all(&0u16.to_le_bytes())?; // stored
        o.write_all(&self.dos_time.to_le_bytes())?;
        o.write_all(&self.dos_date.to_le_bytes())?;
        o.write_all(&crc.to_le_bytes())?;
        o.write_all(&size.to_le_bytes())?;
        o.write_all(&size.to_le_bytes())?;
        o.write_all(&(name.len() as u16).to_le_bytes())?;
        o.write_all(&0u16.to_le_bytes())?;
        o.write_all(name.as_bytes())
    }

    /// Writes the central directory and returns the output.
    pub fn finish(mut self) -> Result<W> {
        let start = self.out.stream_position()?;
        for (name, crc, size, offset) in &self.entries {
            let o = &mut self.out;
            o.write_all(&0x0201_4b50u32.to_le_bytes())?;
            o.write_all(&0x031eu16.to_le_bytes())?; // made by: unix, 3.0
            o.write_all(&20u16.to_le_bytes())?;
            o.write_all(&0x0800u16.to_le_bytes())?;
            o.write_all(&0u16.to_le_bytes())?;
            o.write_all(&self.dos_time.to_le_bytes())?;
            o.write_all(&self.dos_date.to_le_bytes())?;
            o.write_all(&crc.to_le_bytes())?;
            o.write_all(&size.to_le_bytes())?;
            o.write_all(&size.to_le_bytes())?;
            o.write_all(&(name.len() as u16).to_le_bytes())?;
            o.write_all(&[0u8; 8])?; // extra, comment, disk, internal attrs
            o.write_all(&(0o100644u32 << 16).to_le_bytes())?; // external attrs
            o.write_all(&offset.to_le_bytes())?;
            o.write_all(name.as_bytes())?;
        }
        let end = self.out.stream_position()?;
        ensure!(end < LIMIT, "the archive would be larger than 4 GiB");
        ensure!(
            self.entries.len() < 0xffff,
            "too many files for one archive"
        );
        let n = self.entries.len() as u16;
        let o = &mut self.out;
        o.write_all(&0x0605_4b50u32.to_le_bytes())?;
        o.write_all(&[0u8; 4])?;
        o.write_all(&n.to_le_bytes())?;
        o.write_all(&n.to_le_bytes())?;
        o.write_all(&((end - start) as u32).to_le_bytes())?;
        o.write_all(&(start as u32).to_le_bytes())?;
        o.write_all(&0u16.to_le_bytes())?;
        o.flush()?;
        Ok(self.out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn writes_a_readable_archive() {
        let mut z = ZipWriter::new(Cursor::new(Vec::new()), 1_793_877_679);
        z.add("task-0/out.txt", &mut &b"hello\n"[..]).unwrap();
        z.add("task-1/결과.txt", &mut &b""[..]).unwrap();
        let bytes = z.finish().unwrap().into_inner();
        // Local header, then the data right after the name.
        assert_eq!(&bytes[..4], b"PK\x03\x04");
        let crc = u32::from_le_bytes(bytes[14..18].try_into().unwrap());
        assert_eq!(crc, crc32fast::hash(b"hello\n"));
        let name_len = u16::from_le_bytes(bytes[26..28].try_into().unwrap()) as usize;
        assert_eq!(&bytes[30..30 + name_len], b"task-0/out.txt");
        assert_eq!(&bytes[30 + name_len..36 + name_len], b"hello\n");
        // End of central directory: two entries, pointing at the directory.
        let eocd = bytes.len() - 22;
        assert_eq!(&bytes[eocd..eocd + 4], b"PK\x05\x06");
        assert_eq!(u16::from_le_bytes([bytes[eocd + 10], bytes[eocd + 11]]), 2);
        let cd = u32::from_le_bytes(bytes[eocd + 16..eocd + 20].try_into().unwrap()) as usize;
        assert_eq!(&bytes[cd..cd + 4], b"PK\x01\x02");
    }
}
