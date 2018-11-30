extern crate data_encoding;
extern crate linked_hash_map;
extern crate rand;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate filetime;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use data_encoding::BASE64URL_NOPAD;
use linked_hash_map::LinkedHashMap;
use rand::RngCore;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
//use std::time::SystemTime;

pub use self::error::Error;

mod error;

const PARTIAL: &str = "partial";
const ENTRIES: &str = "entries";
const INDEX: &str = "index";
const MAX_KEY_SIZE: usize = 4096;
const FILE_KEY_LEN: usize = 32;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Cache {
    inner: Arc<RwLock<CacheInner>>,
}

impl Cache {
    pub fn new<P: AsRef<Path>>(root: P, max_size: u64, max_files: u64) -> Result<Self> {
        let root = root.as_ref().into();
        CacheInner::new(root, max_size, max_files).map(|cache| Cache {
            inner: Arc::new(RwLock::new(cache)),
        })
    }

    pub fn add<S: AsRef<str>>(&self, key: S) -> Result<FileGuard> {
        let key: String = key.as_ref().into();
        let mut c = self.inner.write().expect("Cannot lock cache");
        c.add(key.clone()).map(move |file| FileGuard {
            cache: self.inner.clone(),
            file: Some(file),
            key,
        })
    }

    pub fn get<S: AsRef<str>>(&self, key: S) -> Option<Result<fs::File>> {
        let mut cache = self.inner.write().expect("Cannot lock cache");
        cache.get(key)
    }

    pub fn save_index(&self) -> Result<()> {
        let cache = self.inner.read().expect("Cannot lock cache");
        cache.save_index()
    }
}

pub struct FileGuard {
    cache: Arc<RwLock<CacheInner>>,
    file: Option<fs::File>,
    key: String,
}

impl io::Write for FileGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.get_file().and_then(|mut f| f.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.get_file().and_then(|mut f| f.flush())
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        // need to clean up if opened item was not properly finished
        let file_name = {
            let cache = self.cache.read().expect("Cannot lock cache");
            cache.partial_path(&self.key)
        };

        if file_name.exists() {
            if let Err(e) = fs::remove_file(&file_name) {
                error!("Cannot delete file {:?}, error {}", file_name, e)
            }
        }
    }
}

impl FileGuard {
    fn get_file(&self) -> io::Result<&fs::File> {
        self.file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "already closed"))
    }
    pub fn finish(&mut self) {
        let mut cache = self.cache.write().expect("Cannot lock cache");
        cache
            .finish(
                self.key.clone(),
                self.file.take().expect("Invalid cache state"),
            )
            .expect("Invalid cache state");
    }
}

fn gen_cache_key() -> String {
    let mut random = [0; FILE_KEY_LEN];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut random);
    return BASE64URL_NOPAD.encode(&random);
}

fn entry_path_helper<P: AsRef<Path>>(root: &PathBuf, file_key: P) -> PathBuf {
    root.join(ENTRIES).join(file_key)
}

struct CacheInner {
    files: LinkedHashMap<String, String>,
    opened: HashMap<String, String>,
    root: PathBuf,
    max_size: u64,
    max_files: u64,
    size: u64,
    num_files: u64,
}

impl CacheInner {
    fn new(root: PathBuf, max_size: u64, max_files: u64) -> Result<Self> {
        let entries_path = root.join(ENTRIES);
        if !entries_path.exists() {
            fs::create_dir(entries_path)?
        }
        let partial_path = root.join(PARTIAL);
        if !partial_path.exists() {
            fs::create_dir(partial_path)?
        }

        let mut cache = CacheInner {
            files: LinkedHashMap::new(),
            opened: HashMap::new(),
            root,
            max_size,
            max_files,
            size: 0,
            num_files: 0,
        };
        if let Err(e) = cache.load_index() {
            error!("Error loading cache index {}", e);
            //TODO - clean entries if cannot load index
        }
        Ok(cache)
    }

    fn add(&mut self, key: String) -> Result<fs::File> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::InvalidKey);
        }
        if self.opened.contains_key(&key) {
            return Err(Error::KeyOpened(key));
        } else if self.files.contains_key(&key) {
            return Err(Error::KeyAlreadyExists(key));
        }

        let mut new_file_key: String;
        loop {
            new_file_key = gen_cache_key();
            let new_path = self.partial_path(new_file_key.clone());
            if !new_path.exists() {
                let f = fs::File::create(&new_path)?;
                self.opened.insert(key, new_file_key);
                return Ok(f);
            }
        }
    }

    fn get<S: AsRef<str>>(&mut self, key: S) -> Option<Result<fs::File>> {
        let k: &str = key.as_ref();
        match self.files.get_refresh(k) {
            None => return None,
            Some(file_key) => {
                let file_name = entry_path_helper(&self.root, &file_key);
                // let now = filetime::FileTime::from_system_time(SystemTime::now());
                // if let Err(e) = filetime::set_file_times(&file_name, now, now) {
                //     error!("Cannot set mtime for file {:?} error {}", file_name, e)
                // }
                Some(fs::File::open(file_name).map_err(|e| e.into()))
            }
        }
    }

    // This works only on *nix, as one can delete safely opened files, Windows might require bit different approach
    fn remove_last(&mut self) -> Result<()> {
        if let Some((_, file_key)) = self.files.pop_front() {
            let file_path = self.entry_path(file_key);
            let file_size = fs::metadata(&file_path)?.len();
            fs::remove_file(file_path)?;
            self.num_files -= 1;
            self.size -= file_size;
        }
        Ok(())
    }

    fn finish(&mut self, key: String, mut file: fs::File) -> Result<()> {
        let file_key = match self.opened.remove(&key) {
            Some(key) => key,
            None => return Err(Error::InvalidCacheState("Missing opened key".into())),
        };
        file.flush()?;
        let old_path = self.partial_path(file_key.clone());
        let new_file_size = file.metadata()?.len();
        while self.size + new_file_size > self.max_size || self.num_files + 1 > self.max_files {
            self.remove_last()?
        }
        let new_path = self.entry_path(&file_key);
        fs::rename(old_path, &new_path)?;
        self.files.insert(key, file_key);
        self.num_files += 1;
        self.size += new_path.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(())
    }

    fn entry_path<P: AsRef<Path>>(&self, file_key: P) -> PathBuf {
        entry_path_helper(&self.root, file_key)
    }

    fn partial_path<P: AsRef<Path>>(&self, file_key: P) -> PathBuf {
        self.root.join(PARTIAL).join(file_key)
    }

    fn save_index(&self) -> Result<()> {
        let tmp_index = self.root.join(String::from(INDEX) + ".tmp");
        {
            let mut f = fs::File::create(&tmp_index)?;
            for (key, value) in self.files.iter() {
                f.write_u16::<BigEndian>(key.len() as u16)?;
                f.write_all(key.as_bytes())?;
                f.write_u16::<BigEndian>(value.len() as u16)?;
                f.write_all(value.as_bytes())?;
            }
        }
        fs::rename(tmp_index, self.root.join(INDEX))?;

        Ok(())
    }

    fn load_index(&mut self) -> Result<()> {
        let index_path = self.root.join(INDEX);

        if index_path.exists() {
            let mut index = LinkedHashMap::<String, String>::new();
            let mut f = fs::File::open(index_path)?;

            loop {
                let key_len = match f.read_u16::<BigEndian>() {
                    Ok(l) => l as usize,
                    Err(e) => match e.kind() {
                        io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(e.into()),
                    },
                };

                if key_len > MAX_KEY_SIZE {
                    return Err(Error::InvalidIndex);
                }

                let mut buf = [0_u8; MAX_KEY_SIZE];
                f.read_exact(&mut buf[..key_len])?;
                let key = String::from_utf8(Vec::from(&buf[..key_len]))
                    .map_err(|_| Error::InvalidIndex)?;
                let value_len = f.read_u16::<BigEndian>()? as usize;
                if value_len > 2 * FILE_KEY_LEN {
                    return Err(Error::InvalidIndex);
                }
                f.read_exact(&mut buf[..value_len])?;
                let value = String::from_utf8(Vec::from(&buf[..value_len]))
                    .map_err(|_| Error::InvalidIndex)?;
                let file_path = self.entry_path(&value);
                if file_path.exists() {
                    index.insert(key, value);
                    self.num_files += 1;
                    self.size += fs::metadata(file_path)?.len();
                }
            }

            self.files = index;
        }
        Ok(())
    }
}

#[cfg(test)]
extern crate env_logger;
#[cfg(test)]
extern crate tempfile;
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    #[test]
    fn basic_test() {
        env_logger::init();
        const MY_KEY: &str = "muj_test_1";
        let temp_dir = tempdir().unwrap();

        let msg = "Hello there";
        {
            let c = Cache::new(temp_dir.path(), 10000, 10).unwrap();
            {
                let mut f = c.add(MY_KEY).unwrap();

                f.write(msg.as_bytes()).unwrap();
                f.finish()
            }
            let mut f = c.get(MY_KEY).unwrap().unwrap();

            let mut msg2 = String::new();
            f.read_to_string(&mut msg2).unwrap();
            assert_eq!(msg, msg2);
            let num_files = c.inner.read().unwrap().num_files;
            assert_eq!(1, num_files);
            c.save_index().unwrap();
        }

        {
            let c = Cache::new(temp_dir.path(), 10000, 10).unwrap();
            let mut f = c.get(MY_KEY).unwrap().unwrap();

            let mut msg2 = String::new();
            f.read_to_string(&mut msg2).unwrap();
            assert_eq!(msg, msg2);
            let num_files = c.inner.read().unwrap().num_files;
            assert_eq!(1, num_files)
        }
    }
}
