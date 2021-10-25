use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{
    disk::{Page, PAGE_SIZE},
    disk_cache::{DiskCache, PageOps},
};

#[derive(Debug, Serialize, Deserialize)]
struct ObjectTable {
    objects: Vec<(String, u32)>,
}

pub fn init_as_object_store(disk_cache: &mut DiskCache) {
    disk_cache.get(0);
    let ot = ObjectTable { objects: vec![] };
    let size_to_write = bincode::serialized_size(&ot).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(disk_cache, pages_num_required, 0);
    bincode::serialize_into(DiskCacheWriter::new(disk_cache, 0), &ot).unwrap();
}

pub fn read_object<T: Serialize + serde::de::DeserializeOwned>(
    disk_cache: &mut DiskCache,
    name: &str,
) -> Option<T> {
    // ensure_pages_to_read(disk_cache, 0);
    let reader = DiskCacheReader::new(disk_cache, 0);
    let ot: ObjectTable = bincode::deserialize_from(reader).unwrap();
    if let Some(o) = ot.objects.iter().find(|o| o.0.as_str() == name) {
        // ensure_pages_to_read(disk_cache, o.1 as usize);
        bincode::deserialize_from(DiskCacheReader::new(disk_cache, o.1 as usize)).ok()
    } else {
        None
    }
}

pub fn write_object<T: Serialize + serde::de::DeserializeOwned>(
    disk_cache: &mut DiskCache,
    name: &str,
    object: &T,
) {
    // ensure_pages_to_read(disk_cache, 0);
    let reader = DiskCacheReader::new(disk_cache, 0);
    let mut ot: ObjectTable = bincode::deserialize_from(reader).unwrap();

    let page_i = if let Some(o) = ot.objects.iter().find(|o| o.0.as_str() == name) {
        o.1 as usize
    } else {
        let i = disk_cache.next_page_id();
        ot.objects.push((name.to_string(), i as u32));
        i
    };

    let size_to_write = bincode::serialized_size(object).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(disk_cache, pages_num_required, page_i);
    bincode::serialize_into(DiskCacheWriter::new(disk_cache, page_i), object).unwrap();

    let size_to_write = bincode::serialized_size(&ot).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(disk_cache, pages_num_required, page_i);
    bincode::serialize_into(DiskCacheWriter::new(disk_cache, 0), &ot).unwrap();
}

// fn ensure_pages_to_read(disk_cache: &mut DiskCache, page_i: usize) {
//     // disk_cache.ensure_page(page_i);
//     let next_page_i = read_next_page_i(disk_cache.get(page_i).read().deref());
//     if next_page_i != 0 {
//         ensure_pages_to_read(disk_cache, next_page_i);
//     };
// }

fn ensure_pages_to_write(disk_cache: &mut DiskCache, pages_num: usize, page_i: usize) {
    // disk_cache.ensure_page(page_i);
    if pages_num == 0 {
        return;
    }
    let next_page_i = read_next_page_i(disk_cache.get(page_i).read().deref());
    let next_page_i = if next_page_i == 0 {
        let next_page_i = disk_cache.next_page_id();
        write_next_page_i(disk_cache.get(page_i).write().deref_mut(), next_page_i);
        next_page_i
    } else {
        next_page_i as usize
    };
    ensure_pages_to_write(disk_cache, pages_num - 1, next_page_i);
}

pub struct DiskCacheWriter<'a> {
    disk_cache: &'a mut DiskCache,
    page_i: Option<usize>,
    i: usize,
    next_page_i: Option<usize>,
}

impl<'a> DiskCacheWriter<'a> {
    pub fn new(disk_cache: &'a mut DiskCache, page_i: usize) -> Self {
        Self {
            disk_cache,
            page_i: Some(page_i),
            i: 0,
            next_page_i: None,
        }
    }
}

impl<'a> std::io::Write for DiskCacheWriter<'a> {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let mut total_write_len = 0;
        while !buf.is_empty() && self.page_i.is_some() {
            let page_ref = self.disk_cache.get(self.page_i.unwrap());
            let mut page = page_ref.write();
            if self.i == 0 {
                self.next_page_i = Some(read_next_page_i(page.deref()));
                if self.next_page_i == Some(0) {
                    self.next_page_i = None;
                }
                self.i += 4;
            }
            let write_len = buf.len().min(PAGE_SIZE as usize - self.i);
            page.slice_mut(self.i, write_len)
                .copy_from_slice(&buf[0..write_len]);
            buf = &buf[write_len..];
            self.i += write_len;
            total_write_len += write_len;

            if self.i >= PAGE_SIZE as usize {
                self.page_i = self.next_page_i;
                self.i = 0;
            }
        }
        Ok(total_write_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct DiskCacheReader<'a> {
    disk_cache: &'a DiskCache,
    page_i: Option<usize>,
    i: usize,
    next_page_i: Option<usize>,
}

impl<'a> DiskCacheReader<'a> {
    pub fn new(disk_cache: &'a DiskCache, page_i: usize) -> Self {
        Self {
            disk_cache,
            page_i: Some(page_i),
            i: 0,
            next_page_i: None,
        }
    }
}

impl<'a> std::io::Read for DiskCacheReader<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_read_len = 0;
        while !buf.is_empty() && self.page_i.is_some() {
            let page_ref = self.disk_cache.get(self.page_i.unwrap());
            let page = page_ref.read();
            if self.i == 0 {
                self.next_page_i = Some(read_next_page_i(page.deref()));
                if self.next_page_i == Some(0) {
                    self.next_page_i = None;
                }
                self.i += 4;
            }
            let read_len = buf.len().min(PAGE_SIZE as usize - self.i);
            buf[0..read_len].copy_from_slice(page.slice(self.i, read_len));
            buf = &mut buf[read_len..];
            self.i += read_len;
            total_read_len += read_len;

            if self.i >= PAGE_SIZE as usize {
                self.page_i = self.next_page_i;
                self.i = 0;
            }
        }
        Ok(total_read_len)
    }
}

fn read_next_page_i(page: &impl Deref<Target = Page>) -> usize {
    use std::convert::TryInto;
    u32::from_le_bytes(page.slice(0, 4).try_into().unwrap()) as usize
}

fn write_next_page_i(page: &mut impl DerefMut<Target = Page>, next_page_i: usize) {
    page.slice_mut(0, 4)
        .copy_from_slice(&(next_page_i as u32).to_le_bytes());
}

#[test]
fn test() {
    let filepath = "hello";
    if let Ok(_) = std::fs::remove_file(filepath) {
        println!("{:?} removed", filepath);
    };
    {
        let mut disk_cache = DiskCache::new(std::sync::Arc::new(std::sync::Mutex::new(
            crate::disk::Disk::open(filepath),
        )));
        init_as_object_store(&mut disk_cache);
        dbg!(read_object::<String>(&mut disk_cache, "hello"));
        write_object::<String>(&mut disk_cache, "hello", &"hello!!!".to_owned());
        write_object::<String>(&mut disk_cache, "too learge", &"too learge...".repeat(100));
        write_object::<String>(&mut disk_cache, "bye", &"bye!!!".to_owned());
        dbg!(read_object::<String>(&mut disk_cache, "hello"));
        dbg!(read_object::<String>(&mut disk_cache, "too learge").map(|x| x.len()));
        dbg!(read_object::<String>(&mut disk_cache, "bye"));
        // dbg!(disk_cache.size());
        disk_cache.close();
    }
    {
        let mut disk_cache = DiskCache::new(std::sync::Arc::new(std::sync::Mutex::new(
            crate::disk::Disk::open(filepath),
        )));
        assert_eq!(
            read_object::<String>(&mut disk_cache, "hello"),
            Some("hello!!!".to_owned())
        );
        assert_eq!(
            dbg!(read_object::<String>(&mut disk_cache, "too learge").map(|x| x.len())),
            Some(1300)
        );
        assert_eq!(
            dbg!(read_object::<String>(&mut disk_cache, "bye")),
            Some("bye!!!".to_owned())
        );
        // dbg!(disk_cache.size());
    }
}
