use std::sync::{Arc, Mutex, RwLock};

use crate::disk::{Disk, Page, PAGE_SIZE};

pub struct DiskCacheInner {
    disk: Arc<Mutex<Disk>>,
    pages: Vec<(usize, Arc<RwLock<PageContainer>>)>,
    last_used: usize,
}

#[derive(Clone)]
pub struct DiskCache(Arc<Mutex<DiskCacheInner>>);

pub struct PageContainer {
    pub id: usize,
    pub page: Page,
    last_used: usize,
    dirty: bool,
}

pub struct PageRef(Arc<RwLock<PageContainer>>);

impl DiskCache {
    pub fn new(disk: Arc<Mutex<Disk>>) -> Self {
        DiskCache(Arc::new(Mutex::new(DiskCacheInner {
            disk,
            pages: Vec::new(),
            last_used: 0,
        })))
    }

    pub fn add_page(&self, page: Page) -> usize {
        let mut disk_cache = self.0.lock().unwrap();
        let page_id = disk_cache.disk.lock().unwrap().next_page_id();

        let pc = Arc::new(RwLock::new(PageContainer {
            id: page_id,
            page,
            last_used: 0,
            dirty: true,
        }));
        disk_cache.pages.push((page_id, pc));

        page_id
    }

    pub fn next_page_id(&self) -> usize{
        self.0.lock().unwrap().disk.lock().unwrap().next_page_id()
    }

    pub fn get(&self, id: usize) -> PageRef {
        let (disk, last_used) = {
            let mut dc = self.0.lock().unwrap();
            dc.last_used += 1;
            if let Some(pc) = dc.pages.iter().find(|pc| pc.0 == id) {
                pc.1.write().unwrap().last_used = dc.last_used;
                return PageRef(pc.1.clone());
            }
            (dc.disk.clone(), dc.last_used)
        };

        let page = disk.lock().unwrap().get(id);
        let pc = Arc::new(RwLock::new(PageContainer {
            id,
            page,
            last_used,
            dirty: false,
        }));

        let mut dc = self.0.lock().unwrap();
        dc.pages.push((id, pc.clone()));
        PageRef(pc)
    }

    pub fn close(&self) {
        let dc = self.0.lock().unwrap();
        let mut disk = dc.disk.lock().unwrap();
        for (_, pc) in &dc.pages {
            let mut pc = pc.write().unwrap();
            if pc.dirty {
                pc.dirty = false;
                disk.save(pc.id, &pc.page);
            }
        }
    }
}

impl PageRef {
    pub fn read(&self) -> std::sync::RwLockReadGuard<PageContainer> {
        self.0.read().unwrap()
    }

    pub fn write(&self) -> std::sync::RwLockWriteGuard<PageContainer> {
        let mut pc = self.0.write().unwrap();
        pc.dirty = true;
        pc
    }
}

impl<'a> std::ops::Deref for PageContainer {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a> std::ops::DerefMut for PageContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

#[test]
fn test() {
    let dc = DiskCache::new(Arc::new(Mutex::new(Disk::open("hello"))));
    dbg!(dc.get(0).read()[0]);
}

////////////////////////////
impl std::fmt::Debug for PageContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for x in &self.page {
            write!(f, "{} ", x)?;
        }
        write!(f, "\n")
    }
}

pub trait PageOps {
    fn new_leaf() -> Self;
    fn set_size(&mut self, size: usize);
    fn slice(&self, offset: usize, size: usize) -> &[u8];
    fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8];
    fn write(&mut self, offset: usize, bytes: &[u8]);
}

impl PageOps for Page {
    fn new_leaf() -> Self {
        let mut page = Page::from([0; PAGE_SIZE as usize]);
        page[0] = 1;
        page
    }

    fn set_size(&mut self, size: usize) {
        self[1 + 4..1 + 4 + 2].copy_from_slice(&(size as u16).to_le_bytes());
    }

    #[inline]
    fn slice(&self, offset: usize, size: usize) -> &[u8] {
        &self[offset..offset + size]
    }

    #[inline]
    fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        &mut self[offset..offset + size]
    }

    #[inline]
    fn write(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
}
