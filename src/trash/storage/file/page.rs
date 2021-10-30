use super::pager::{PageRaw, PAGE_SIZE};

pub struct Page {
    raw: PageRaw,
}

impl From<PageRaw> for Page {
    fn from(raw: PageRaw) -> Self {
        Page { raw }
    }
}

impl std::ops::Deref for Page {
    type Target = PageRaw;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl std::ops::DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for x in &self.raw {
            write!(f, "{} ", x)?;
        }
        write!(f, "\n")
    }
}

impl Page {
    pub fn new_leaf() -> Self {
        let mut page = Page::from([0; PAGE_SIZE as usize]);
        page[0] = 1;
        page
    }

    pub fn set_parent(&mut self, node_i: usize) {
        self[1..1 + 4].copy_from_slice(&(node_i as u32).to_le_bytes());
    }

    pub fn set_size(&mut self, size: usize) {
        self[1 + 4..1 + 4 + 2].copy_from_slice(&(size as u16).to_le_bytes());
    }

    pub fn set_next(&mut self, node_i: usize) {
        self[1 + 4 + 2..1 + 4 + 2 + 4].copy_from_slice(&(node_i as u32).to_le_bytes());
    }

    #[inline]
    pub fn slice(&self, offset: usize, size: usize) -> &[u8] {
        &self[offset..offset + size]
    }

    #[inline]
    pub fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        &mut self[offset..offset + size]
    }

    #[inline]
    pub fn write(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
}
