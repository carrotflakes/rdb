use std::{fs::File, io::Cursor};

pub const PAGE_SIZE: u64 = 4 * 1024;

pub type Page = [u8; PAGE_SIZE as usize];

pub struct Disk {
    file: Cursor<File>,
    pages_num: usize,
}

impl Disk {
    pub fn open(filepath: &str) -> Self {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filepath)
            .unwrap();
        let file_len = file.metadata().unwrap().len();
        let file = Cursor::new(file);
        let pages_num = (file_len / PAGE_SIZE) as usize;

        Self { file, pages_num }
    }

    pub fn pages_num(&self) -> usize {
        self.pages_num
    }

    pub fn next_page_id(&mut self) -> usize {
        let page_id = self.pages_num;
        self.pages_num += 1;
        page_id
    }

    pub fn get(&mut self, id: usize) -> Page {
        if self.pages_num <= id {
            self.pages_num = id + 1;
        }
        self.file.set_position(id as u64 * PAGE_SIZE);
        let mut page = [0; PAGE_SIZE as usize];
        std::io::Read::read(self.file.get_mut(), &mut page).unwrap();
        page
    }

    pub fn save(&mut self, id: usize, page: &Page) {
        self.file.set_position(id as u64 * PAGE_SIZE);
        std::io::Write::write(self.file.get_mut(), page).unwrap();
    }
}
