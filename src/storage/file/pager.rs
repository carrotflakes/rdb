pub const PAGE_SIZE: u64 = 4 * 1024;

pub type Page = [u8; PAGE_SIZE as usize];

pub struct Pager {
    pub(crate) file: std::io::Cursor<std::fs::File>,
    pub(crate) file_len: u64,
    pub(crate) pages: Vec<Option<Page>>,
    pub(crate) pages_num: usize,
}

impl Pager {
    pub fn open(filepath: &str) -> Self {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filepath)
            .unwrap();
        let file_len = file.metadata().unwrap().len();
        let file = std::io::Cursor::new(file);
        let pages_num = (file_len / PAGE_SIZE) as usize;

        Self {
            file,
            file_len,
            pages: vec![None; pages_num],
            pages_num,
        }
    }

    pub fn get_ref(&mut self, i: usize) -> &Page {
        if self.pages[i].is_none() {
            let mut pages_num = (self.file_len / PAGE_SIZE) as usize;
            if self.file_len % PAGE_SIZE != 0 {
                pages_num += 1;
            }

            if i <= pages_num {
                self.file.set_position(i as u64 * PAGE_SIZE);
                let mut buf = [0; PAGE_SIZE as usize];
                std::io::Read::read(self.file.get_mut(), &mut buf).unwrap();
                self.pages[i] = Some(buf);
            }
            if i >= self.pages_num {
                self.pages_num = i + 1;
            }
        }
        self.pages[i].as_ref().unwrap()
    }

    pub fn get_mut(&mut self, i: usize) -> &mut Page {
        if self.pages[i].is_none() {
            let mut pages_num = (self.file_len / PAGE_SIZE) as usize;
            if self.file_len % PAGE_SIZE != 0 {
                pages_num += 1;
            }

            if i <= pages_num {
                self.file.set_position(i as u64 * PAGE_SIZE);
                let mut buf = [0; PAGE_SIZE as usize];
                std::io::Read::read(self.file.get_mut(), &mut buf).unwrap();
                self.pages[i] = Some(buf);
            }
            if i >= self.pages_num {
                self.pages_num = i + 1;
            }
        }
        self.pages[i].as_mut().unwrap()
    }

    pub fn save(&mut self) {
        for i in 0..self.pages.len() {
            self.flush(i);
        }
    }

    pub fn flush(&mut self, i: usize) {
        if let Some(data) = &self.pages[i] {
            self.file.set_position(i as u64 * PAGE_SIZE);
            std::io::Write::write(self.file.get_mut(), data.as_ref()).unwrap();
        }
    }
}

// #[test]
// fn test() {
//     let mut pager = Pager::open("hello");
//     let page = pager.get(0);
//     dbg!(&page.borrow().as_ref()[0..10]);
//     page.borrow_mut()[0] = 1;
//     page.borrow_mut()[1] = 2;
//     page.borrow_mut()[2] = 3;
//     pager.save();
// }
