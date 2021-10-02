use std::{fs::File, io::Cursor};

pub const PAGE_SIZE: u64 = 4 * 1024;

pub type PageRaw = [u8; PAGE_SIZE as usize];

pub trait Page:
    From<PageRaw> + std::ops::Deref<Target = PageRaw> + std::ops::DerefMut<Target = PageRaw>
{
}

impl<P> Page for P where
    P: From<PageRaw> + std::ops::Deref<Target = PageRaw> + std::ops::DerefMut<Target = PageRaw>
{
}

pub struct Pager<P: Page> {
    file: Cursor<File>,
    file_len: u64,
    pages: Vec<Option<P>>,
}

impl<P: Page> Pager<P> {
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

        Self {
            file,
            file_len,
            pages: (0..pages_num).map(|_| None).collect(),
        }
    }

    pub fn size(&self) -> usize {
        self.pages.len()
    }

    pub fn get_ref(&mut self, i: usize) -> &P {
        self.get_mut(i)
    }

    pub fn get_mut(&mut self, i: usize) -> &mut P {
        if self.pages.len() == i {
            self.pages.push(Some(P::from([0; PAGE_SIZE as usize])));
        }
        if self.pages[i].is_none() {
            let mut pages_num = (self.file_len / PAGE_SIZE) as usize;
            if self.file_len % PAGE_SIZE != 0 {
                pages_num += 1;
            }

            if i <= pages_num {
                self.file.set_position(i as u64 * PAGE_SIZE);
                let mut buf = [0; PAGE_SIZE as usize];
                std::io::Read::read(self.file.get_mut(), &mut buf).unwrap();
                self.pages[i] = Some(P::from(buf));
            }
        }
        self.pages[i].as_mut().unwrap()
    }

    pub fn push(&mut self, page: P) -> usize {
        self.pages.push(Some(page));
        self.pages.len() - 1
    }

    pub fn swap(&mut self, i: usize, page: P) -> P {
        self.pages[i].replace(page).unwrap()
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
//     let mut pager = Pager::<PageRaw>::open("hello");
//     let page = pager.get_mut(0);
//     dbg!(&page.as_ref()[0..10]);
//     page.as_mut()[0] = 1;
//     page.as_mut()[1] = 2;
//     page.as_mut()[2] = 3;
//     pager.save();
// }
