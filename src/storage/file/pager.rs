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

#[derive(Debug)]
struct PageContainer<P: Page> {
    page: P,
    modified: bool,
}

pub struct Pager<P: Page> {
    file: Cursor<File>,
    file_len: u64,
    pages: Vec<Option<PageContainer<P>>>,
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

    pub fn ensure_page(&mut self, i: usize) {
        self.get_mut_inner(i);
    }

    pub fn get_ref(&self, i: usize) -> &P {
        &self.pages[i].as_ref().unwrap().page
    }

    pub fn get_mut(&mut self, i: usize) -> &mut P {
        let container = self.get_mut_inner(i);
        container.modified = true; //?
        &mut container.page
    }

    fn get_mut_inner(&mut self, i: usize) -> &mut PageContainer<P> {
        if self.pages.len() == i {
            self.pages.push(Some(PageContainer {
                page: P::from([0; PAGE_SIZE as usize]),
                modified: false,
            }));
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
                self.pages[i] = Some(PageContainer {
                    page: P::from(buf),
                    modified: false,
                });
            }
        }
        self.pages[i].as_mut().unwrap()
    }

    pub fn push(&mut self, page: P) -> usize {
        self.pages.push(Some(PageContainer {
            page,
            modified: true,
        }));
        self.pages.len() - 1
    }

    pub fn swap(&mut self, i: usize, page: P) -> P {
        self.pages[i]
            .replace(PageContainer {
                page,
                modified: true,
            })
            .unwrap()
            .page
    }

    pub fn save(&mut self) {
        for i in 0..self.pages.len() {
            self.flush(i);
        }
    }

    pub fn flush(&mut self, i: usize) {
        if let Some(container) = &mut self.pages[i] {
            if container.modified {
                self.file.set_position(i as u64 * PAGE_SIZE);
                std::io::Write::write(self.file.get_mut(), container.page.as_ref()).unwrap();

                container.modified = false;
            }
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
