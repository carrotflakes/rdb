use serde::{Deserialize, Serialize};

use super::{
    page::Page,
    pager::{Pager, PAGE_SIZE},
};

#[derive(Debug, Serialize, Deserialize)]
struct ObjectTable {
    objects: Vec<(String, u32)>,
}

pub fn init_as_simple_store(pager: &mut Pager<Page>) {
    let ot = ObjectTable { objects: vec![] };
    let size_to_write = bincode::serialized_size(&ot).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(pager, pages_num_required, 0);
    bincode::serialize_into(PagerWriter::new(pager, 0), &ot).unwrap();
}

pub fn read_object<T: Serialize + serde::de::DeserializeOwned>(
    pager: &mut Pager<Page>,
    name: &str,
) -> Option<T> {
    ensure_pages_to_read(pager, 0);
    let reader = PagerReader::new(pager, 0);
    let ot: ObjectTable = bincode::deserialize_from(reader).unwrap();
    if let Some(o) = ot.objects.iter().find(|o| o.0.as_str() == name) {
        ensure_pages_to_read(pager, o.1 as usize);
        bincode::deserialize_from(PagerReader::new(pager, o.1 as usize)).ok()
    } else {
        None
    }
}

pub fn write_object<T: Serialize + serde::de::DeserializeOwned>(
    pager: &mut Pager<Page>,
    name: &str,
    object: &T,
) {
    ensure_pages_to_read(pager, 0);
    let reader = PagerReader::new(pager, 0);
    let mut ot: ObjectTable = bincode::deserialize_from(reader).unwrap();

    let page_i = if let Some(o) = ot.objects.iter().find(|o| o.0.as_str() == name) {
        o.1 as usize
    } else {
        let i = pager.size();
        ot.objects.push((name.to_string(), i as u32));
        i
    };

    let size_to_write = bincode::serialized_size(object).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(pager, pages_num_required, page_i);
    bincode::serialize_into(PagerWriter::new(pager, page_i), object).unwrap();

    let size_to_write = bincode::serialized_size(&ot).unwrap();
    let pages_num_required = (size_to_write / (PAGE_SIZE - 4)) as usize;
    ensure_pages_to_write(pager, pages_num_required, page_i);
    bincode::serialize_into(PagerWriter::new(pager, 0), &ot).unwrap();
}

fn ensure_pages_to_read(pager: &mut Pager<Page>, page_i: usize) {
    pager.ensure_page(page_i);
    let next_page_i = read_next_page_i(pager.get_ref(page_i));
    if next_page_i != 0 {
        ensure_pages_to_read(pager, next_page_i);
    };
}

fn ensure_pages_to_write(pager: &mut Pager<Page>, pages_num: usize, page_i: usize) {
    pager.ensure_page(page_i);
    if pages_num == 0 {
        return;
    }
    let next_page_i = read_next_page_i(pager.get_mut(page_i));
    let next_page_i = if next_page_i == 0 {
        let next_page_i = pager.size();
        write_next_page_i(pager.get_mut(page_i), next_page_i);
        next_page_i
    } else {
        next_page_i as usize
    };
    ensure_pages_to_write(pager, pages_num - 1, next_page_i);
}

pub struct PagerWriter<'a> {
    pager: &'a mut Pager<Page>,
    page_i: Option<usize>,
    i: usize,
    next_page_i: Option<usize>,
}

impl<'a> PagerWriter<'a> {
    pub fn new(pager: &'a mut Pager<Page>, page_i: usize) -> Self {
        Self {
            pager,
            page_i: Some(page_i),
            i: 0,
            next_page_i: None,
        }
    }
}

impl<'a> std::io::Write for PagerWriter<'a> {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let mut total_write_len = 0;
        while !buf.is_empty() && self.page_i.is_some() {
            let page = self.pager.get_mut(self.page_i.unwrap());
            if self.i == 0 {
                self.next_page_i = Some(read_next_page_i(page));
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

pub struct PagerReader<'a> {
    pager: &'a Pager<Page>,
    page_i: Option<usize>,
    i: usize,
    next_page_i: Option<usize>,
}

impl<'a> PagerReader<'a> {
    pub fn new(pager: &'a Pager<Page>, page_i: usize) -> Self {
        Self {
            pager,
            page_i: Some(page_i),
            i: 0,
            next_page_i: None,
        }
    }
}

impl<'a> std::io::Read for PagerReader<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_read_len = 0;
        while !buf.is_empty() && self.page_i.is_some() {
            let page = self.pager.get_ref(self.page_i.unwrap());
            if self.i == 0 {
                self.next_page_i = Some(read_next_page_i(page));
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

fn read_next_page_i(page: &Page) -> usize {
    use std::convert::TryInto;
    u32::from_le_bytes(page.slice(0, 4).try_into().unwrap()) as usize
}

fn write_next_page_i(page: &mut Page, next_page_i: usize) {
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
        let mut pager = Pager::<Page>::open(filepath);
        init_as_simple_store(&mut pager);
        dbg!(read_object::<String>(&mut pager, "hello"));
        write_object::<String>(&mut pager, "hello", &"hello!!!".to_owned());
        write_object::<String>(&mut pager, "too learge", &"too learge...".repeat(100));
        write_object::<String>(&mut pager, "bye", &"bye!!!".to_owned());
        dbg!(read_object::<String>(&mut pager, "hello"));
        dbg!(read_object::<String>(&mut pager, "too learge").map(|x| x.len()));
        dbg!(read_object::<String>(&mut pager, "bye"));
        dbg!(pager.size());
        pager.save();
    }
    {
        let mut pager = Pager::<Page>::open(filepath);
        dbg!(read_object::<String>(&mut pager, "hello"));
        dbg!(read_object::<String>(&mut pager, "too learge").map(|x| x.len()));
        dbg!(read_object::<String>(&mut pager, "bye"));
        dbg!(pager.size());
    }
}
