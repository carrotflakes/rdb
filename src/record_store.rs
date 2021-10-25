// # fixed size
// [4] next page id
//   [1] state
//   [?] record
//   ...
// [1] state (END_OF_PAGE)
//
// # variable size
// [4] next page id
//   [1] state
//   [2] size
//   [?] record
//   ...
// [1] state (END_OF_PAGE)
//
// state:
//   0: empty
//   1: used
//   2: deleted
//   4: end of page

use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use crate::{disk::PAGE_SIZE, disk_cache::DiskCache};

const EMPTY: u8 = 0;
const USED: u8 = 1;
const DELETED: u8 = 2;
const END_OF_PAGE: u8 = 3;

#[derive(Serialize, Deserialize)]
pub struct RecordStore {
    first_page_id: usize,
    last_page_id: usize,
    next_index: usize,
    record_size: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct Cursor {
    page_id: usize,
    index: usize,
}

impl RecordStore {
    pub fn new(first_page_id: usize, record_size: Option<usize>) -> Self {
        Self {
            first_page_id,
            last_page_id: first_page_id,
            next_index: 4,
            record_size,
        }
    }

    pub fn push_record(&mut self, disk_cache: &mut DiskCache, record: &[u8]) -> Cursor {
        // check if insertable
        let insert_data_size = if let Some(record_size) = self.record_size {
            1 + record_size
        } else {
            1 + 2 + record.len()
        };
        let remain_data_size = PAGE_SIZE as usize - self.next_index - 1;
        if insert_data_size > remain_data_size {
            // next page
            let next_page_id = disk_cache.next_page_id();

            // write next_page_id
            let page_ref = disk_cache.get(self.last_page_id);
            let mut page = page_ref.write();
            page[0..4].copy_from_slice(&(next_page_id as u32).to_le_bytes());
            page[self.next_index] = END_OF_PAGE;

            self.last_page_id = next_page_id;
            self.next_index = 4;
        }

        let page_ref = disk_cache.get(self.last_page_id);
        let mut page = page_ref.write();

        let cursor = Cursor {
            page_id: self.last_page_id,
            index: self.next_index,
        };

        page[cursor.index] = USED;
        if let Some(record_size) = self.record_size {
            page[cursor.index + 1..cursor.index + 1 + record_size].copy_from_slice(record);
        } else {
            let record_size = record.len();
            page[cursor.index + 1..cursor.index + 1 + 2]
                .copy_from_slice(&(record_size as u16).to_le_bytes());
            page[cursor.index + 1 + 2..cursor.index + 1 + 2 + record_size].copy_from_slice(record);
        }
        self.next_index += insert_data_size;
        cursor
    }

    pub fn first_cursor(&self) -> Cursor {
        Cursor {
            page_id: self.first_page_id,
            index: 4,
        }
    }

    pub fn cursor_get(&self, disk_cache: &DiskCache, cursor: &Cursor) -> Option<Vec<u8>> {
        let page_ref = disk_cache.get(cursor.page_id);
        let page = page_ref.read();

        if page[cursor.index] != USED {
            return None;
        }

        Some(if let Some(record_size) = self.record_size {
            page[cursor.index + 1..cursor.index + 1 + record_size].to_vec()
        } else {
            let record_size = parse_u16(&page[cursor.index + 1..cursor.index + 1 + 2]) as usize;
            page[cursor.index + 1 + 2..cursor.index + 1 + 2 + record_size].to_vec()
        })
    }

    pub fn cursor_update(&self, disk_cache: &mut DiskCache, cursor: &Cursor, record: &[u8]) {
        let record_size = self.record_size.expect("cursor_update is currently only supported on fixed size records");
        
        let page_ref = disk_cache.get(cursor.page_id);
        let mut page = page_ref.write();

        page[cursor.index + 1..cursor.index + 1 + record_size].copy_from_slice(record);
    }

    pub fn cursor_delete(&self, disk_cache: &mut DiskCache, cursor: &Cursor) {
        let page_ref = disk_cache.get(cursor.page_id);
        let mut page = page_ref.write();

        page[cursor.index] = DELETED;
    }

    pub fn cursor_next(&self, disk_cache: &DiskCache, cursor: &Cursor) -> Cursor {
        let page_ref = disk_cache.get(cursor.page_id);
        let page = page_ref.read();

        let index = if let Some(record_size) = self.record_size {
            cursor.index + 1 + record_size
        } else {
            let record_size = parse_u16(&page[cursor.index + 1..cursor.index + 1 + 2]) as usize;
            cursor.index + 1 + 2 + record_size
        };

        if page[index] == END_OF_PAGE {
            let page_id = parse_u32(&page[0..4]) as usize;
            Cursor { page_id, index: 4 }
        } else {
            Cursor {
                page_id: cursor.page_id,
                index,
            }
        }
    }

    pub fn cursor_is_end(&self, disk_cache: &DiskCache, cursor: &Cursor) -> bool {
        let page_ref = disk_cache.get(cursor.page_id);
        let page = page_ref.read();

        page[cursor.index] == EMPTY
    }
}

impl From<u32> for Cursor {
    fn from(v: u32) -> Self {
        Cursor {
            page_id: (v / PAGE_SIZE as u32) as usize,
            index: (v % PAGE_SIZE as u32) as usize,
        }
    }
}

impl From<Cursor> for u32 {
    fn from(cursor: Cursor) -> Self {
        cursor.page_id as u32 * PAGE_SIZE as u32 + cursor.index as u32
    }
}

fn parse_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

fn parse_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

#[test]
fn test() {
    let filepath = "main.rdb";
    if let Ok(_) = std::fs::remove_file(filepath) {
        println!("{:?} removed", filepath);
    };
    let mut disk = crate::disk::Disk::open(filepath);
    let page_id1 = disk.next_page_id();
    let page_id2 = disk.next_page_id();
    let mut disk_cache =
        crate::disk_cache::DiskCache::new(std::sync::Arc::new(std::sync::Mutex::new(disk)));

    let mut rs1 = RecordStore::new(page_id1, Some(4));

    for i in 0..1000 {
        rs1.push_record(&mut disk_cache, &[11, 22, 33, (i % 256) as u8]);
    }
    let mut cursor = rs1.first_cursor();
    assert_eq!(
        rs1.cursor_get(&mut disk_cache, &cursor),
        Some(vec![11, 22, 33, 0])
    );

    for _ in 0..900 {
        cursor = rs1.cursor_next(&mut disk_cache, &cursor);
    }
    assert_eq!(
        rs1.cursor_get(&mut disk_cache, &cursor),
        Some(vec![11, 22, 33, 132])
    );
    assert_eq!(cursor.page_id, 2);
    assert_eq!(cursor.index, 414);

    let mut rs2 = RecordStore::new(page_id2, None);

    for i in 0..500 {
        rs2.push_record(&mut disk_cache, &[44, 55, 66, (i % 256) as u8]);
        rs2.push_record(&mut disk_cache, &[11, 22, 33, 44, 55, 66, (i % 256) as u8]);
    }
    let mut cursor = rs2.first_cursor();
    assert_eq!(
        rs2.cursor_get(&mut disk_cache, &cursor),
        Some(vec![44, 55, 66, 0])
    );

    for _ in 0..900 {
        cursor = rs2.cursor_next(&mut disk_cache, &cursor);
    }
    assert_eq!(
        rs2.cursor_get(&mut disk_cache, &cursor),
        Some(vec![44, 55, 66, 194])
    );
    assert_eq!(cursor.page_id, 3);
    assert_eq!(cursor.index, 3567);
}
