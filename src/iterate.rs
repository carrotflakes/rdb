pub trait Iterate {
    type IterateIndex;
    type Item;
    type Cursor: std::fmt::Debug + Clone;

    fn first_cursor(&self, iterate_index: Self::IterateIndex) -> Self::Cursor;
    fn find(&self, iterate_index: Self::IterateIndex, item: &Self::Item) -> Self::Cursor;

    fn cursor_get(&self, cursor: &Self::Cursor) -> Option<Self::Item>;
    fn cursor_next(&self, cursor: &mut Self::Cursor) -> bool;
    fn cursor_next_occupied(&self, cursor: &Self::Cursor) -> Self::Cursor;
    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool;

    fn cursor_delete(&mut self, cursor: &Self::Cursor) -> bool;
    fn cursor_update(&mut self, cursor: &Self::Cursor, item: Self::Item) -> bool;

    fn add(&mut self, iterate_index: Self::IterateIndex, item: Self::Item) -> bool;
}

// pub trait IterateFind: Iterate {
//     fn find(&self, iterate_index: Self::IterateIndex) -> Self::Cursor;
// }

// pub trait IterateAdd: Iterate {
//     fn add(&mut self, item: Self::Item) -> bool;
// }

// pub trait IterateCursorUpdate: Iterate {
//     fn cursor_delete(&mut self, cursor: &Self::Cursor) -> bool;
//     fn cursor_update(&mut self, cursor: &Self::Cursor, item: Self::Item) -> bool;
// }

// pub trait IterateCursorNextOccupied: Iterate {
//     fn cursor_next_occupied(&self, cursor: &Self::Cursor) -> Self::Cursor;
// }
