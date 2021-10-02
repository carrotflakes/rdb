use crate::data::Data;

pub mod query;
pub mod schema;

pub fn string_to_data(str: String) -> Data {
    if let Ok(v) = str.parse() {
        Data::U64(v)
    } else {
        Data::String(str.clone())
    }
}
