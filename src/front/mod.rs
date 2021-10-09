pub mod yaml;

use crate::data::Data;

pub fn print_table(column_names: &[String], values: &[Data]) {
    for c in column_names {
        print!("{} ", c);
    }
    for (i, v) in values.iter().enumerate() {
        if i % column_names.len() == 0 {
            println!();
        }
        print!("{} ", v);
    }
    println!();
}
