pub mod yaml;

use crate::data::Data;

pub fn print_table(column_names: &[String], values: &[Data]) {
    let mut widths: Vec<usize> = column_names.iter().map(|n| n.len()).collect();

    for row in values.chunks(column_names.len()) {
        for i in 0..column_names.len() {
            widths[i] = widths[i].max(format!("{}", &row[i]).chars().count());
        }
    }

    fn show_with_pad(v: &impl std::fmt::Display, width: usize, left: bool) {
        let len = format!("{}", v).chars().count();
        if left {
            print!("{0}{1}", v, " ".repeat(width - len));
        } else {
            print!("{1}{0}", v, " ".repeat(width - len));
        }
    }

    for i in 0..column_names.len() {
        print!("|");
        show_with_pad(&column_names[i], widths[i], true);
    }
    println!("|");

    for row in values.chunks(column_names.len()) {
        for i in 0..column_names.len() {
            let left = match &row[i] {
                Data::U64(_) | Data::OptionU64(_) => false,
                Data::String(_) => true,
                Data::Lancer(_) => true,
            };
            print!("|");
            show_with_pad(&row[i], widths[i], left);
        }
        println!("|");
    }
}
