use crate::btree::{BTree, BTreeNode};

use super::{impl_btree::Meta, File};

impl File {
    pub fn print_summary(&self) {
        println!("{} pages", self.pager.size());
        println!("{} tables", self.schema.tables.len());
        for table in &self.schema.tables {
            println!("- {}", table.name,);
            for column in &table.columns {
                println!("  - {}: {:?}", column.name, column.dtype);
            }
        }
        println!("{} sources", self.sources.len());
        for source in &self.sources {
            println!("table: {}", &self.schema.tables[source.table_index].name);
            println!("key: {}", &source.key_columns.join(", "));
            println!(
                "value: {}",
                &source
                    .value_types
                    .iter()
                    .map(|t| format!("{:?}", t))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            self.print_page(&source.meta, source.page_index, 0);
        }
    }

    fn print_page(&self, meta: &Meta, node_i: usize, indent: usize) {
        let ind = "  ".repeat(indent);
        let node = self.pager.node_ref(node_i);
        if node.is_leaf(meta) {
            let next = node.get_next(meta);
            println!("{}- leaf(size: {}, page: {}, next: {:?})", ind, node.size(meta), node_i, next);
        } else {
            println!(
                "{}- internal(size: {}, page: {})",
                ind,
                node.size(meta),
                node_i
            );
            for node_i in node.get_children(meta) {
                self.print_page(meta, node_i, indent + 1);
            }
        }
    }
}
