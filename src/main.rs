use std::collections::HashMap;

use rdb::{
    data::Data,
    engine::Engine,
    front::{
        print_table,
        yaml::{
            query::{
                parse_insert_from_yaml, parse_named_queries_from_yaml, parse_update_from_yaml,
            },
            schema::parse_schema_from_yaml,
        },
    },
    query::{Expr, Insert, ProcessItem, Select, SelectSource, SelectSourceTable, Stream},
    storage::Storage,
};

fn main() {
    let schema = parse_schema_from_yaml(
        r"
tables:
-   name: user
    columns:
    -   name: id
        type: u64
        auto_increment: true
    -   name: name
        type: string
    -   name: email
        type: string
    primary_key: id
-   name: message
    columns:
    -   name: id
        type: u64
        auto_increment: true
    -   name: user_id
        type: u64
    -   name: text
        type: string
    primary_key: id
    indices:
    -   name:
        columns: [user_id]
    ",
    )
    .unwrap();

    // let mut s = rdb::storage::in_memory::InMemory::new();
    let filepath = "main.rdb";
    if let Ok(_) = std::fs::remove_file(filepath) {
        println!("{:?} removed", filepath);
    };
    let mut s = rdb::storage::file::File::open(filepath);
    s.add_table(schema.tables[0].clone());
    s.add_table(schema.tables[1].clone());
    s.add_row(
        "user",
        vec![
            Data::U64(1),
            Data::String("niko".to_string()),
            Data::String("niko@oneshot.game".to_string()),
        ],
    )
    .unwrap();
    s.add_row(
        "user",
        vec![
            Data::U64(2),
            Data::String("ralsei".to_string()),
            Data::String("ralsei@deltarune.game".to_string()),
        ],
    )
    .unwrap();
    s.add_row(
        "message",
        vec![
            Data::U64(1),
            Data::U64(1),
            Data::String("hello".to_string()),
        ],
    )
    .unwrap();
    s.add_row(
        "message",
        vec![
            Data::U64(3),
            Data::U64(1),
            Data::String("hello!!".to_string()),
        ],
    )
    .unwrap();

    let mut engine = Engine::from_storage(s);

    let query = Select {
        sub_queries: vec![],
        streams: vec![Stream {
            source: SelectSource::Table(SelectSourceTable {
                table_name: "user".to_string(),
                keys: vec!["id".to_string()],
                from: Some(vec![Data::U64(0)]),
                to: Some(vec![Data::U64(100)]),
            }),
            process: vec![ProcessItem::Select {
                columns: vec![
                    ("id!".to_owned(), Expr::Column("id".to_owned())),
                    ("name!".to_owned(), Expr::Column("name".to_owned())),
                ],
            }],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);

    engine
        .execute_insert(&Insert::Row {
            table_name: "message".to_owned(),
            column_names: vec!["id".to_owned(), "user_id".to_owned(), "text".to_owned()],
            values: vec![
                Data::U64(4),
                Data::U64(1),
                Data::String("I'm not a cat!".to_owned()),
            ],
        })
        .unwrap();

    engine
        .execute_insert(
            &parse_insert_from_yaml(
                r"
table: message
row:
    id: 2
    user_id: 2
    text: hello!
",
            )
            .unwrap(),
        )
        .unwrap();

    let queries = parse_named_queries_from_yaml(
        r"
name: select_messages
select:
    source:
        table: message
        iterate:
            over:
            -   id
    process:
    - join:
        table: user
        left_key: user_id
        right_key: id
    - select:
        -   name: id
            from: id
        -   name: text
            from: text
        -   name: user_name
            from: 'user.name'
---
name: insert_user
insert:
    table: user
    row:
        id: '3'
        name: echo
        email: echo
---
name: insert_messages_from_select
insert:
    table: message
    select:
        source:
            table: message
            iterate:
                over:
                -   id
        process:
        -   select:
            -   name: text
            -   name: user_id
                value: '3'
---
name: select_with_filter
select:
    source:
        table: message
        iterate:
            over:
            -   id
    process:
    -   filter:
            eq:
            -   column: text
            -   string: hello
---
name: select_etc
select:
    source:
        table: message
        iterate:
            over:
            -   id
    process:
    -   distinct: user_id
    -   add_column:
            name: hoge
            expr:
                string: hey
---
name: select_skip_limit
select:
    source:
        table: message
        iterate:
            over:
            -   id
    process:
    -   skip: 2
    -   limit: 3
---
name: select_index
select:
    source:
        table: message
        iterate:
            over: [user_id]
            just: ['1']
---
name: update
update:
    table: message
    iterate:
        over: [id]
        from: ['5']
    columns:
        text:
            string: I 💗 pancake
---
name: delete1
delete:
    source:
        table: message
        iterate:
            over:
            -   id
            from:
            -   '5'
---
name: etc
select:
    source:
        iota:
            column: i
            from: 10
            to: 15
    process:
    -   select:
        -   name: a
            from: i
        -   name: b
            from: i
    -   add_column:
            name: c
            expr:
                enumerate: 1
",
    )
    .unwrap();
    let queries = queries.into_iter().collect::<HashMap<_, _>>();

    for q in [
        "select_messages",
        "insert_user",
        "insert_messages_from_select",
        "select_messages",
        "select_with_filter",
        "select_etc",
        "select_skip_limit",
        "select_index",
        "update",
        "select_messages",
        "delete1",
        "select_messages",
        "etc",
    ] {
        println!("[{}]", q);
        let (cs, vs) = engine.execute_query(&queries[q]).unwrap();
        if !cs.is_empty() {
            print_table(&cs, &vs);
        }
    }

    parse_update_from_yaml(
        r"
table: hoge
iterate:
    over: [id]
columns:
    hoge:
        u64: 1
    ",
    )
    .unwrap();

    engine.flush();
}
