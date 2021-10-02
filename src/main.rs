use rdb::{
    data::{Data, Type},
    engine::Engine,
    front::{print_table, yaml::parse_query_from_yaml},
    query::{ProcessItem, Query, QuerySource},
    schema,
    storage::Storage,
};

fn main() {
    let schema = schema::Schema {
        tables: vec![
            schema::Table {
                name: "user".to_string(),
                columns: vec![
                    schema::Column {
                        name: "id".to_string(),
                        dtype: Type::U64,
                    },
                    schema::Column {
                        name: "name".to_string(),
                        dtype: Type::String,
                    },
                    schema::Column {
                        name: "email".to_string(),
                        dtype: Type::String,
                    },
                ],
                primary_key: Some(0),
            },
            schema::Table {
                name: "message".to_string(),
                columns: vec![
                    schema::Column {
                        name: "id".to_string(),
                        dtype: Type::U64,
                    },
                    schema::Column {
                        name: "user_id".to_string(),
                        dtype: Type::U64,
                    },
                    schema::Column {
                        name: "text".to_string(),
                        dtype: Type::String,
                    },
                ],
                primary_key: Some(0),
            },
        ],
    };

    let mut s = rdb::storage::in_memory::InMemory::new();
    s.add_table(schema.tables[0].clone());
    s.add_table(schema.tables[1].clone());
    dbg!(s
        .add_row(
            "user",
            vec![
                Data::U64(1),
                Data::String("niko".to_string()),
                Data::String("niko@oneshot.game".to_string())
            ]
        )
        .unwrap());
    dbg!(s
        .add_row(
            "user",
            vec![
                Data::U64(2),
                Data::String("ralsei".to_string()),
                Data::String("ralsei@deltarune.game".to_string())
            ]
        )
        .unwrap());
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
    s.add_row(
        "message",
        vec![
            Data::U64(2),
            Data::U64(2),
            Data::String("hello!".to_string()),
        ],
    )
    .unwrap();

    // let mut c = s.get_const_cursor_range(s.source_index("user").unwrap(), 0, 100);
    // dbg!(s.get_from_cursor(&c));
    // s.advance_cursor(&mut c);
    // // dbg!(s.get_from_cursor(&c));

    let mut engine = Engine::new(schema, s);

    let query = Query {
        sub_queries: vec![],
        source: QuerySource {
            table_name: "user".to_string(),
            keys: vec!["id".to_string()],
            from: Some(vec![Data::U64(0)]),
            to: Some(vec![Data::U64(100)]),
        },
        process: vec![ProcessItem::Select {
            columns: vec![
                ("id".to_owned(), "id!".to_owned()),
                ("name".to_owned(), "name!".to_owned()),
            ],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_query(&query).unwrap();
    print_table(&cs, &vs);

    // let query = Query {
    //     sub_queries: vec![],
    //     source: QuerySource {
    //         table_name: "message".to_string(),
    //         iterate_over: "id".to_string(),
    //         from: 0,
    //         to: 100,
    //     },
    //     process: vec![ProcessItem::Join {
    //         table_name: "user".to_owned(),
    //         left_key: "user_id".to_owned(),
    //         right_key: "id".to_owned(),
    //     },ProcessItem::Select {
    //         columns: vec![
    //             ("id".to_owned(), "id".to_owned()),
    //             ("text".to_owned(), "text".to_owned()),
    //             ("user.name".to_owned(), "user_name".to_owned()),
    //         ],
    //     }],
    //     post_process: vec![],
    // };
    let query = parse_query_from_yaml(
        r"
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
        as: id
    -   name: text
        as: text
    -   name: 'user.name'
        as: user_name
",
    )
    .unwrap();
    let (cs, vs) = engine.execute_query(&query).unwrap();
    print_table(&cs, &vs);
}
