use rdb::{
    data::{Data, Type},
    engine::Engine,
    front::print_table,
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
            },
        ],
    };

    let mut s = rdb::in_memory::InMemory::new();
    s.add_table("user".to_string(), 3);
    s.add_table("message".to_string(), 3);
    dbg!(s
        .push_row(
            0,
            vec![
                Data::U64(1),
                Data::String("niko".to_string()),
                Data::String("niko@oneshot.game".to_string())
            ]
        )
        .unwrap());
    dbg!(s
        .push_row(
            0,
            vec![
                Data::U64(2),
                Data::String("ralsei".to_string()),
                Data::String("ralsei@deltarune.game".to_string())
            ]
        )
        .unwrap());
    s.push_row(
        1,
        vec![
            Data::U64(1),
            Data::U64(1),
            Data::String("hello".to_string()),
        ],
    )
    .unwrap();
    s.push_row(
        1,
        vec![
            Data::U64(2),
            Data::U64(2),
            Data::String("hello!".to_string()),
        ],
    )
    .unwrap();
    s.push_row(
        1,
        vec![
            Data::U64(3),
            Data::U64(1),
            Data::String("hello!!".to_string()),
        ],
    )
    .unwrap();

    let mut c = s.get_const_cursor_range(s.source_index("user").unwrap(), 0, 100);
    dbg!(s.get_from_cursor(&c));
    s.advance_cursor(&mut c);
    // dbg!(s.get_from_cursor(&c));

    let mut engine = Engine::new(schema, s);

    let query = Query {
        sub_queries: vec![],
        source: QuerySource {
            table_name: "user".to_string(),
            iterate_over: "id".to_string(),
            from: 0,
            to: 100,
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

    
    let query = Query {
        sub_queries: vec![],
        source: QuerySource {
            table_name: "message".to_string(),
            iterate_over: "id".to_string(),
            from: 0,
            to: 100,
        },
        process: vec![ProcessItem::Join {
            table_name: "user".to_owned(),
            left_key: "user_id".to_owned(),
            right_key: "id".to_owned(),
        },ProcessItem::Select {
            columns: vec![
                ("id".to_owned(), "id".to_owned()),
                ("text".to_owned(), "text".to_owned()),
                ("user.name".to_owned(), "user_name".to_owned()),
            ],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_query(&query).unwrap();
    print_table(&cs, &vs);
}
