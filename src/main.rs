use rdb::{
    data::{Data, Type},
    engine::Engine,
    front::{
        print_table,
        yaml::{
            query::{parse_insert_from_yaml, parse_select_from_yaml},
            schema::parse_table_from_yaml,
        },
    },
    query::{Expr, Insert, ProcessItem, Select, SelectSource},
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
                        default: None,
                    },
                    schema::Column {
                        name: "name".to_string(),
                        dtype: Type::String,
                        default: None,
                    },
                    schema::Column {
                        name: "email".to_string(),
                        dtype: Type::String,
                        default: None,
                    },
                ],
                primary_key: Some(0),
                constraints: Vec::new(),
            },
            parse_table_from_yaml(
                r"
name: message
columns:
-   name: id
    type: u64
    auto_increment: true
-   name: user_id
    type: u64
-   name: text
    type: string
primary_key: id
            ",
            )
            .unwrap(),
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

    let mut engine = Engine::from_storage(s);

    let query = Select {
        sub_queries: vec![],
        source: SelectSource {
            table_name: "user".to_string(),
            keys: vec!["id".to_string()],
            from: Some(vec![Data::U64(0)]),
            to: Some(vec![Data::U64(100)]),
        },
        process: vec![ProcessItem::Select {
            columns: vec![
                ("id!".to_owned(), Expr::Column("id".to_owned())),
                ("name!".to_owned(), Expr::Column("name".to_owned())),
            ],
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
    let query = parse_select_from_yaml(
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
        from: id
    -   name: text
        from: text
    -   name: user_name
        from: 'user.name'
",
    )
    .unwrap();
    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);

    engine
        .execute_insert(
            &parse_insert_from_yaml(
                r"
table: user
row:
    id: 3
    name: echo
    email: echo
",
            )
            .unwrap(),
        )
        .unwrap();

    engine
        .execute_insert(
            &parse_insert_from_yaml(
                r"
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
            value: 3
",
            )
            .unwrap(),
        )
        .unwrap();

    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);
}
