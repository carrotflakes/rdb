use rand::{prelude::SliceRandom, Rng, SeedableRng};

use crate::{
    builtin_schema::new_auto_increment_table,
    data::Data,
    engine::Engine,
    front::{print_table, yaml::schema::parse_table_from_yaml},
    query::{
        Delete, Expr, FilterItem, Insert, ProcessItem, Select, SelectSource, SelectSourceTable,
        Stream,
    },
};

#[test]
fn monkey_test() {
    let mut rng: rand::rngs::SmallRng = SeedableRng::seed_from_u64(1);

    let table = parse_table_from_yaml(
        r"
name: user
columns:
-   name: id
    type: string
    #auto_increment: true
-   name: name
    type: string
-   name: email
    type: string
-   name: age
    type: u64
    default: '20'
primary_key: [id]
indices:
-   name: name
    columns: [name]
-   name: age
    columns: [age]
",
    )
    .unwrap();

    // let id_data = |i: u64| Data::U64(i);
    let id_data = |i: u64| Data::String(i.to_string());

    let filepath = "main.rdb";
    if let Ok(_) = std::fs::remove_file(filepath) {
        println!("{:?} removed", filepath);
    };
    let mut engine = Engine::from_storage(crate::storage::file::File::open(filepath));
    engine.create_table(new_auto_increment_table());
    engine.create_table(table);

    let insert_num = 200usize;

    let mut slice: Vec<_> = (0..insert_num).collect();
    slice.shuffle(&mut rng);
    for i in slice.clone() {
        engine
            .execute_insert(&Insert::Row {
                table_name: "user".to_owned(),
                column_names: vec![
                    "id".to_owned(),
                    "name".to_owned(),
                    "email".to_owned(),
                    "age".to_owned(),
                ],
                values: vec![
                    id_data(i as u64),
                    Data::String(format!("{}", rng.gen_range(1..100000))),
                    Data::String(format!("{}@example.com", rng.gen_range(1..100000))),
                    Data::U64(rng.gen_range(1..100)),
                ],
            })
            .unwrap();
    }

    // show all
    let query = Select {
        sub_queries: vec![],
        streams: vec![Stream {
            source: SelectSource::Table(SelectSourceTable {
                table_name: "user".to_string(),
                keys: vec!["id".to_string()],
                from: None,
                to: None,
            }),
            process: vec![],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);
    assert_eq!(vs.len() / cs.len(), insert_num);

    // show age >= 50
    let query = Select {
        sub_queries: vec![],
        streams: vec![Stream {
            source: SelectSource::Table(SelectSourceTable {
                table_name: "user".to_string(),
                keys: vec!["id".to_string()],
                from: None,
                to: None,
            }),
            process: vec![ProcessItem::Filter {
                items: vec![FilterItem::Ge(
                    Expr::Column("age".to_owned()),
                    Expr::Data(Data::U64(50)),
                )],
            }],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);

    // select each
    for i in 0..insert_num {
        let query = Select {
            sub_queries: vec![],
            streams: vec![Stream {
                source: SelectSource::Table(SelectSourceTable {
                    table_name: "user".to_string(),
                    keys: vec!["id".to_string()],
                    from: Some(vec![id_data(i as u64)]),
                    to: Some(vec![id_data(i as u64)]),
                }),
                process: vec![],
            }],
            post_process: vec![],
        };
        let (cs, vs) = engine.execute_select(&query).unwrap();
        // print_table(&cs, &vs);
        // dbg!(i);
        assert_eq!(vs.len() / cs.len(), 1);
    }

    // delete all
    slice.shuffle(&mut rng);
    for i in slice.clone() {
        dbg!(i);
        //if i==2 {break;}
        let query = Delete {
            source: SelectSource::Table(SelectSourceTable {
                table_name: "user".to_string(),
                keys: vec!["id".to_string()],
                from: Some(vec![id_data(i as u64)]),
                to: Some(vec![id_data(i as u64)]),
            }),
            filter: vec![],
        };
        let count = engine.execute_delete(&query).unwrap();
        assert_eq!(count, 1);
    }

    let query = Select {
        sub_queries: vec![],
        streams: vec![Stream {
            source: SelectSource::Table(SelectSourceTable {
                table_name: "user".to_string(),
                keys: vec!["id".to_string()],
                from: None,
                to: None,
            }),
            process: vec![],
        }],
        post_process: vec![],
    };
    let (cs, vs) = engine.execute_select(&query).unwrap();
    print_table(&cs, &vs);
    assert_eq!(vs.len() / cs.len(), 0);

    engine.storage().print_summary();
}
