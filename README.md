# Carrot flavored relational database

## TODO

### basic

- [ ] operations
  - [x] select
  - [x] insert
  - [x] delete
  - [ ] update
  - [ ] create table
  - [ ] drop table
  - [ ] alter table
    - [ ] add column
    - [ ] delete column
    - [ ] change column name
    - [ ] change column type
    - [ ] change column allow null
  - [ ] create index
  - [ ] add constraint
  - [ ] ...
- [ ] BTree
- [ ] pager
- [ ] null
- [ ] sub query
- [ ] constraints
  - [ ] unique constraint
  - [ ] foreign key constraint
    - [ ] RESTRICT, CASCADE, SET NULL, NO ACTION
  - [ ] check constraint
- [ ] auto increment
- [ ] index
- [ ] transaction
- [ ] explain
- [ ] tests
  - [ ] unit tests
  - [ ] monkey test
- [ ] benchmark
- [ ] dump

### extra

- [ ] tcp connection
- [ ] logging
- [ ] large data
- [ ] multi thread
- [ ] user management
- [ ] CLI
- [ ] SQL compatible interface
- [ ] VM

## Notes

``` yaml
schema:
  tables:
    user:
      column:
      - name: id
        type: u64
      - name: name
        type: string
      - name: email
        type: string
      primaryKey: id
      constraints:
        uniqueId:
          type: unique
          keys
          - id
      indices:
      - name: id
        keys
        - id  

select:
  name: getUser
  subTables
    - ...
  source:
    table: user
    keys:
    - id
    just:
    - 1
  process:
    - select:
      - id
      - name
      - email
    - filter:
      eq:
      - !column email
      - !string "hoge@a.com"
    - join:
      table: profile
      leftKey: id
      rightKey: userId
    - distinct: email
    - addComputeColumn: count
    - skip: 10
    - limit: 10
  postProcess:
    - sortBy: id
    - skip: 10
    - limit: 10

insert:
  table: message
  select: ...
```

```
Transaction
     ↓
Local storage
     ↓
Lock
     ↓
Remote storage
```

Select
Insert
  Select
Update
  Select
Delete
  Select

## tables

- id
- name

## columns

- id
- table_id
- name

## indices

- id 
- table_name

## index_keys

- id
- iterate_id
- column_id
