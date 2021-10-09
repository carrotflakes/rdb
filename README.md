# Carrot flavored relational database

## TODO

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
- [ ] BTree
- [ ] pager
- [ ] null
- [ ] constraints
  - [ ] unique constraint
  - [ ] foreign key constraint
  - [ ] check constraint
- [ ] auto increment
- [ ] explain
- [ ] jarnal log
- [ ] index
- [ ] large data
- [ ] multi thread
- [ ] tests
  - [ ] unit tests
  - [ ] integration tests
  - [ ] monky test
- [ ] benchmark
- [ ] tcp connection
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
