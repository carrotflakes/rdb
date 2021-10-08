# Carrot flavored relational database

## TODO
- [ ] null
- [ ] delete
- [ ] update
- [ ] unique constraint
- [ ] foreign key constraint
- [ ] check constraint
- [ ] drop table
- [ ] alter table
- [ ] auto increment
- [ ] explain
- [ ] jarnal log
- [ ] index
- [ ] large data
- [ ] unit tests
- [ ] integration tests
- [ ] benchmark
- [ ] tcp connection
- [ ] CLI
- [ ] SQL
- [ ] VM

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
