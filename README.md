# Carrot flavored relational database

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

query
  name: getUser
  subTables
    - ...
  source:
    table: user
    iterate:
      over: id
      from: 0
      to: 100
  process:
    - select:
      - id
      - name
      - email
    - filter: email == "hoge@a.com"
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
```
