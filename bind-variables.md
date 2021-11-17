# HOW TO: SQL Bind Variables

The following is discussion about properly constructing SQL queries using the
psycopg2 Python library (https://www.psycopg.org/docs/).

In general DO NOT pre-compose SQL queries before passing them to
`cursor.execute()`. Instead, use placeholders in various ways. One general
approach is to use placeholders and another is SQL string composition.

## Placeholders

Relavant docs: https://www.psycopg.org/docs/usage.html

Different placeholder approaches:
- %s: `%s`
- brackets: `{} xxx {}` (only used in SQL string composition, see below)
- numbered: `{0} xxx {1}` (only used in SQL string composition, see below)
- named: `cursor.mogrify('SELECT * from table where id = %(some_id)s', 
{'some_id': 1234})` (only used in SQL string composition, see below)

### %s

`%s` can be used in a few different contexts, examples:

```python
command = "SELECT * FROM table WHERE some_id=%s;"
cursor.execute(command, ('package-345678',))

command = "INSERT INTO table VALUES (%s, %s);"
cursor.execute(command, (4, 5,))
```

You don't need to use different letters (e.g, `%d`), always use `%s`.



## SQL string composition

Docs: https://www.psycopg.org/docs/sql.html


### Brackets

`{}` are auto-numbered templates. You can also use numbered templates
(e.g., `{0}`, `{1}`).

Brackets w/o numbers use the inputs in order they are supplied `format()`
of `sql.SQL`

```python
from psycopg2 import sql
query = sql.SQL("SELECT {} FROM {}").format(
	sql.SQL(', ').join([sql.Identifier('foo'), sql.Identifier('bar')]),
	sql.Identifier('table'))
print(query.as_string(conn))
# select "foo", "bar" from "table"
```

Bracketed numbers can be used to set specific replacement locations matching
the inputs to `format()` of `sql.SQL`

```python
query = sql.SQL("SELECT {0} FROM {1}").format(
	sql.SQL(', ').join([sql.Identifier('foo'), sql.Identifier('bar')]),
	sql.Identifier('table'))
print(query.as_string(conn))
# select "foo", "bar" from "table"

query = sql.SQL("SELECT {1} FROM {0}").format(
	sql.Identifier('table'),
	sql.SQL(', ').join([sql.Identifier('foo'), sql.Identifier('bar')]))
print(query.as_string(conn))
# select "foo", "bar" from "table"
```

You can also use named placeholders with the following pattern: `%(name)s`.
It requires passing a dict with names of course. For example:

```python
cursor.mogrify("SELECT %(foo)s, %(bar)s FROM table", {'foo':5,'bar':7})
```

You can use a combination of brackets and `%s`, where the brackets are
filled in when you construct the sql object, and the `%s` is filled in when 
you run `cursor.execute` (or `cursor.mogrify`). For example:

```python
query = sql.SQL("select {field} from {table} where {pkey} = %s").format(
    field=sql.Identifier('my_name'),
    table=sql.Identifier('some_table'),
    pkey=sql.Identifier('id'))
cursor.mogrify(query, (42,))
```


## How to inspect SQL query without executing it

One option is `mogrify` https://www.psycopg.org/docs/cursor.html#cursor.mogrify

Example:

```python
print(cursor.mogrify("INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar',)))
```

Another option is `as_string` 
https://www.psycopg.org/docs/sql.html?psycopg2.sql.Composable.as_string#psycopg2.sql.Composable.as_string,
but only applies to Composed/Composable objects

Example:

```python
query = sql.SQL("SELECT {} FROM {}").format(
	sql.SQL(', ').join([sql.Identifier('foo'), sql.Identifier('bar')]),
	sql.Identifier('table'))
query.as_string(conn)
```



## Don't worry about data types

psycopg2 will handle the types for you. For example, don't put quotes around
placeholders:

```python
# WRONG
command = "SELECT * FROM table WHERE some_id='%s';"
cursor.execute(command, ('package-345678',))
# CORRECT
command = "SELECT * FROM table WHERE some_id=%s;"
cursor.execute(command, ('package-345678',))
```

## Insert many

An approach that may make sense especially if there's a lot of data to insert is
to use `execute_values`

Use of `execute_values` in jump-api:

- `journalsdb.py`: `recompute_journal_metadata`
- `consortium.py`: `Consortium.recompute_journal_dicts`

In this example, `insert_tuples` is a list of 10,000 tuples. With 
`execute_values` it takes care of chunking with the `page_size`
parameter (defaults to 100). Here we're doing inserts of 1000 at
a time. 

```python
from psycopg2.extras import execute_values
insert_tuples = ... # Something that returns a list of tuples
cols = ['a','b','c',]
with get_db_cursor() as cursor:
    qry = sql.SQL("INSERT INTO table ({}) VALUES %s").format(
        sql.SQL(', ').join(map(sql.Identifier, cols)))
    execute_values(cursor, qry, insert_tuples, page_size=1000)
```

Note: at this time there's no equivalent to `.mogrify` to be able
to inspect the SQL that gets used in `execute_values`.


## Inserting JSON

To insert JSON into the remote database, instead of converting a dict to JSON
e.g, by `json.dumps(some_dict)`, psycopg2 has a method for that. 

```python
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)
a_dict = {'a':5, 'b':6}
Json(a_dict)
# <psycopg2._json.Json at 0x103397fd0>
```

The object resulting from `Json` gets handled internally by psycopg2.

An example is in the `save_raw_scenario_to_db` method in `saved_scenario.py`.

