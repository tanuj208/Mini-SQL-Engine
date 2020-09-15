# Mini-SQL-Engine

## Introduction
A Mini SQL Engine which runs a subset of queries using Command Line Interface. All tables are required to be in `.csv` format stored in a directory named `files`. There must be a file named `metadata.txt` in `files` directory which contains metadata of each table.

This Engine supports the following types of queries
- **Select queries** Example - `Select * from table_name;`.
- **Project Columns** Example - `Select col1, col2 from table_name;`.
- **Aggregate Function** Sum, average, max & min are supported. Example - `Select max(col1) from table_name;`.
- **Distinct** Example - `Select distinct col1, col2 from table_name;`.
- **Where** Also supports queries with multiple tables. Example - `Select col1, col2 from table_name1, table_name2 where col1=val1 and col2=val2;`
- **Join** Projection of one or more(including all the columns) from two tables with one join
condition. Example - `Select col1, col2 from table1,table2 where table1.col1=table2.col2;`.

It also handles invalid queries and prints appropriate error statements.

## Running the code
`pip install -r requirements.txt`
`python3 main.py <Query in double quotes>`

There are some sample queries in run.sh. You can run them by the command `./run.sh`
