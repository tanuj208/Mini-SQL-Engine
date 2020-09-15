import sys
import sqlparse
from query import Query
from database import Database

database = Database('files/metadata.txt')
query = sys.argv[1]
query = sqlparse.format(query, keyword_case = 'upper')
query = sqlparse.parse(query)
for q in query:
    q = Query(q, database)
    q.parse_statement()
    q.query_statement()
    q.print_output()