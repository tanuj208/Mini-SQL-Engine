SELECT_ERROR = "Query does not start with SELECT clause"
FROM_ERROR = "Query does not contain FROM clause"
INVALID_SYNTAX = "Invalid syntax"
INVALID_WHERE_CONDITION = "Invalid where condition"
FILE_OPEN_ERROR = "{} file could not be opened, following error occurred"
UPDATE_METADATA_ERROR = "Please update the metadata"
NUMBER_ALLOWED_ERROR = "Only numbers are allowed as table entries"
NO_TABLE_ERROR = "There is no {} table in the database"
NO_COLUMN_IN_TABLE_ERROR = "There is no {} column in {} table"
NO_COLUMN_ERROR = "There is no {} column in any of given tables"
SEMICOLON_ERROR = "Each query must end with ;"
TABLE_REPEAT_ERROR = "Table name can't be repeated in FROM clause"
AGGREGATE_FUNCTION_ERROR = "Cannot select aggregate function and normal column together"
NO_AGGREGATE_FUNCTION_ERROR = "Allowed aggregates functions are MAX, MIN, AVG, SUM"
STAR_AGGREGATE_ERROR = "* is not allowed inside aggregate functions"
AMBIGUITY_ERROR = "{} column name is ambiguous"

def show_error(err, err2 = None):
    print(err)
    if err2:
        print(err2)
    exit(0)

