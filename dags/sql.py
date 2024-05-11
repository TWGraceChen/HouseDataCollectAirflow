def createstate(table_name,schema):
    sql = "CREATE TABLE IF NOT EXISTS "+ table_name + "("
    for c in schema:
        sql = sql + c + " " + schema[c] +","
    sql = sql[:-1] + ")" 
    return sql

def dropstate(table_name):
    sql = "DROP TABLE IF EXISTS "+ table_name
    return sql

def getcols(table_name,schema):
    cols = []
    for c in schema:
        cols.append(c)

    return cols