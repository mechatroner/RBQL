import rbql
from rbql import rbql_csv

input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
]
user_query = 'SELECT a1, int(a2) % 1000 WHERE a3 != "USA" LIMIT 3'
output_table = []
error_info, warnings = rbql.table_run(user_query, input_table, output_table)
if error_info is None:
    for record in output_table:
        print(','.join([str(v) for v in record]))
else:
    print('Error: {}: {}'.format(error_info['type'], error_info['message']))
