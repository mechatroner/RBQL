import rbql
input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
]
user_query = 'SELECT a1, a2 % 1000 WHERE a3 != "USA" LIMIT 3'
output_table = []
warnings = []
rbql.query_table(user_query, input_table, output_table, warnings)
for record in output_table:
    print(','.join([str(v) for v in record]))
