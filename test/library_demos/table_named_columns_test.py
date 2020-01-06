import rbql
input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
]
user_query = 'SELECT a.name, "birth century: {}".format(a.DOB // 100 + 1) WHERE a.name == "Roosevelt" or re.search("an", a.country, re.IGNORECASE) is not None ORDER BY random.random()'
output_table = []
warnings = []
rbql.query_table(user_query, input_table, output_table, warnings, input_column_names=['name', 'DOB', 'country'])
for record in output_table:
    print(','.join([str(v) for v in record]))
