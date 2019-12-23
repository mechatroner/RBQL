import rbql
from rbql import rbql_csv

user_query = 'SELECT a1, int(a2) % 1000 WHERE a3 != "USA" LIMIT 5'
error_info, warnings = rbql_csv.query_csv(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8')
if error_info is None:
    print(open('output.csv').read())
else:
    print('Error: {}: {}'.format(error_info['type'], error_info['message']))
