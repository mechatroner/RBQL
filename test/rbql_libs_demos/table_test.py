import rbql
from rbql import rbql_csv

user_query = 'SELECT a1, int(a2) % 1000 WHERE a3 != "USA" LIMIT 5'
error_info, warnings = rbql_csv.csv_run(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8')
if error_info is None:
    print('result table:')
    result_data = open('output.csv').read()
    print(result_data)
else:
    print('Error: {}: {}'.format(error_info['type'], error_info['message']))
