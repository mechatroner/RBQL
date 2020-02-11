import rbql
user_query = 'SELECT a1, int(a2) % 1000 WHERE a3 != "USA" LIMIT 5'
warnings = []
rbql.query_csv(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8', warnings)
print(open('output.csv').read())
