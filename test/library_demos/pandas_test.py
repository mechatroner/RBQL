import pandas
import rbql
input_dataframe = pandas.DataFrame([
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
], columns=['name', 'year', 'country'])
user_query = 'SELECT a.name, a.year % 1000 WHERE a.country != "France" LIMIT 3'
warnings = []
result_dataframe = rbql.query_pandas_dataframe(user_query, input_dataframe, warnings)
print(result_dataframe)
