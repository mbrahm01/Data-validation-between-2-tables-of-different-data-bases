import pandas as pd
import pyodbc,warnings

warnings.filterwarnings("ignore", category=UserWarning)

server1 = '(localdb)\manish'
database1 = 'manish'

connection_string1 = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server1};DATABASE={database1};'
connection1 = pyodbc.connect(connection_string1)
query1 = "SELECT * FROM [dbo].[baseline_counts]"
df1 = pd.read_sql(query1, connection1)
# print(df1)
# df1_sorted= df1.sort_values('result_group_id')
server2 = '(localdb)\manish2'
database2 = 'manish2'

connection_string2 = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server2};DATABASE={database2};'
connection2 = pyodbc.connect(connection_string2)

query2 = "SELECT * FROM [dbo].[baseline_counts]"
df2 = pd.read_sql(query2, connection2)

df_merge=pd.merge(df1, df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
df_merge2=pd.merge(df2, df1, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
print(df_merge)
print(df_merge2)
# Close the connections
connection1.commit()
connection2.commit()
connection1.close()
connection2.close()
