import os
import pandas as pd
import sqlalchemy

str_connection = 'sqlite:///{path}'

base_dir = (os.path.abspath(''))
data_dir = os.path.join(base_dir, 'data')

print(f'Meu diretorio do projeto é: {base_dir}')
print(f'\nMeu diretorio do data é: {data_dir}')

files_names = os.listdir(data_dir)

#Abrindo conexão com o Banco de Dados....
str_connection = str_connection.format(path=os.path.join(data_dir, 'olist.db'))
conecction = sqlalchemy.create_engine(str_connection)

for i in files_names:
    df_temp = pd.read_csv(os.path.join(data_dir, i))
    table_name = "tb_" + i.strip('.csv').replace('olist_', '').replace('_dataset', '')
    df_temp.to_sql(table_name, conecction)
