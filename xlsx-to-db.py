from psycopg2.extras import execute_values
import psycopg2
import pandas as pd
import glob
import os

connection = psycopg2.connect(user='adm_idf',
            password='4G8j2&2z8Qg3',
            host='d-idf-br-mkt-sgbd-postgre-fr.postgres.database.azure.com',
            port='5432',
            database='postgres')

batch_size: int = 20000

# path = os.getcwd()
files = glob.glob(os.path.join('../kidf-files/sidv-data/2019/', "*.xlsx"))

for file in files:
    print(file)
    df = pd.read_excel(file)

    print(df.head())
    print(len(df))


    query  = "INSERT INTO sidv_idfm.ridership_per_line_tmp VALUES %s"
    cursor = connection.cursor()
    tuples = []
    for idx, row in df.iterrows():
        tmp = []
        for cell in row:
            # print(cell)
            tmp.append(str(cell))
        tuples.append(tuple(tmp))

        if idx % batch_size == 0:
            # print(tuples)
            execute_values(cursor, query, tuples)
            connection.commit()
            tuples = []
        # print(idx)
    execute_values(cursor, query, tuples)
    connection.commit()
