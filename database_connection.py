import time
# import pymssql
import pandas as pd


server = 'aibest.database.windows.net'
database = 'aibest'
username = 'robert'
password = '{Dragos123}'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = 'DRIVER='+driver+';SERVER=tcp:'+server + \
    ';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password

# def test():
#     conn = pymssql.connect(server=server, user=username, password='Dragos123', database=database)
#     cursor = conn.cursor()

#     cursor.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES')  
#     row = cursor.fetchone()  
#     while row:  
#         print(f"{row}\n")     
#         row = cursor.fetchone()  

    # conn.close()
    
# def execute_query(query):
#     with pymssql.connect(server=server, user=username, password='Dragos123', database=database) as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(query)
#             row = cursor.fetchall()
#             return(row)


def insert(import_data):
    start_time=time.time()
    querry = ""
    for table_name, table_data in import_data.items():
        # if table_name!="salariati": continue
        querry += f"\nDROP TABLE IF EXISTS {table_name} ;\nCREATE TABLE {table_name} ("
        for column_name, column_type in table_data[0].items():
            querry += f"{column_name} VARCHAR(250),"
        querry = querry[:-1]+");"
        # for row in table_data:
        #     querry += f"\nINSERT INTO {table_name} VALUES ("
        #     for column_name, column_value in row.items():
        #         querry += f"'{column_value}',"
        #     querry = querry[:-1]+");"
        # break
        for row in table_data:
            row_columns=""
            row_values=""
            for column_name, column_value in row.items():
                row_columns += f"{column_name},"
                row_values += f"'{column_value}',"  
            querry+= f"\nINSERT INTO {table_name} ({row_columns[:-1]}) VALUES({row_values[:-1]});"


    print(f'Sql query building finished in : {time.time()-start_time}')
    with open("revisalImportQuery.sql", "w",encoding="utf-8") as text_file:
        print(f"{querry}", file=text_file)
    
    # salariati = pd.DataFrame(import_data)
    # salariati.reset_index(drop=True,inplace=True)#reset index after duplicates
    # writer = pd.ExcelWriter(f'output.xlsx', engine='xlsxwriter')
    with pd.ExcelWriter(f'revisalImport.xlsx') as writer:
        for name,data in import_data.items():
            pd.DataFrame(data).to_excel(writer,sheet_name=name)        

    # try:
    #     with pymssql.connect(server=server, user=username, password='Dragos123', database=database) as conn:
    #         with conn.cursor() as cursor:
    #             print(f'Sql query execution starting in : {time.time()-start_time}')
    #             cursor.execute(querry)
    #             print(f'Sql query execution finished in : {time.time()-start_time}')
    # except:
    #     querry+="-- insertul nu a mers ! "
    return querry
    
