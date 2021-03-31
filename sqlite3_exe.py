
sqlite_db = 'ysr_temp.db'

def create_table(table_name , df):
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    cols_name = ','.join(df.columns)
    query = ''' CREATE TABLE %s (%s)'''%(table_name,cols_name)
    cur.execute(query)
    conn.commit()
    conn.close()
    
def insert_table(table_name,df,key):
    query = f"""SELECT MODEL_KEY FROM {table_name} WHERE MODEL_KEY = '{key}' """
    print(query)
    model_list = select_table(query)
    if len(model_list) > 0 :
        execute_query(f"""DELETE FROM YSR_ML_VALIDATION WHERE MODEL_KEY = '{key}'""")
    else:
        conn = sqlite3.connect(sqlite_db)
        cur = conn.cursor()
        input_data = [tuple(x)for x in df.values]
        cols_len = '?,'*int(len(df.columns))
        cols_len = cols_len[:-1]
        query    = '''INSERT INTO %s VALUES(%s)'''%(table_name,cols_len)
        cur.executemany(query, input_data)
        conn.commit()
        conn.close()
        return print("%s 테이블 데이터 입력완료"%table_name)    
    
def select_table(query):
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    query = query
    input_t = cur.execute(query)
    cols = [column[0] for column in input_t.description]
    df = pd.DataFrame.from_records(data=input_t.fetchall(), columns=cols)
    print(' row : %s  / columns : %s'%(df.shape[0],df.shape[1]))
    conn.close()
    return df

def execute_query(query):
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    query = query
    cur.execute(query)
    conn.close()
    print("%s 실행완료"%query)

    
    
