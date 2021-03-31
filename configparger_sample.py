import configparser
global HOST,PORT,DB_ID,DB_PW,MODEL_DIR,MODEL_NAME
conf_dir = '/home/cdsadmin/python_src/EY/Emart/conf/config.ini'
cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
cfg.read(conf_dir)

HOST      = cfg['dbconnect']['host']
PORT      = int(cfg['dbconnect']['port'])
DB_ID     = cfg['dbconnect']['ID']
DB_PW     = cfg['dbconnect']['PW']
MODEL_DIR = cfg['Timing']['MODEL_DIR']

def DB_Connection() :
    conn=dbapi.connect(HOST,PORT,DB_ID,DB_PW)
    return conn

def select_query(sql) :
    conn = DB_Connection()
    cnt = pd.read_sql(sql, conn)
    conn.close()
    
def load_model(filename):
    model_dir  = MODEL_DIR+filename
    load_model = joblib.load(model_dir)
    return load_model

print(MODEL_DIR)
