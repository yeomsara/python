
def save_model(clf,mdl_file_nm):
    from sklearn.externals import joblib
    DIR = '/home/cdsadmin/python_src/EY/YUN/
    model = pickle.dump(clf,open(DIR+mdl_file_nm,'wb'))
    print('%경로에 %모델 저장완료 '%(DIF,mdl_file_nm))

#save model
def load_model(filename):
    import pickle
    load_model = pickle.load(open(filename))
    return load_model

tree_param_grad ={
    'max_depth' : np.array(range(5,45,5)).tolist(),
    'n_estimotors' : np.array(range(100,1100,100)).tolist(),
    'subsa,ple' : np.array(np.round(np.arange(0.5,1,0.1),2))
}

def dict_product(param):
    import itertools
    return (dict(zip(param,x)) for x in itertools.product(*param.values())) 

params = list(dict_product(tree_param_grad))

#load model 
