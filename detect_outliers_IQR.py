
def detect_outliers(df):
    outlier_cols = []
    perc_list = []
    upper_limit = []
    lower_limit = []
    outlier_cnt = []
    all_cnt = []
    for col, data in df.items():
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        iqr = q3 - q1
        scale = 1.5
        lower = q1 - scale * iqr
        upper = q3 + scale * iqr
        v_col = data[(data < lower) | (data > upper)]
        perc =( np.shape(v_col)[0] / np.shape(df)[0] )* 100.0
        print("Column %s outliers [ %s / %s ] = %.2f%%" % (col,np.shape(v_col)[0],np.shape(df)[0], perc))
        outlier_indices = np.shape(v_col)[0]
        if (outlier_indices == 0):
            print('%s 변수에 탐지된 이상치는 없습니다.\n'%col)
        else:
            print('%s 변수에 탐지된 이상치 데이터의 개수  : %s\n'%(col,outlier_indices))
            outlier_cols.append(col)
            perc_list.append(perc)
            outlier_cnt.append(np.shape(v_col)[0])
            all_cnt.append(np.shape(df)[0])
            lower_limit.append(lower)
            upper_limit.append(upper)
    result = pd.DataFrame({'outlier_list' : outlier_cols,
                           'all_cnt' : all_cnt,
                           'outlier_cnt' : outlier_cnt,
                           'perc' : perc_list,
                           'upper_limit':upper_limit,
                           'lower_limit':lower_limit})
    result = result.sort_values(by=['perc'],ascending = False)  
    return result
