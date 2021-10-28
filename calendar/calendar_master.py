
from datetime import datetime,date,timedelta


national_holidays = pd.read_csv('national_holidays_2018_2031.csv')
national_holidays = national_holidays.drop_duplicates(subset=['national_cd','date'],keep='first')
national_holidays = national_holidays[~national_holidays.holiday_names.str.contains('Solstice')]
national_holidays = national_holidays[~national_holidays.holiday_names.str.contains('Equinox')]

sdate = date(2018,1,1)
edate = date(2031,12,31)

delta = edate - sdate

date_list    = [(sdate+timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days+1)]
weekday_list = [(sdate+timedelta(days=i)).weekday() for i in range(delta.days+1)]
cal_df = pd.DataFrame({'date' : date_list,'weekday_cd':weekday_list})
days   = ['월','화','수','목','금','토','일']
cal_df['weekday']    = cal_df.weekday_cd.apply(lambda x : days[x])
cal_df['weekend_yn'] = cal_df.weekday.apply(lambda x : 'Y' if x in ['토','일'] else 'N')

cal_df['year'] = cal_df.date.str.slice(0,4)
cal_df['month'] = cal_df.date.str.slice(5,7)
cal_df['day'] = cal_df.date.str.slice(8,10)
cal_df = cal_df.reindex(columns=['year','month','day','date','weekday_cd','weekday','weekend_yn'])

national_holidays.to_csv('national_holidays_master.csv',index=0)
cal_df.to_csv('calendar_master.csv',index=0)
