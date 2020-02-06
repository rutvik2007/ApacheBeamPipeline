#given a datetime object, return a feature vector with the following features
# month (11) // 0/1 for each month
#day of week (6) // 0/1 for each day
#week of month approximately
#hour of day  // 0000am-0259am, 0300am-0559am, 0600am-0859am, 0900am-1159am, 1200pm-1459pm, 1500pm-1759pm, 1800pm-2059pm... 0/
# 31 for each 4 hour period
#minute of hour 5 // 0-9, 10-19, 20-29, 30-39, 40-49 ... 0/1 for each 10 minute period

import numpy as np
from datetime import datetime

def get_date_features(x):
    months=np.zeros([11],int)
    if not x.month==12:
        months[x.month-1]=1
    days=np.zeros([6],int)
    weekday=x.weekday()
    if not weekday==6:
        days[weekday]=1
    month_part=int(x.day/7.5)
    weeks=np.zeros([4],int)
    if not month_part==4:
        weeks[month_part]=1
    hours=np.zeros([7],int)
    day_part=int(x.hour/3)
    if not day_part==7:
        hours[day_part]=1
    minutes=np.zeros([5],int)
    hour_part=int(x.minute/10)
    if not hour_part==5:
        minutes[hour_part]=1
    print(months, weeks, days, hours, minutes)
    return (np.concatenate((months, weeks, days, hours, minutes)))

print(np.size(get_date_features(datetime.now())))