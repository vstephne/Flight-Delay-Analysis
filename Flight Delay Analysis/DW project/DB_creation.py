import pandas as pd
import numpy as np
df = pd.read_csv("flights.csv")
from sklearn.model_selection import StratifiedShuffleSplit
df_imp = df[['YEAR','MONTH','DAY','DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER',
       'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT','TAIL_NUMBER',
       'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT',
       'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE',
       'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME',
       'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED', 'CANCELLATION_REASON',
       'AIR_SYSTEM_DELAY', 'SECURITY_DELAY', 'AIRLINE_DELAY',
       'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY']]
ss_split = StratifiedShuffleSplit(n_splits=1,test_size=0.01,random_state=42)
for train_index, test_index in ss_split.split(df_imp,df_imp.MONTH):
    print("TRAIN:", train_index, "TEST:", test_index)
    #df_unused = df_imp.loc[train_index]
    df_used = df_imp.loc[test_index]
#df_sample = df_imp.sample(frac=0.5)
print("data used now is of size",len(df_used))
print("amount of data redused is",np.divide(len(df_used),len(df_imp)))
ss_2019 = StratifiedShuffleSplit(n_splits=1,test_size=0.25,random_state=42)
for train_index, test_index in ss_2019.split(df_used,df_used.MONTH):
    print("TRAIN:", train_index, "TEST:", test_index)
    df_2019 = df_imp.loc[test_index]
ss = StratifiedShuffleSplit(n_splits=1,test_size=0.5,random_state=42)

for train_index, test_index in ss.split(df_used,df_used.MONTH):
    print("TRAIN:", train_index, "TEST:", test_index)
    df_part_1 = df_imp.loc[train_index]
    df_part_2 = df_imp.loc[test_index]
for train_index, test_index in ss.split(df_part_1,df_part_1.MONTH):
    print("TRAIN:", train_index, "TEST:", test_index)
    df_2015 = df_imp.loc[train_index]
    df_2016 = df_imp.loc[test_index]
for train_index, test_index in ss.split(df_part_2,df_part_2.MONTH):
    print("TRAIN:", train_index, "TEST:", test_index)
    df_2017 = df_imp.loc[train_index]
    df_2018 = df_imp.loc[test_index]
df_2015["MONTH"] = list(np.random.choice(range(1,13),len(df_2015),replace=True))
df_2016["MONTH"] = list(np.random.choice(range(1,13),len(df_2016),replace=True))
df_2017["MONTH"] = list(np.random.choice(range(1,13),len(df_2017),replace=True))
df_2018["MONTH"] = list(np.random.choice(range(1,13),len(df_2018),replace=True))
df_2019["MONTH"] = list(np.random.choice(range(1,13),len(df_2019),replace=True))
print("len of data now for each year is", len(df_2015))
print("lenght of data of 2015 is",len(df_2015))
print("lenght of data of 2016 is",len(df_2016))
print("lenght of data of 2017 is",len(df_2017))
print("lenght of data of 2018 is",len(df_2018))
print("lenght of data of 2019 is",len(df_2019))
df_2016["YEAR"] = 2016
df_2017["YEAR"] = 2017
df_2018["YEAR"] = 2018
df_2019["YEAR"] = 2019
Flight_2015 = df_2015[['FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE']]
Flight_2016 = df_2016[['FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE']]
Flight_2017 = df_2017[['FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE']]
Flight_2018 = df_2018[['FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE']]
Flight_2019 = df_2019[['FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE']]
Flight_sched_2015 = df_2015[['SCHEDULED_ARRIVAL','SCHEDULED_DEPARTURE','DESTINATION_AIRPORT','ORIGIN_AIRPORT','FLIGHT_NUMBER']]
Flight_sched_2016 = df_2016[['SCHEDULED_ARRIVAL','SCHEDULED_DEPARTURE','DESTINATION_AIRPORT','ORIGIN_AIRPORT','FLIGHT_NUMBER']]
Flight_sched_2017 = df_2017[['SCHEDULED_ARRIVAL','SCHEDULED_DEPARTURE','DESTINATION_AIRPORT','ORIGIN_AIRPORT','FLIGHT_NUMBER']]
Flight_sched_2018 = df_2018[['SCHEDULED_ARRIVAL','SCHEDULED_DEPARTURE','DESTINATION_AIRPORT','ORIGIN_AIRPORT','FLIGHT_NUMBER']]
Flight_sched_2019 = df_2019[['SCHEDULED_ARRIVAL','SCHEDULED_DEPARTURE','DESTINATION_AIRPORT','ORIGIN_AIRPORT','FLIGHT_NUMBER']]
df_2015.reset_index(inplace=True)
df_2016.reset_index(inplace=True)
df_2017.reset_index(inplace=True)
df_2018.reset_index(inplace=True)
df_2019.reset_index(inplace=True)
FLY_2015 = df_2015[['WEATHER_DELAY','SECURITY_DELAY','ARRIVAL_DELAY','DEPARTURE_DELAY','ARRIVAL_TIME','DEPARTURE_TIME','DAY_OF_WEEK','DAY','MONTH','YEAR','FLIGHT_NUMBER']]
FLY_2016 = df_2016[['WEATHER_DELAY','SECURITY_DELAY','ARRIVAL_DELAY','DEPARTURE_DELAY','ARRIVAL_TIME','DEPARTURE_TIME','DAY_OF_WEEK','DAY','MONTH','YEAR','FLIGHT_NUMBER']]
FLY_2017 = df_2017[['WEATHER_DELAY','SECURITY_DELAY','ARRIVAL_DELAY','DEPARTURE_DELAY','ARRIVAL_TIME','DEPARTURE_TIME','DAY_OF_WEEK','DAY','MONTH','YEAR','FLIGHT_NUMBER']]
FLY_2018 = df_2018[['WEATHER_DELAY','SECURITY_DELAY','ARRIVAL_DELAY','DEPARTURE_DELAY','ARRIVAL_TIME','DEPARTURE_TIME','DAY_OF_WEEK','DAY','MONTH','YEAR','FLIGHT_NUMBER']]
FLY_2019 = df_2019[['WEATHER_DELAY','SECURITY_DELAY','ARRIVAL_DELAY','DEPARTURE_DELAY','ARRIVAL_TIME','DEPARTURE_TIME','DAY_OF_WEEK','DAY','MONTH','YEAR','FLIGHT_NUMBER']]

FLY_2015.to_csv("Fly_2015.csv")
Flight_sched_2015.to_csv("Flight_Sched_2015.csv")
Flight_2015.to_csv("Flight_2015.csv")

FLY_2016.to_csv("Fly_2016.csv")
Flight_sched_2016.to_csv("Flight_Sched_2016.csv")
Flight_2016.to_csv("Flight_2016.csv")

FLY_2017.to_csv("Fly_2017.csv")
Flight_sched_2017.to_csv("Flight_Sched_2017.csv")
Flight_2017.to_csv("Flight_2017.csv")

FLY_2018.to_csv("Fly_2018.csv")
Flight_sched_2018.to_csv("Flight_Sched_2018.csv")
Flight_2018.to_csv("Flight_2018.csv")

FLY_2019.to_csv("Fly_2019.csv")
Flight_sched_2019.to_csv("Flight_Sched_2019.csv")
Flight_2019.to_csv("Flight_2019.csv")