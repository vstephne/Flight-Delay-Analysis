import pandas as pd
import numpy as np
import sqlite3

Flight_2015 = pd.read_csv("Flight_2015.csv")
Flight_2016 = pd.read_csv("Flight_2016.csv")
Flight_2017 = pd.read_csv("Flight_2017.csv")
Flight_2018 = pd.read_csv("Flight_2018.csv")
Flight_2019 = pd.read_csv("Flight_2019.csv")
FLY_2015 = pd.read_csv("Fly_2015.csv")
FLY_2016 = pd.read_csv("Fly_2016.csv")
FLY_2017 = pd.read_csv("Fly_2017.csv")
FLY_2018 = pd.read_csv("Fly_2018.csv")
FLY_2019 = pd.read_csv("Fly_2019.csv")
Flight_sched_2015 = pd.read_csv("Flight_Sched_2015.csv")
Flight_sched_2016 = pd.read_csv("Flight_Sched_2016.csv")
Flight_sched_2017 = pd.read_csv("Flight_Sched_2017.csv")
Flight_sched_2018 = pd.read_csv("Flight_Sched_2018.csv")
Flight_sched_2019 = pd.read_csv("Flight_Sched_2019.csv")
Airline = pd.read_csv("Airline.csv")
airport = pd.read_csv("Airport.csv")

Flight_DW = pd.concat([Flight_2015,Flight_2016,Flight_2017,Flight_2018,Flight_2019],ignore_index=True)
Flight_sched_DW = pd.concat([Flight_sched_2015,Flight_sched_2016,Flight_sched_2017,Flight_sched_2018,Flight_sched_2019],ignore_index=True)
FLY_DW = pd.concat([FLY_2015,FLY_2016,FLY_2017,FLY_2018,FLY_2019],ignore_index=True)

FLY_DW.drop_duplicates(inplace=True)
Flight_DW.drop_duplicates(inplace=True)
Flight_sched_DW.drop_duplicates(inplace=True)

def generate_Sk(id):
    return ''.join(str(ord(c)) for c in id)
Flight_sched_DW["temp"] = Flight_sched_DW.ORIGIN_AIRPORT.apply(generate_Sk)
Flight_sched_DW["Flight_key"] = Flight_sched_DW["FLIGHT_NUMBER"].astype(str) + Flight_sched_DW["temp"].astype(str)
Flight_dim = Flight_sched_DW
airport = pd.read_csv("Airport.csv")
airport["Airport_key"] = airport.IATA_CODE.apply(generate_Sk)
airport.drop("Unnamed: 0",inplace=True,axis=1)
Airport_dim = airport.set_index("Airport_key")
Location_dim = Airport_dim[["IATA_CODE","COUNTRY","STATE","CITY"]].drop_duplicates()
Location_dim.reset_index(inplace=True)
Location_dim.drop(["Airport_key"],inplace=True,axis=1)
Location_dim.drop_duplicates(inplace=True)
Location_dim["Location_key"] = list(np.random.choice(range(10000,99999),len(Location_dim),replace=False))
Airport_dim.drop(["CITY","STATE","COUNTRY"],inplace=True,axis=1)
Airport_dim.drop_duplicates(inplace=True)
Flight_dim.drop("temp",inplace=True,axis=1)
FLY_DW["Delay_key"] = list(np.random.choice(range(10000,99999),len(FLY_DW),replace=False))
FLY_DW["ARRIVAL_DELAY"] = FLY_DW.ARRIVAL_DELAY.apply(np.abs)
FLY_DW["DEPARTURE_DELAY"] = FLY_DW.DEPARTURE_DELAY.apply(np.abs)
FLY_DW["WEATHER_DELAY"] = FLY_DW.WEATHER_DELAY.apply(np.abs)
FLY_DW["SECURITY_DELAY"] = FLY_DW.SECURITY_DELAY.apply(np.abs)
Flight_dim.drop(["Unnamed: 0"],inplace=True,axis=1)
Airport_dim.reset_index(inplace=True)
time_temp = FLY_DW[["DEPARTURE_DELAY","ARRIVAL_DELAY","SECURITY_DELAY","WEATHER_DELAY","DAY_OF_WEEK","DAY","YEAR","MONTH","FLIGHT_NUMBER"]].sort_index().drop_duplicates()
time_temp["Time_key"] = list(np.random.choice(range(1,len(time_temp)+1),len(time_temp),replace=False))
time_temp["WEATHER_DELAY"].fillna(0,inplace=True)
time_temp["ARRIVAL_DELAY"].fillna(0,inplace=True)
time_temp["DEPARTURE_DELAY"].fillna(0,inplace=True)
time_temp["SECURITY_DELAY"].fillna(0,inplace=True)
Flight_dim.reset_index(inplace=True)
Airline = pd.read_csv("Airline.csv")
fact_temp = pd.merge(Flight_dim[["ORIGIN_AIRPORT","FLIGHT_NUMBER","Flight_key"]],time_temp[["Time_key","WEATHER_DELAY","SECURITY_DELAY","ARRIVAL_DELAY","DEPARTURE_DELAY","FLIGHT_NUMBER"]],on="FLIGHT_NUMBER")
fact_temp_2 = pd.merge(fact_temp[['ORIGIN_AIRPORT', 'Flight_key', 'Time_key','WEATHER_DELAY', 'SECURITY_DELAY', 'ARRIVAL_DELAY', 'DEPARTURE_DELAY']],Airport_dim[["Airport_key","IATA_CODE"]],left_on="ORIGIN_AIRPORT",right_on="IATA_CODE")
fact_temp_3 = pd.merge(fact_temp_2[["IATA_CODE","Airport_key","Time_key","Flight_key","SECURITY_DELAY","DEPARTURE_DELAY","ARRIVAL_DELAY","WEATHER_DELAY"]],Location_dim[["Location_key","IATA_CODE"]],on="IATA_CODE")
Fact_table = fact_temp_3.drop(["IATA_CODE"],axis=1)
Fact_table.drop_duplicates(inplace=True)
Location_dim.drop(["IATA_CODE"],axis=1,inplace=True)
Flight_dim.drop(["index"],inplace=True,axis=1)
time_temp.drop(["FLIGHT_NUMBER","SECURITY_DELAY","WEATHER_DELAY","ARRIVAL_DELAY","DEPARTURE_DELAY"],axis=1,inplace=True)
Time_dim = time_temp.drop_duplicates()
#Fact_table.to_csv("Fact_table.csv")
#Location_dim.to_csv("Delay_Dim_table.csv")
#Airport_dim.to_csv("Airport_Dim_table.csv")
#Flight_dim.to_csv("Flight_Dim_table.csv")
#Time_dim.to_csv("Time_Dim_table.csv")

name = input("Please Enter the Name of DW to save ")
conn = sqlite3.connect(name)
c = conn.cursor()
c.execute('CREATE TABLE Time_dim (DAY_OF_WEEK INTEGER, DAY INTEGER, YEAR INTEGER, MONTH INTEGER, Time_key INTEGER)')
c.execute('CREATE TABLE Location_dim (COUNTRY TEXT, STATE TEXT, CITY TEXT, Location_key INTEGER)')
c.execute('CREATE TABLE Airport_dim (Airport_key INTEGER, IATA_CODE TEXT, AIRPORT TEXT)')
c.execute('CREATE TABLE Flight_dim (SCHEDULED_ARRIVAL INTEGER, SCHEDULED_DEPARTURE INTEGER, DESTINATION_AIRPORT TEXT,ORIGIN_AIRPORT TEXT, FLIGHT_NUMBER INTEGER, Flight_key INTEGER)')
c.execute('CREATE TABLE Fact_table (Flight_key INTEGER, Time_key INTEGER, WEATHER_DELAY REAL, SECURITY_DELAY REAL, ARRIVAL_DELAY REAL, DEPARTURE_DELAY REAL, Airport_key INTEGER, Location_key INTEGER)')
conn.commit()

Time_dim.to_sql('Time_dim', conn, if_exists='replace', index = False)
Location_dim.to_sql('Location_dim', conn, if_exists='replace', index = False)
Airport_dim.to_sql('Airport_dim', conn, if_exists='replace', index = False)
Flight_dim.to_sql('Flight_dim', conn, if_exists='replace', index = False)
Fact_table.to_sql('Fact_table', conn, if_exists='replace', index = False)

print("Data Warehouse Created and Saved in",name)