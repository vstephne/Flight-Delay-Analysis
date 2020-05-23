import sqlite3
from sqlite3 import Error
from flask import Flask, request, jsonify

app = Flask(__name__)

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

def airportFlight(airport,flight):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT airport_key,IATA_CODE from Airport_dim where AIRPORT= '%s'" % airport)
    rows_city = cur.fetchall()
    IATA_CODE=rows_city[0][1]
    airport_key=rows_city[0][0]
    #cur.execute("SELECT Flight_key from Fact_table where Airport_Key= '%s'" % airport_key)
    cur.execute("SELECT DISTINCT Flight_key from Flight_dim where ORIGIN_AIRPORT= '%s' and FLIGHT_NUMBER='%s'" % (IATA_CODE,flight) )
    rows_flight = cur.fetchall()
    flight_key=rows_flight[0][0]
    return flight_key,airport_key,IATA_CODE

def getFlightBasedAirport(IATA_CODE):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT FLIGHT_NUMBER from Flight_dim where ORIGIN_AIRPORT= '%s'" % (IATA_CODE) )
    rows_flight = cur.fetchall()
    flight_key=rows_flight
    return flight_key

def getAirportKey(airport):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT airport_key,IATA_CODE from Airport_dim where AIRPORT= '%s'" % airport)
    rows_city = cur.fetchall()
    IATA_CODE=rows_city[0][1]
    airport_key=rows_city[0][0]
   
    return airport_key,IATA_CODE

def getTimeKeyFactAirport(airport_key):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT Time_key from Fact_table where Airport_key='%s'" % (airport_key))
    rows_time = cur.fetchall()

    return rows_time

def getTimeKeyFact(flight_key,airport_key):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT Time_key from Fact_table where Airport_key='%s' and Flight_key='%s'" % (airport_key,flight_key))
    rows_time = cur.fetchall()

    return rows_time



def getTimeKeyDim(year):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT Time_key from Time_dim where YEAR='%s'" % (year))
    rows_time = cur.fetchall()

    return rows_time

def getTimeKeyDimMonth(year,month):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT Time_key from Time_dim where YEAR='%s' and MONTH='%s'" % (year,month))
    rows_time = cur.fetchall()
    
    return rows_time


#given airport,flight and year the API return all the time keys 
@app.route("/AFT", methods=["GET"])
def getTimeKeyYear():
    airport = request.args.get('airport')
    flight = request.args.get('flight')
    year = request.args.get('year')
    flight_key,airport_key,IATA_CODE=airportFlight(airport,flight)
    #print(flight_key,airport_key)
    fact_time_key = getTimeKeyFact(flight_key,airport_key)
    dim_time_key =  getTimeKeyDim(year)
    
    
    time_key_year=set(dim_time_key).intersection(fact_time_key)
    time_key_year =  tuple(i[0] for i in time_key_year)
    
    return (time_key_year,flight_key,airport_key)

#returns flight_key,airport_key,IATA_CODE,flight_number,airport_name if airport name and flight number are provided as input

def flightAirport():
    airport = request.args.get('airport')
    flight = request.args.get('flight')
    return airportFlight(airport,flight)



#API takes in airport name and year and return airport_key(can  be used to get delay at airport level)
@app.route("/AirportYear", methods=["GET"])
def getAirportLevelDetail():  #gets airport key and time key
    airport = request.args.get('airport')
    year = request.args.get('year')
 
    airport_key,IATA_CODE=getAirportKey(airport)
    
    fact_time_key = getTimeKeyFactAirport(airport_key)
    dim_time_key =  getTimeKeyDim(year)
    
    time_key_month=set(dim_time_key).intersection(fact_time_key)
    
    return jsonify({'timeKey':list(time_key_month),
                   'year':year,
                   'airport':airport,
                   'airport_key':airport_key})

#API tales in airport name and returns flights which fly from the airport
@app.route("/getFlightsAir", methods=["GET"])
def getAirportFlightDetail():  #gets airport key and time key
    airport = request.args.get('airport')

    airport_key,IATA_CODE=getAirportKey(airport)
    flights=getFlightBasedAirport(IATA_CODE)
    flights =  tuple(i[0] for i in flights)
    print(flights)
    return jsonify({'flights':flights,
                   'airport':airport
                   })
###########################################################################################################
#API takes in airport name and flight number and returns individual delay sum and total delay sum over 4 years
@app.route("/sumDelayAF", methods=["GET"])
def getsumDelayAF():  
    flight_key,airport_key,IATA_CODE = flightAirport()
    print(flight_key,airport_key)
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY) from Fact_table where Airport_key='%s' and Flight_key='%s'" % (airport_key,flight_key))
    row_delay_sum = cur.fetchall()
    print(row_delay_sum[0][0])
    return jsonify({"SECURITY_DELAY":row_delay_sum[0][0],
                    "DEPARTURE_DELAY":row_delay_sum[0][1],
                    "WEATHER_DELAY":row_delay_sum[0][2],
                    "ARRIVAL_DELAY":row_delay_sum[0][3],
                    "TOTAL_DELAY":(row_delay_sum[0][0]+row_delay_sum[0][1]+row_delay_sum[0][2]+row_delay_sum[0][3])
                    })

#API takes in airport name, flight name and  year returns individual delay sum and total delay sum over 4 years
@app.route("/sumDelayAFY", methods=["GET"])
def getsumDelayAFY():  
    time_key,flight_key,airport_key = getTimeKeyYear()
    
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY) from Fact_table where Airport_key={} and Flight_key={} and Time_key IN {}" .format(airport_key,flight_key,str(time_key)))
    row_delay_sum = cur.fetchall()
    print(row_delay_sum[0][0])
    return jsonify({"SECURITY_DELAY":row_delay_sum[0][0],
                    "DEPARTURE_DELAY":row_delay_sum[0][1],
                    "WEATHER_DELAY":row_delay_sum[0][2],
                    "ARRIVAL_DELAY":row_delay_sum[0][3],
                    "TOTAL_DELAY":(row_delay_sum[0][0]+row_delay_sum[0][1]+row_delay_sum[0][2]+row_delay_sum[0][3])
                    })


#############################################################  HOME PAGE    ########################################

#API returns flights affected and not affected by weather delay over all the years
@app.route("/weatherDelay", methods=["GET"])
def getgeneralWeather():      
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) from Fact_table where WEATHER_DELAY=0.0")
    count_no_weather_delay = cur.fetchall()
    print("no delay:",count_no_weather_delay[0][0])
    cur.execute("SELECT COUNT(*) from Fact_table" )
    count_total_weather_delay = cur.fetchall()
    count_weather_delay=count_total_weather_delay[0][0] - count_no_weather_delay[0][0]
    
    return jsonify({ "flights Delayed due to weather" : count_weather_delay,
                  "flights not delayed due to weather": count_no_weather_delay[0][0] })

#API returns flights affected and not affected by security delay over all the years
@app.route("/securityDelay", methods=["GET"])
def getgeneralSecurity():      
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) from Fact_table where SECURITY_DELAY=0.0")
    count_no_security_delay = cur.fetchall()
    
    cur.execute("SELECT COUNT(*) from Fact_table" )
    count_total_security_delay = cur.fetchall()
    count_security_delay=count_total_security_delay[0][0] - count_no_security_delay[0][0]
    
    return jsonify({ "flights Delayed due to weather" : count_security_delay,
                  "flights not delayed due to weather": count_no_security_delay[0][0] })

#API returns flights affected and not affected by departure delay over all the years
@app.route("/arrivalDelay", methods=["GET"])
def getgeneralArrivalDelay():      
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) from Fact_table where ARRIVAL_DELAY=0.0")
    count_no_arrival_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table" )
    count_total_arrival_delay = cur.fetchall()
    count_arrival_delay=count_total_arrival_delay[0][0] - count_no_arrival_delay[0][0]
    
    return jsonify({ "flights Delayed due to weather" : count_arrival_delay,
                  "flights not delayed due to weather": count_no_arrival_delay[0][0] })

#API returns flights affected and not affected by departure delay over all the years
@app.route("/departureDelay", methods=["GET"])
def getgeneralDepaartureDelay():      
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) from Fact_table where DEPARTURE_DELAY=0.0")
    count_no_departure_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table" )
    count_total_departure_delay = cur.fetchall()
    count_departure_delay=count_total_departure_delay[0][0] - count_no_departure_delay[0][0]
    
    return jsonify({ "flights Delayed due to weather" : count_departure_delay,
                  "flights not delayed due to weather": count_no_departure_delay[0][0] })

#API returns flights affected and not affected by departure delay over individual years
@app.route("/CDepartureYearDelay", methods=["GET"])
def getDepartureYearDelay(): 
    year = request.args.get('year')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where YEAR='%s'" % year)
    time_key_year = cur.fetchall()
    time_key_year =  tuple(i[0] for i in time_key_year)
    cur.execute("SELECT COUNT(*) from Fact_table where DEPARTURE_DELAY=0.0 and Time_key IN {}".format(str(time_key_year)) )
    count_no_year_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_year)) )
    count_total_year_delay = cur.fetchall()
    count_year_delay=count_total_year_delay[0][0] - count_no_year_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered year" : count_year_delay,
                  "flights not delayed due for entered year": count_no_year_delay[0][0] })

#API returns flights affected and not affected by weather delay over individual years
@app.route("/CWeatherYearDelay", methods=["GET"])
def getWeatherYearDelay(): 
    year = request.args.get('year')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where YEAR='%s'" % year)
    time_key_year = cur.fetchall()
    time_key_year =  tuple(i[0] for i in time_key_year)
    cur.execute("SELECT COUNT(*) from Fact_table where WEATHER_DELAY=0.0 and Time_key IN {}".format(str(time_key_year)) )
    count_no_year_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_year)) )
    count_total_year_delay = cur.fetchall()
    count_year_delay=count_total_year_delay[0][0] - count_no_year_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered year" : count_year_delay,
                  "flights not delayed due for entered year": count_no_year_delay[0][0] })

#API returns flights affected and not affected by security delay over individual years
@app.route("/CSecurityYearDelay", methods=["GET"])
def getSecurityYearDelay(): 
    year = request.args.get('year')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where YEAR='%s'" % year)
    time_key_year = cur.fetchall()
    time_key_year =  tuple(i[0] for i in time_key_year)
    cur.execute("SELECT COUNT(*) from Fact_table where SECURITY_DELAY=0.0 and Time_key IN {}".format(str(time_key_year)) )
    count_no_year_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_year)) )
    count_total_year_delay = cur.fetchall()
    count_year_delay=count_total_year_delay[0][0] - count_no_year_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered year" : count_year_delay,
                  "flights not delayed due for entered year": count_no_year_delay[0][0] })

#API returns flights affected and not affected by arrival delay over individual years
@app.route("/CArrivalYearDelay", methods=["GET"])
def getArrivalYearDelay(): 
    year = request.args.get('year')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where YEAR='%s'" % year)
    time_key_year = cur.fetchall()
    time_key_year =  tuple(i[0] for i in time_key_year)
    cur.execute("SELECT COUNT(*) from Fact_table where ARRIVAL_DELAY=0.0 and Time_key IN {}".format(str(time_key_year)) )
    count_no_year_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_year)) )
    count_total_year_delay = cur.fetchall()
    count_year_delay=count_total_year_delay[0][0] - count_no_year_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered year" : count_year_delay,
                  "flights not delayed due for entered year": count_no_year_delay[0][0] })


#API takes in input as Year and returns sum of individual category and total delay
@app.route("/sumDelayYear", methods=["GET"])
def getsumDelayYear():  
    year = request.args.get('year')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where YEAR='%s'" % year)
    time_key_year = cur.fetchall()
    time_key_year =  tuple(i[0] for i in time_key_year)  
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY) from Fact_table where Time_key IN {}" .format(str(time_key_year)))
    row_delay_sum = cur.fetchall()
    return jsonify({"SECURITY_DELAY":row_delay_sum[0][0],
                    "DEPARTURE_DELAY":row_delay_sum[0][1],
                    "WEATHER_DELAY":row_delay_sum[0][2],
                    "ARRIVAL_DELAY":row_delay_sum[0][3],
                    "TOTAL_DELAY":(row_delay_sum[0][0]+row_delay_sum[0][1]+row_delay_sum[0][2]+row_delay_sum[0][3])
                    })

#API takes in input as Month and returns sum of individual category and total delay
@app.route("/sumDMonth", methods=["GET"])
def getsumDelaymonth():  
    month = request.args.get('month')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s'" % month)
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)  
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY) from Fact_table where Time_key IN {}" .format(str(time_key_month)))
    row_delay_sum = cur.fetchall()
    return jsonify({"SECURITY_DELAY":row_delay_sum[0][0],
                    "DEPARTURE_DELAY":row_delay_sum[0][1],
                    "WEATHER_DELAY":row_delay_sum[0][2],
                    "ARRIVAL_DELAY":row_delay_sum[0][3],
                    "TOTAL_DELAY":(row_delay_sum[0][0]+row_delay_sum[0][1]+row_delay_sum[0][2]+row_delay_sum[0][3])
                    })


#API returns flights affected and not affected by departure delay over individual months
@app.route("/CDeparturemonthDelay", methods=["GET"])
def getDeparturemonthDelay(): 
    month = request.args.get('month')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s'" % month)
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)
    cur.execute("SELECT COUNT(*) from Fact_table where DEPARTURE_DELAY=0.0 and Time_key IN {}".format(str(time_key_month)) )
    count_no_month_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_month)) )
    count_total_month_delay = cur.fetchall()
    count_month_delay=count_total_month_delay[0][0] - count_no_month_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered month" : count_month_delay,
                  "flights not delayed due for entered month": count_no_month_delay[0][0] })

#API returns flights affected and not affected by weather delay over individual months
@app.route("/CWeathermonthDelay", methods=["GET"])
def getWeathermonthDelay(): 
    month = request.args.get('month')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s'" % month)
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)
    cur.execute("SELECT COUNT(*) from Fact_table where WEATHER_DELAY=0.0 and Time_key IN {}".format(str(time_key_month)) )
    count_no_month_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_month)) )
    count_total_month_delay = cur.fetchall()
    count_month_delay=count_total_month_delay[0][0] - count_no_month_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered month" : count_month_delay,
                  "flights not delayed due for entered month": count_no_month_delay[0][0] })

#API returns flights affected and not affected by security delay over individual months
@app.route("/CSecuritymonthDelay", methods=["GET"])
def getSecuritymonthDelay(): 
    month = request.args.get('month')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s'" % month)
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)
    cur.execute("SELECT COUNT(*) from Fact_table where SECURITY_DELAY=0.0 and Time_key IN {}".format(str(time_key_month)) )
    count_no_month_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_month)) )
    count_total_month_delay = cur.fetchall()
    count_month_delay=count_total_month_delay[0][0] - count_no_month_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered month" : count_month_delay,
                  "flights not delayed due for entered month": count_no_month_delay[0][0] })

#API returns flights affected and not affected by arrival delay over individual months
@app.route("/CArrivalmonthDelay", methods=["GET"])
def getArrivalmonthDelay(): 
    month = request.args.get('month')     
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s'" % month)
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)
    cur.execute("SELECT COUNT(*) from Fact_table where ARRIVAL_DELAY=0.0 and Time_key IN {}".format(str(time_key_month)) )
    count_no_month_delay = cur.fetchall()
   
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {}".format(str(time_key_month)) )
    count_total_month_delay = cur.fetchall()
    count_month_delay=count_total_month_delay[0][0] - count_no_month_delay[0][0]
    
    return jsonify({ "flights Delayed due for entered month" : count_month_delay,
                  "flights not delayed due for entered month": count_no_month_delay[0][0] })



############################################ END OF HOME PAGE #####################################################



########################################### MONTH YEAR DELAY #######################################################

#API takes in input as Month and returns sum of individual category and total delay
@app.route("/MonthYear", methods=["GET"])
def getsumDelayMonthYear():  
    month = request.args.get('month')   
    year = request.args.get('year')
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Time_key from Time_dim where MONTH='%s' and YEAR='%s'" % (month,year))
    time_key_month = cur.fetchall()
    time_key_month =  tuple(i[0] for i in time_key_month)  
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY),COUNT(*) from Fact_table where Time_key IN {} and (ARRIVAL_DELAY!=0.0 or DEPARTURE_DELAY!=0.0 or SECURITY_DELAY!=0.0 or WEATHER_DELAY!=0.0)" .format(str(time_key_month)))
    row_delay_sum = cur.fetchall()
    cur.execute("SELECT COUNT(*) from Fact_table where Time_key IN {} " .format(str(time_key_month)))
    row_delay_total = cur.fetchall()
    print(row_delay_sum[0][0],"  ",row_delay_sum[0][1]," ",row_delay_sum[0][2])
    return jsonify({"SECURITY_DELAY":row_delay_sum[0][0],
                    "DEPARTURE_DELAY":row_delay_sum[0][1],
                    "WEATHER_DELAY":row_delay_sum[0][2],
                    "ARRIVAL_DELAY":row_delay_sum[0][3],
                    "TOTAL_DELAY":(row_delay_sum[0][0]+row_delay_sum[0][1]+row_delay_sum[0][2]+row_delay_sum[0][3]),
                    "Count of delay on the provided month and year with delay":row_delay_sum[0][4],
                    "Count of delay on the provided month and year without delay":row_delay_total[0][0]-row_delay_sum[0][4]
                    })

###############################################################################################################################################################################
    

################################################## AIRPORT,FLIGHT,YEAR DELAY ##################################################################################################
    
#given airport,flight,month and year the API return all the time keys 
@app.route("/AFlightMonth", methods=["GET"])
def getTimeKeyMonth():
    airport = request.args.get('airport')
    flight = request.args.get('flight')
    year = request.args.get('year')
    month = request.args.get('month')
    flight_key,airport_key,IATA_CODE=airportFlight(airport,flight)
    fact_time_key = getTimeKeyFact(flight_key,airport_key)
    dim_time_key =  getTimeKeyDimMonth(year,month)
    
    time_key_month=set(dim_time_key).intersection(fact_time_key)
    time_key_month =  tuple(i[0] for i in time_key_month)
    print(flight_key,airport_key,time_key_month)
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY) from Fact_table where Airport_key={} and Flight_key={} and Time_key IN {} ". format(airport_key,flight_key,str(time_key_month)))
    row_delay_total = cur.fetchall()
    return jsonify(row_delay_total)

#################################################################################################################


def getCityIdBasedOnState(state):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Location_key from Location_dim where STATE= '%s'" % state)
    rows_city = cur.fetchall()
    loc_key=rows_city
    loc_key =  tuple(i[0] for i in loc_key)
    return loc_key

def getLocIdBasedOnCity(city):
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT Location_key from Location_dim where CITY= '%s'" % city)
    rows_city = cur.fetchall()
    loc_key=rows_city
    loc_key =  tuple(i[0] for i in loc_key)
    return loc_key

#input state gives sum of delay across all airpot in state and total delay 
@app.route("/state", methods=["GET"])
def getCityFlight():
    #airport = request.args.get('airport')
    #flight = request.args.get('flight')
    state = request.args.get('state')
    loc_key = getCityIdBasedOnState(state)
    #flight_key,airport_key,IATA_CODE=airportFlight(airport,flight)
    #fact_time_key = getTimeKeyFact(flight_key,airport_key)
    # dim_time_key =  getTimeKeyDimMonth(year,month)
    
    """" time_key_month=set(dim_time_key).intersection(fact_time_key)
    time_key_month =  tuple(i[0] for i in time_key_month)
    print(flight_key,airport_key,time_key_month)"""
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY),COUNT(*) from Fact_table where Location_key IN {} ". format(str(loc_key)))
    row_delay_state = cur.fetchall()
    #print(row_delay_state)
    return jsonify({"SECURITY_DELAY":row_delay_state[0][0],
                    "DEPARTURE_DELAY":row_delay_state[0][1],
                    "WEATHER_DELAY":row_delay_state[0][2],
                    "ARRIVAL_DELAY":row_delay_state[0][3],
                    "TOTAL_DELAY":(row_delay_state[0][0]+row_delay_state[0][1]+row_delay_state[0][2]+row_delay_state[0][3]),
                    "COUNT DELAY": row_delay_state[0][4] })

#input state gives sum of delay across all airpot in state and total delay 
@app.route("/cityDelay", methods=["GET"])
def getCityFlightDelay():
    #airport = request.args.get('airport')
    #flight = request.args.get('flight')
    city = request.args.get('city')
    loc_key = getLocIdBasedOnCity(city)
    #flight_key,airport_key,IATA_CODE=airportFlight(airport,flight)
    #fact_time_key = getTimeKeyFact(flight_key,airport_key)
    # dim_time_key =  getTimeKeyDimMonth(year,month)
    
    """" time_key_month=set(dim_time_key).intersection(fact_time_key)
    time_key_month =  tuple(i[0] for i in time_key_month)
    print(flight_key,airport_key,time_key_month)"""
    conn=add_user()
    cur = conn.cursor()
    cur.execute("SELECT SUM(SECURITY_DELAY),SUM(DEPARTURE_DELAY),SUM(WEATHER_DELAY),SUM(ARRIVAL_DELAY),COUNT(*) from Fact_table where Location_key = '%s' " % (loc_key))
    row_delay_state = cur.fetchall()
    #print(row_delay_state)
    return jsonify({"SECURITY_DELAY":row_delay_state[0][0],
                    "DEPARTURE_DELAY":row_delay_state[0][1],
                    "WEATHER_DELAY":row_delay_state[0][2],
                    "ARRIVAL_DELAY":row_delay_state[0][3],
                    "TOTAL_DELAY":(row_delay_state[0][0]+row_delay_state[0][1]+row_delay_state[0][2]+row_delay_state[0][3]),
                    "COUNT DELAY": row_delay_state[0][4] })



def add_user():
    #connecting to flight database
    database_flight = r"C:\Users\I516398\Downloads\DB-New1\Flight_DW.db"
    conn_flight = create_connection(database_flight)
    return conn_flight


if __name__ == '__main__':
    app.run(debug=True)