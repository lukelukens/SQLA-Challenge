from flask import Flask, render_template, jsonify, request
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import numpy as np
import pandas as pd
import datetime as dt
import requests

engine = create_engine("sqlite:///../../GWU-ARL-DATA-PT-12-2019-U-C/02-Homework/10-Advanced-Data-Storage-and-Retrieval/Instructions/Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)

app = Flask(__name__)

@app.route("/")
def home():
    lid = request.args.get('lid')

    return render_template('weather.html')

#   * Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
#   * Return the JSON representation of your dictionary.
@app.route("/api/v1.0/precipitation")
def precipitation():
    year_df = pd.read_sql('select * from measurement', con=engine)
    print(year_df)
    date_key={}
    for _, row in year_df.iterrows():
        date_key[row.date]=row.prcp
    return jsonify(date_key)
    # return "daddy"


#   * Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def stations():
    station_df=pd.read_sql('select * from station', con=engine)
    return jsonify(list(station_df.station))

# #   * query for the dates and temperature observations from a year from the last data point.
# #   * Return a JSON list of Temperature Observations (tobs) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    rain = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date > last_year).\
    order_by(Measurement.date).all()
    rain_totals = []
    for result in rain:
        row = {}
        row["date"] = rain[0]
        row["prcp"] = rain[1]
        rain_totals.append(row)
    return jsonify(rain_totals)

# #   * Return a JSON list of the minimum temperature, the average temperature,  
# #   * and the max temperature for all dates greater than and equal to the start date.
@app.route("/api/v1.0/", methods=['POST'])
def start():
    start = dt.datetime.strptime(request.form.get('start_start'), "%m/%d/%y")
    
    try:
        end = dt.datetime.strptime(request.form.get('startend_end'), "%m/%d/%y")
        rain = session.query(Measurement.date, Measurement.prcp).\
            filter(Measurement.date > start).\
                filter(Measurement.date < end).all()
        
    except:
        print(f"No end date given. Returning data from {start} to last date.")
        rain = session.query(Measurement.date, Measurement.prcp).\
            filter(Measurement.date > start).all()
        
    total = 0
    min = 0
    max = 0
    counter=0

    for pair in rain:
        try: 
            float_pair = float(pair[1])
            total += float_pair
            counter+=1
            if pair[1] > max: max = float_pair
            elif pair[1] < min: min = float_pair
        except TypeError: 
            pass
    return jsonify({"Minimum":min, "Maximum": max, "Average":total/counter})


if __name__ == "__main__": 
    app.run(debug=True, port='5000')