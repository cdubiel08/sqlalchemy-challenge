from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_

app = Flask(__name__)

engine = create_engine("sqlite:///hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

Measurements = Base.classes.measurement
Stations = Base.classes.station


@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App! <br/><br/>"
        f"<br/>"
        f"Available Routes: <br/>"
        f"/api/v1.0/precipitation <br/>"
        f"/api/v1.0/stations <br/>"
        f"/api/v1.0/tobs <br/>"
        f"/api/v1.0/[start]"
        f"/api/v1.0/[start]/[end]"
    )

@app.route("/api/v1.0/precipitation")
def prcp():

    session = Session(engine)
    precipitation = session.query(Measurements.date, Measurements.prcp)
    
    session.close()
    
    prcp_dict = {}
    for row in precipitation:
        prcp_dict[row[0]] = row[1]

    return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():

    session = Session(engine)
    query = session.query(Stations.id, Stations.station).all()

    session.close()

    stations = {}
    for i in range(len(query)):
        stations[query[i][0]] = query[i][1]
    
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def tobs():
    prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    session = Session(engine)
    query1 = session.query(Measurements.station, func.count(Measurements.station)).\
                        group_by(Measurements.station).\
                        order_by(func.count(Measurements.station).desc())

    station = query1[0][0]
    
    query2 = session.query(Measurements.date, Measurements.tobs).\
                filter(Measurements.station == station).\
                filter(Measurements.date >= prev_year).all()
    
    session.close()

    return jsonify(query2) 

@app.route("/api/v1.0/<start>")
def avg_start(start):

    session = Session(engine)

    query = session.query(func.min(Measurements.tobs),
                      func.max(Measurements.tobs),
                      func.avg(Measurements.tobs)).\
                      filter(Measurements.date >= start).all()
    
    session.close()

    summary = {
            "date_start": start,
            "min": query[0][0],
            "max": query[0][1],
            "avg": query[0][2]
    }


    return jsonify(summary)

@app.route("/api/v1.0/<start>/<end>")
def avg_start_end(start=None, end=None):

    session = Session(engine)

    query = session.query(func.min(Measurements.tobs),
                      func.max(Measurements.tobs),
                      func.avg(Measurements.tobs)).\
                      filter(and_(Measurements.date >= start, Measurements.date <= end)).all()
    
    session.close()

    summary = {
            "date_start": start,
            "date_end": end,
            "min": query[0][0],
            "max": query[0][1],
            "avg": query[0][2]
    }

    return jsonify(summary)



    

if __name__ == "__main__":
    app.run(debug=True)
