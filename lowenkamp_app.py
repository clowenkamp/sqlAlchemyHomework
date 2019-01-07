import datetime as dt
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#-----------------------------------------------
# Flask App Setup                              |
#-----------------------------------------------






app = Flask(__name__)

@app.route("/")
def welcome():
    precip_df = pd.read_sql("SELECT date, prcp FROM Measurement", con=engine, columns=[["date"],["prcp"]])
    precip_df["date"] = pd.to_datetime(precip_df["date"],format="%Y-%m-%d", errors="coerce")
    max_date = str(precip_df["date"].max().date()-dt.timedelta(days=1))
    min_date = str(precip_df["date"].max().date()-dt.timedelta(days=366))
    return (
        f"<H3>Welcome to the Hawaii Climate Analysis API!<br/><br />"
        f"<b>Avalable Routes:<br/></b>"
        f"<a href='/api/v1.0/precipitation'>/api/v1.0/precipitation</a><br/>"
        f"<a href='/api/v1.0/stations'>/api/v1.0/stations</a><br/>"
        f"<a href='/api/v1.0/tobs'>/api/v1.0/tobs</a><br/>"
        f"<a href='/api/v1.0/temp/"+min_date+"'>/api/v1.0/temp-range/</a>start<br />"
        f"<a href='/api/v1.0/temp-range/"+min_date+"/"+max_date+"'>api/v1.0/temp-range/</a>start/end"
        f"<H3>You can alter date parameters for the last two routes in the web address to analyze different data<br/><br />"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    precip_df = pd.read_sql("SELECT date, prcp FROM Measurement", con=engine, columns=[["date"],["prcp"]])
    precip_df["date"] = pd.to_datetime(precip_df["date"],format="%Y-%m-%d", errors="coerce")
    max_date = str(precip_df["date"].max().date()-dt.timedelta(days=1))
    min_date = str(precip_df["date"].max().date()-dt.timedelta(days=366))
    prcp_str = "SELECT date, prcp FROM Measurement WHERE date >'"+str(min_date)+"' AND date <='"+str(max_date)+"'  ORDER BY date ASC"
    precipitation = pd.read_sql(prcp_str, con=engine, columns=[["prcp"],["date"]])
    

    return precipitation.to_json(orient="records")

@app.route("/api/v1.0/stations")
def stations():
    stat_str = "SELECT station FROM station"
    active_df = pd.read_sql(stat_str, con=engine, columns=[["Station"]])
    active_df.set_index("station")
    return active_df.to_json(orient="records")


@app.route("/api/v1.0/tobs")
def temp_monthly():
    precip_df = pd.read_sql("SELECT date, prcp FROM Measurement", con=engine, columns=[["date"],["prcp"]])
    precip_df["date"] = pd.to_datetime(precip_df["date"],format="%Y-%m-%d", errors="coerce")
    max_date = str(precip_df["date"].max().date()-dt.timedelta(days=1))
    min_date = str(precip_df["date"].max().date()-dt.timedelta(days=366))
    tobs_str= "SELECT date, tobs FROM Measurement WHERE date >'"+str(min_date)+"' AND date <='"+str(max_date)+"'  ORDER BY date ASC"
    tobs_df = pd.read_sql(tobs_str, con=engine, columns=[["date"],["tobs"]])

    return tobs_df.to_json(orient="records")


@app.route("/api/v1.0/temp/<start>")
def stats(start=None):
    no_e_str = "SELECT MIN(tobs) Min, AVG(tobs) Avg, MAX(tobs) Max FROM Measurement WHERE date >'"+start+"' ORDER BY date ASC"
    st_df = pd.read_sql(no_e_str, con=engine, columns=[["date"],["tobs"]])
    return st_df.to_json(orient="records")


@app.route("/api/v1.0/temp-range/<start>/<end>")
def stat_range(start=None, end=None):
    precip_df = pd.read_sql("SELECT date, prcp FROM Measurement", con=engine, columns=[["date"],["prcp"]])
    precip_df["date"] = pd.to_datetime(precip_df["date"],format="%Y-%m-%d", errors="coerce")
    max_date = str(precip_df["date"].max().date()-dt.timedelta(days=1))
    min_date = str(precip_df["date"].max().date()-dt.timedelta(days=366))
    se_str = "SELECT MIN(tobs) Min, AVG(tobs) Avg, MAX(tobs) Max FROM Measurement WHERE date >'" + start + "' AND date <='" + end + "' ORDER BY date ASC"
    ed_df = pd.read_sql(se_str, con=engine, columns=[["date"],["tobs"]])
    return ed_df.to_json(orient="records")


if __name__ == '__main__':
    app.run(debug=True)