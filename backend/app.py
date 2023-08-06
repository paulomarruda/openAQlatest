from flask import Flask 
from models import OpenAQ

openAQ = OpenAQ()
openAQ.connect()

app = Flask(__name__)
@app.route("/")
def root():
    return {
        "avaiable" : ["locations", "parameters", "latestMeasurements"]
    }
@app.route("/locations")
def locations():
    return openAQ.locations
@app.route("/parameters")
def parameters():
    return openAQ.parameters
@app.route("/latestMeasurements")
def measurements():
    return openAQ.latestMeasurements
