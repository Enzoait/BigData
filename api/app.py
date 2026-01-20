import flask
import pymongo
from flows.config import MONGO_URI, DATABASE_NAME
from datetime import datetime

app = flask.Flask(__name__)


def _sanitize_docs(docs: list) -> list:
    """Convertit les datetimes en ISO et retire le champ interne _written_at pour JSON."""
    sanitized = []
    for doc in docs:
        new = {}
        for k, v in doc.items():
            if k == '_written_at':
                continue
            if isinstance(v, datetime):
                new[k] = v.isoformat()
            else:
                new[k] = v
        sanitized.append(new)
    return sanitized

@app.route("/")
def home():
    return "Big Data ETL API is running."

@app.route("/agg_daily")
def agg_daily():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["agg_daily"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/agg_monthly")
def agg_monthly():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["agg_monthly"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/dim_client")
def dim_client():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["dim_client"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/dim_date")
def dim_date():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["dim_date"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/fact_purchase")
def fact_purchase():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["fact_purchases"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/kpis")
def kpis():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["kpis"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/revenue_by_country")
def revenue_by_country():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["revenue_by_country"]
    data = list(collection.find({}, {"_id": 0}))
    data = _sanitize_docs(data)
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/stats_by_product")
def stats_by_product():
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[DATABASE_NAME]
    collection = mongo_db["stats_by_product"]
    data = list(collection.find({}, {"_id": 0}))
    latest = collection.find_one(sort=[("_written_at", -1)], projection={"_written_at": 1})
    latest_ts = None
    if latest and "_written_at" in latest:
        latest_ts = latest["_written_at"].isoformat()
    return flask.jsonify({"data": data, "latest_written_at": latest_ts, "api_time": datetime.utcnow().isoformat()})

@app.route("/test")
def test():
    return "API is working fine."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)