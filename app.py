from flask import Flask, jsonify, request, render_template
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import requests
import datetime
import pytz

# Load environment variables
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Initialize Flask app
app = Flask(__name__)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client["qubit_database"]  # Database name
collection = db["stocks"]  # Collection name

API_BASE_URL = "http://127.0.0.1:5000/stocks"

@app.route('/', methods=['GET', 'POST'])
def index():
    news = None
    error = None

    if request.method == 'POST':
        symbol = request.form.get('symbol')
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')

        if not symbol:
            error = "Stock symbol is required."
        else:
            params = {"symbol": symbol.upper()}

            if start_date and end_date:
                url = f"{API_BASE_URL}/{symbol}/range"
                params['start_date'] = start_date
                params['end_date'] = end_date
            else:
                url = f"{API_BASE_URL}/{symbol}"

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                news = response.json()
            except requests.exceptions.RequestException as e:
                error = f"Error fetching stock news: {str(e)}"

    return render_template('index.html', news=news, error=error)

# GET latest news for a specific stock (no date filtering)
@app.route("/stocks/<symbol>", methods=["GET"])
def get_stock_news(symbol):
    query = {"attribute.tickers": symbol.upper()}
    limit = request.args.get("limit", default=10, type=int)

    stocks = list(collection.find(query, {"_id": 0}).sort("time_object.timestamp", -1).limit(limit))

    if not stocks:
        return jsonify({"message": f"No news found for {symbol}"}), 404

    return jsonify(stocks), 200

# GET news for a specific stock within a date range
@app.route("/stocks/<symbol>/range", methods=["GET"])
def get_stock_news_range(symbol):
    query = {"attribute.tickers": symbol.upper()}
    limit = request.args.get("limit", default=10, type=int)

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    if start_date or end_date:
        date_filter = {}
        if start_date:
            start_dt = pytz.utc.localize(datetime.datetime.strptime(start_date, "%Y-%m-%d"))
            date_filter["$gte"] = start_dt
        if end_date:
            end_dt = pytz.utc.localize(datetime.datetime.strptime(end_date, "%Y-%m-%d"))
            date_filter["$lte"] = end_dt
        query["time_object.timestamp"] = date_filter

    stocks = list(collection.find(query, {"_id": 0}).sort("time_object.timestamp", -1).limit(limit))

    if not stocks:
        return jsonify({"message": f"No news found for {symbol} in the given date range"}), 404

    return jsonify(stocks), 200

if __name__ == "__main__":
    app.run(debug=True)