from flask import Flask, jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Initialize Flask app
app = Flask(__name__)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client["quant_data"]
collection = db["news_articles"]

# GET news for a specific stock (e.g., AAPL)
@app.route("/stocks/<symbol>", methods=["GET"])
def get_stock_news(symbol):
    query = {"symbol": symbol.upper()}  # Case-insensitive symbol search
    limit = request.args.get("limit", default=10, type=int)
    date = request.args.get("date")  # Optional filter by date

    if date:
        query["date"] = date  # Filter by date

    stocks = list(collection.find(query, {"_id": 0}).limit(limit))
    if not stocks:
        return jsonify({"message": f"No news found for {symbol}"}), 404
    
    return jsonify(stocks), 200

if __name__ == "__main__":
    app.run(debug=True)
