from flask import Flask, jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

# Load environment variables
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Initialize Flask app
app = Flask(__name__)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client["quant_data"]

# GET news for a specific stock (e.g., AAPL)
@app.route("/stocks/<symbol>", methods=["GET"])
def get_stock_news(symbol):
    collection = db["news_articles"]
    query = {"symbol": symbol.upper()}  # Case-insensitive symbol search
    limit = request.args.get("limit", default=10, type=int)
    date = request.args.get("date")  # Optional filter by date

    if date:
        query["date"] = date  # Filter by date

    stocks = list(collection.find(query, {"_id": 0}).limit(limit))
    if not stocks:
        return jsonify({"message": f"No news found for {symbol}"}), 404
    
    return jsonify(stocks), 200

# GET news articles for a specific company
@app.route("/articles/<source_name>/<company>", methods=["GET"])
def get_stock_news(source_name, company):
    limit = request.args.get("limit", default=10, type=int)
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    publisher = request.args.get("publisher")
    author = request.args.get("author")
    exclude_publisher = request.args.get("exclude_publisher")
    exclude_author = request.args.get("exclude_author")

    # There is a separate collection for each source, since we must reconstruct
    # the ADAGE (including the data_source field) when retreiving data
    if (source_name == "news_api_org"):
        collection = db["news_api"]
    else:
        # news_articles is the default collection to retrieve data from
        collection = db["news_articles"]

    # Use a regex to search for the company name in article titles and descriptions
    query = {"$or": []}
    query["$or"].append({"attribute.title": {"$regex": company, "$options": "i"}})
    query["$or"].append({"attribute.description": {"$regex": company, "$options": "i"}})

    if start_date:
        # Only return articles written AFTER a certain date
        query["time_object.timestamp"] = {"$gte": datetime.fromisoformat(start_date)}

    if end_date:
        # Only return articles written BEFORE a certain date
        query["time_object.timestamp"] = {"$lte": datetime.fromisoformat(end_date)}

    if publisher:
        if exclude_publisher:
            # Exclude articles published by a particular site
            query["attribute.publisher"] = {"$ne": {"$regex": publisher, "$options": "i"}}
        else:
            # Only return articles published by a particular site
            query["attribute.publisher"] = {"$regex": publisher, "$options": "i"}
    
    if author:
        if exclude_author:
            # Exclude articles published by a particular author
            query["attribute.author"] = {"$ne": {"$regex": author, "$options": "i"}}
        else:
            # Only return articles published by a particular author
            query["attribute.author"] = {"$regex": author, "$options": "i"}

    articles = list(collection.find(query, {"_id": 0}).limit(limit))
    if not articles:
        return jsonify({"message": f"No news found for {company}"}), 404
    
    adage_data = formattingADAGE(articles, datetime.now(), source_name)
    
    return jsonify(adage_data), 200

# Function which reconstructs retreived data in ADAGE 3.0 format    
def formattingADAGE(data, time_now, source_name):
    adage_data = {
        "data_source": str,
        "dataset_type": str,
        "dataset_id": str,
        "time_object": {
            "timestamp": datetime,
            "timezone": "UTC",
        },
        "events": []
    }
    if (source_name == "news_api_org"):
        adage_data["data_source"] = source_name
        adage_data["dataset_type"] = "News data"
        adage_data["dataset_id"] = "1"
        adage_data["time_object"]["timestamp"] = time_now

        # Assumes the data variable is a list, and in the correct format for
        # ADAGE 3.0's events[] list
        adage_data["events"] = data
    return adage_data

# Function that finds the newest and oldest article in the database
# (overall, or for a particular company)
def newest_oldest_article(source_name, company):
    if (source_name == "news_api_org"):
        collection = db["news_api"]
    else:
        # news_articles is the default collection to retrieve data from
        collection = db["news_articles"]

    query = []

    if company:
        # If the company is not specified, the newest article overall will be found
        match_company = {"$or": []}
        match_company["$or"].append({"attribute.title": {"$regex": company, "$options": "i"}})
        match_company["$or"].append({"attribute.description": {"$regex": company, "$options": "i"}})

        query.append({"$match": match_company})

    find_newest_oldest = {}
    find_newest_oldest["_id"] = "null"
    find_newest_oldest["newest"] = {"$max": "$time_object.timestamp"}
    find_newest_oldest["oldest"] = {"$min": "$time_object.timestamp"}

    query.append({"$group": find_newest_oldest})

    query_result = dict(collection.aggregate(find_newest_oldest))

    newest = query_result["newest"]
    oldest = query_result["oldest"]

    return newest, oldest

if __name__ == "__main__":
    app.run(debug=True)
