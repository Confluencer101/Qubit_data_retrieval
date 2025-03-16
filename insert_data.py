import os
import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Load MongoDB connection URI
# Load .env variables
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

try:
    client = MongoClient(mongo_uri)  # 5 sec timeout
    db = client["quant_data"]
    collection = db["news_articles"]

    # Check connection
    print(client.server_info())  # Should print server details

except Exception as e:
    print("MongoDB connection error:", e)

# Sample data
sample_data = [
    {
        "time_object": {
            "timestamp": datetime.datetime.fromisoformat("2025-03-10T14:30:00Z"),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "Bloomberg",
            "title": "Apple Stock Surges After Strong Earnings Report",
            "tickers": "AAPL",
            "author": "John Doe",
            "description": "Apple reported better-than-expected quarterly earnings, driven by strong iPhone sales and growth in its services segment.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": datetime.datetime.fromisoformat("2025-03-09T12:00:00Z"),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "CNBC",
            "title": "Apple Faces Regulatory Scrutiny Over App Store Policies",
            "tickers": "AAPL",
            "author": "Jane Doe",
            "description": "Regulators in the EU and US are investigating Apple's App Store policies for potential anti-competitive practices.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": datetime.datetime.fromisoformat("2025-03-08T08:45:00Z"),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "TechCrunch",
            "title": "Apple Announces New AI Features for iPhones and MacBooks",
            "tickers": "AAPL",
            "author": "John Doe",
            "description": "Apple has unveiled a suite of AI-powered features, including enhanced Siri capabilities and real-time language translation.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": datetime.datetime.fromisoformat("2025-03-07T15:20:00Z"),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "Reuters",
            "title": "Apple Stock Declines Amid Supply Chain Concerns",
            "tickers": "AAPL",
            "author": "Jane Doe",
            "description": "Apple's stock dipped as investors reacted to reports of supply chain disruptions affecting iPhone production in China.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": datetime.datetime.fromisoformat("2025-03-06T10:15:00Z"),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "Yahoo Financeg",
            "title": "Warren Buffett Increases Stake in Apple",
            "tickers": "AAPL",
            "author": "James Doe",
            "description": "Berkshire Hathaway has significantly increased its investment in Apple, reinforcing its confidence in the company's future.",
            "url": "https://doesnotexist.com"
        }
    },
]

# Insert data into MongoDB
insert_result = collection.insert_many(sample_data)
print(f"Inserted {len(insert_result.inserted_ids)} documents.")
