import os
from pymongo import MongoClient
import datetime
import pytz
from dotenv import load_dotenv

# Load MongoDB connection URI

load_dotenv()  # Load .env variables
mongo_uri = os.getenv("MONGO_URI")  # Ensure this is correct

try:
    client = MongoClient(mongo_uri)  # 5 sec timeout
    db = client["qubit_database"]  # Replace with your actual DB name
    collection = db["stocks"]

    # Check connection
    print(client.server_info())  # Should print server details

except Exception as e:
    print("MongoDB connection error:", e)

# Sample data
sample_data = [
      {
        "time_object": {
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-10", "%Y-%m-%d")),
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
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-09", "%Y-%m-%d")),
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
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-08", "%Y-%m-%d")),  # Fixed duplicate key
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
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-07", "%Y-%m-%d")),
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
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-06", "%Y-%m-%d")),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "Yahoo Finance",
            "title": "Warren Buffett Increases Stake in Apple",
            "tickers": "AAPL",
            "author": "James Doe",
            "description": "Berkshire Hathaway has significantly increased its investment in Apple, reinforcing its confidence in the company's future.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-10", "%Y-%m-%d")),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "Bloomberg",
            "title": "Tesla Stock Surges After Strong Earnings Report",
            "tickers": "TESLA",
            "author": "Jane Doe",
            "description": "Tesla reported better-than-expected quarterly earnings, driven by strong iPhone sales and growth in its services segment.",
            "url": "https://doesnotexist.com"
        }
    },
    {
        "time_object": {
            "timestamp": pytz.utc.localize(datetime.datetime.strptime("2025-03-09", "%Y-%m-%d")),
            "duration": None,
            "duration_unit": None,
            "timezone": "UTC"
        },
        "event_type": "News article",
        "attribute": {
            "publisher": "CNBC",
            "title": "Tesla Faces Regulatory Scrutiny over Policies",
            "tickers": "TESLA",
            "author": "Jane Doe",
            "description": "Regulators in the EU and US are investigating Tesla policies for potential anti-competitive practices.",
            "url": "https://doesnotexist.com"
        }
    }
]

# Insert data into MongoDB
insert_result = collection.insert_many(sample_data)
print(f"Inserted {len(insert_result.inserted_ids)} documents.")
