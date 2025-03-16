import os
import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Load MongoDB connection URI

load_dotenv()  # Load .env variables
mongo_uri = os.getenv("MONGO_URI")  # Ensure this is correct

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
        "tickers": "AAPL",
        "title": "Apple Stock Surges After Strong Earnings Report",
        "content": "Apple reported better-than-expected quarterly earnings, driven by strong iPhone sales and growth in its services segment.",
        "publishedAt": "2025-03-10T14:30:00Z",
        "source": "Bloomberg"
    },
    {
        "tickers": "AAPL",
        "title": "Apple Faces Regulatory Scrutiny Over App Store Policies",
        "content": "Regulators in the EU and US are investigating Apple's App Store policies for potential anti-competitive practices.",
        "publishedAt": "2025-03-09T12:00:00Z",
        "source": "CNBC"
    },
    {
        "tickers": "AAPL",
        "title": "Apple Announces New AI Features for iPhones and MacBooks",
        "content": "Apple has unveiled a suite of AI-powered features, including enhanced Siri capabilities and real-time language translation.",
        "publishedAt": "2025-03-08T08:45:00Z",
        "source": "TechCrunch"
    },
    {
        "tickers": "AAPL",
        "title": "Apple Stock Declines Amid Supply Chain Concerns",
        "content": "Apple's stock dipped as investors reacted to reports of supply chain disruptions affecting iPhone production in China.",
        "publishedAt": "2025-03-07T15:20:00Z",
        "source": "Reuters"
    },
    {
        "tickers": "AAPL",
        "title": "Warren Buffett Increases Stake in Apple",
        "content": "Berkshire Hathaway has significantly increased its investment in Apple, reinforcing its confidence in the company's future.",
        "publishedAt": "2025-03-06T10:15:00Z",
        "source": "Yahoo Finance"
    }
]

# Need to run the following datetime command in order for publishedAt to actually
# be inserted in datetime format instead of just a string
sample_data_2 = [
    {
        "tickers": "AAPL",
        "title": "Apple Stock Surges After Strong Earnings Report",
        "content": "Apple reported better-than-expected quarterly earnings, driven by strong iPhone sales and growth in its services segment.",
        "publishedAt": datetime.datetime.fromisoformat("2025-03-10T14:30:00Z"),
        "source": "Bloomberg"
    }
]

# Insert data into MongoDB
insert_result = collection.insert_many(sample_data_2)
print(f"Inserted {len(insert_result.inserted_ids)} documents.")
