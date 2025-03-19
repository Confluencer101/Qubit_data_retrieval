import pytest
from app import app  # Import the Flask app from your main file
from datetime import datetime
import pytz
from dateutil import parser

@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

# Test: Get stock news for a specific symbol
def test_get_stock_news(client):
    response = client.get("/stocks/AAPL")
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0  # AAPL exists in MongoDB test data
    assert data[0]["attribute"]["tickers"] == "AAPL"

# Test: Get stock news for a symbol with a date range
def test_get_stock_news_range(client):
    response = client.get("/stocks/AAPL/range?start_date=2025-03-07&end_date=2025-03-09")
    assert response.status_code == 200
    data = response.get_json()
    
    assert isinstance(data, list)
    assert len(data) > 0  # At least one result should be returned

    # Check if the returned dates fall in range
    for article in data:
        print(article["time_object"]["timestamp"]) 
        #published_date = datetime.fromisoformat(article["time_object"]["timestamp"]).replace(tzinfo=pytz.utc)
        #published_date = datetime.fromisoformat(article["time_object"]["timestamp"]).replace(tzinfo=pytz.utc)
        published_date = parser.parse(article["time_object"]["timestamp"]).replace(tzinfo=pytz.utc)
     # This should be in the correct ISO format
        assert datetime(2025, 3, 7, tzinfo=pytz.utc).date() <= published_date.date() <= datetime(2025, 3, 9, tzinfo=pytz.utc).date()

# Test: Get stock news with no results (wrong symbol)
def test_get_stock_news_not_found(client):
    response = client.get("/stocks/FAKE")
    assert response.status_code == 404
    data = response.get_json()
    assert data["message"] == "No news found for FAKE"

# Test: Get stock news with no results in a given date range
def test_get_stock_news_range_not_found(client):
    response = client.get("/stocks/AAPL/range?start_date=2020-01-01&end_date=2020-01-02")
    assert response.status_code == 404
    data = response.get_json()
    assert data["message"] == "No news found for AAPL in the given date range"






































# Load environment variables
# load_dotenv()
# mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# # Connect to local MongoDB
# client = MongoClient(mongo_uri)
# db = client["qubit_database"]
# collection = db["stocks"]

# def test_get_tesla_stock_news():
#     """Test API returns only Tesla (TESLA) stock news."""
#     response = requests.get(f"{BASE_URL}/stocks/TESLA")
    
#     # Ensure request was successful
#     assert response.status_code == 200  

#     # Parse response data
#     data = response.json()
    
#     # Ensure we got at least one Tesla stock entry
#     assert len(data) > 0, "No data returned for Tesla stocks"

#     # Ensure every returned item is for Tesla
#     assert all(item["tickers"] == "TESLA" for item in data), "Non-Tesla stock found in response"
