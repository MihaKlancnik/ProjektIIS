import requests
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import time

# MongoDB connection
MONGODB_URI = "mongodb+srv://mihaklancnik2:mihakorenjak@cluster0.vixnc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = AsyncIOMotorClient(MONGODB_URI)
db = client["Kriptobaza"]
bit_col = db["Bitcoin"]
sol_col = db["Solana"]
eth_col = db["Ethereum"]

# CoinGecko API URL
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=eur"

def fetch_crypto_prices():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error during request: {e}")
        return None

def wait_until_full_hour():
    now = datetime.utcnow()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    wait_time = (next_hour - now).total_seconds()
    print(f"Waiting {wait_time:.2f} seconds until the next full hour...")
    time.sleep(wait_time)

def save_to_mongo():
    wait_until_full_hour()
    while True:
        data = fetch_crypto_prices()
        if data:
            timestamp = datetime.utcnow()

            bitcoin_data = {"timestamp": timestamp, "price_eur": data["bitcoin"]["eur"]}
            ethereum_data = {"timestamp": timestamp, "price_eur": data["ethereum"]["eur"]}
            solana_data = {"timestamp": timestamp, "price_eur": data["solana"]["eur"]}

            bit_col.insert_one(bitcoin_data)
            eth_col.insert_one(ethereum_data)
            sol_col.insert_one(solana_data)

            print("Data saved successfully")
        else:
            print("No data to save")

        wait_until_full_hour()

save_to_mongo()