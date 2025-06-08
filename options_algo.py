import robin_stocks as rs
import robin_stocks.robinhood as rh
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

username = os.getenv("RH_USERNAME")
password = os.getenv("RH_PASSWORD")

login = rh.login(username, password)

my_stocks = rh.build_holdings()

print(my_stocks)