import robin_stocks as rs
import robin_stocks.robinhood as rh
import os

username = os.getenv("RH_USERNAME")
password = os.getenv("RH_PASSWORD")

login = rh.login(username, password)

my_stocks = rh.build_holdings()

print(my_stocks)

print(username)
print(password)