import csv
import requests
import json
import itertools

yahoo_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}?range=5y&includePrePost=false&interval=1d&corsDomain=finance.yahoo.com&.tsrc=finance'

def parse_stock_data(data):
  parsed_data = json.loads(data)
  stock_data = parsed_data['chart']['result'][0]
  symbol = stock_data['meta']['symbol']
  timestamps = stock_data['timestamp']
  closing_price = stock_data['indicators']['quote'][0]['close']
  volume = stock_data['indicators']['quote'][0]['volume']
  num_points = len(volume)
  symbols = [symbol] * num_points
  return zip(symbols, timestamps, volume, closing_price)

with open('./nasdaq_stock_symbols.txt') as f:
  nasdaq_symbols = f.readlines()

nasdaq_symbols = [x.strip() for x in nasdaq_symbols]

with open('./nyse_stock_symbols.txt') as f:
    nyse_symbols = f.readlines()

nyse_symbols = [x.strip() for x in nyse_symbols]

nasdaq_urls = [yahoo_url.format(symbol) for symbol in nasdaq_symbols]
nyse_urls = [yahoo_url.format(symbol) for symbol in nyse_symbols]

nasdaq_data = []
for url in nasdaq_urls:
  counter = 3
  data = None
  while (counter > 0 and data == None):
    try:
      data = requests.get(url)
      nasdaq_data.append(parse_stock_data(data.content))
    except:
      counter -= 1

nyse_data = []
for url in nyse_urls:
  counter = 3
  data = None
  while counter > 0 and data == None:
    try:
      data = requests.get(url)
      nyse_data.append(parse_stock_data(data.content))
    except:
      counter -= 1

file = open('./stock_data.csv', 'w')
cols = ['symbol', 'timestamp', 'volumne', 'close']
file.write(",".join(cols))
file.write("\n")
for sym_data in nasdaq_data:
  for point in sym_data:
    file.write(",".join([str(x) for x in point]))
    file.write("\n")

file.close()

data = requests.get(url)


csv_file = open('./stock_data.csv')
reader = csv.DictReader(csv_file)
csv_file_out = open('./stock_data_new.csv', 'w')

fieldnames = ['time_string', 'timestamp', 'symbol', 'volumne', 'close']
csv_writer = csv.DictWriter(csv_file_out, fieldnames=fieldnames)
csv_writer.writeheader()

for row in reader:
  row['time_string'] = datetime.datetime.fromtimestamp(int(row['timestamp'])).isoformat()
  csv_writer.writerow(row)

csv_file.close()
csv_file_out.close()
