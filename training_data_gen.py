"""
Generating training data for stocks for the last 5 years using Yahoo Finance API
"""
from yahoo_finance import Share

import argparse
import csv
import datetime
from multiprocessing import Pool
import json
import threading

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--symbols', help='Ticker Symbols list')
parser.add_argument('--output', help='Output File')
args = parser.parse_args()
SymbolData = {}
f = open(args.output, "w")
lock = threading.Lock()

def get_symbols():
  with open(args.symbols, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
    symbols = [ line[0] for line in list(reader)]
  return symbols
  
def symbol_data(symbol):
  print "Getting data for {0}".format(symbol)
  share = Share(symbol)
  num_days = 5 * 365
  end_date = datetime.datetime.today()
  start_date = end_date - datetime.timedelta(days=num_days)
  t = {'symbol': symbol, 'data': share.get_historical(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))}
  lock.acquire() 
  print >> f,t
  lock.release()

symbols = get_symbols()
#symbols = symbols[:50]
p = Pool(50)
p.map(symbol_data, symbols)
#json.dump(t, f)
f.close()