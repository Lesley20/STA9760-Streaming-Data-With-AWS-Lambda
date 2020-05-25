import boto3
import os
import subprocess
import sys

import json

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf
import pandas as pd

def lambda_handler(event, context):

    data = yf.download(
            tickers = "FB SHOP BYND NFLX PINS SQ TTD OKTA SNAP DDOG",
            start = "2020-05-14",
            end = "2020-05-15",
            interval = "1m",

            # group by ticker (to access via data['SPY'])
            # (optional, default is 'column')
            group_by = 'ticker',

            # adjust all OHLC automatically
            # (optional, default is False)
            auto_adjust = True,
            
            # download pre/post regular market hours data
            # (optional, default is False)
            # prepost = False,

            # use threads for mass downloading? (True/False/Integer)
            # (optional, default is True)
            threads = True,

            # proxy URL scheme use use when downloading?
            # (optional, default is None)
            proxy = None
    )


    # stocks we want to obtain 
    stocks = ["FB","SHOP", "BYND", "NFLX", "PINS", "SQ", "TTD", "OKTA", "SNAP", "DDOG"]

    results = []

    for name in stocks:
        
        for row_index, row in data[name].iterrows():
            
            results.append({
                "high": row['High'],
                "low": row['Low'],
                "ts": row_index.strftime("%Y-%m-%d %H:%M:%S"),
                "name": name
            })
            
    #initialize boto3 client
    fh = boto3.client("firehose", "us-east-2")
    
    # convert it to json
    #js_file = json.dumps(results) 
    
    for res in results:
        as_jsonstr = json.dumps(res)
        
        # this actually pushed to our firehose datastream
        # we must "encode" in order to convert it into the
        # bytes datatype as all of AWS libs operate over
        # bytes not strings
    
        fh.put_record(
            DeliveryStreamName="project3-delivery-stream",
            Record={"Data": as_jsonstr.encode('utf-8')})
        
    # return
    return{
        'statusCode': 200,
        'body': json.dumps(f'Done! Recorded: {as_jsonstr}')
    }