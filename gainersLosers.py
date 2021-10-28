import numpy as np
import pandas as pd
from pandas.core.indexes import period
import yfinance as yf
from datetime import date, datetime, timedelta
import os
import time
import requests
from datawrapper import Datawrapper

ACCESS_TOKEN = os.getenv('DW_API_KEY')

dw = Datawrapper(access_token=ACCESS_TOKEN)

def updateChart(dw_chart_id, dataSet, updateDate, dw_api_key):
    dw.add_data(
    chart_id=dw_chart_id,
    data=dataSet
    )

    time.sleep(2)

    headers = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Authorization": "Bearer " + dw_api_key
    }

    response = requests.request(method="PATCH", 
                                url="https://api.datawrapper.de/v3/charts/" + dw_chart_id, 
                                json={"metadata": {
                                        "annotate": {
                                            "notes": "Data from Yahoo Finance. Updated " + fileDate
                                    }
                                }},
                                headers=headers)

    response.raise_for_status()

    time.sleep(2)

    dw.publish_chart(chart_id=dw_chart_id)


# table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
# sAndP = table[0]

# sAndPRef = sAndP[['Symbol', 'Security']]
# sAndPRef.rename(columns={"Symbol":"Ticker", "Security":"Company Name"}, inplace=True)

sAndP = pd.read_csv('S&P500.csv', names=['Symbol', 'Security'], sep=",",
                    dtype={'Symbol':str, 'Security':str}, skiprows=1)
sAndPRef = pd.read_csv('S&P500.csv', names=['Ticker', 'Company Name'], sep=",",
                        dtype={'Ticker':str, 'Company Name':str}, skiprows=1)

sAndP['Symbol'] = sAndP['Symbol'].str.replace('.', '-')

spTickers = ' '.join(sAndP['Symbol'])

glRaw = yf.download(
        tickers = spTickers,
        period = '2d',
        interval = '1d',
        group_by = 'ticker',
        auto_adjust = False,
        prepost = False,
        threads = True,
        proxy = None
    )

gl = glRaw.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

gl.reset_index(level=0,inplace=True)

gl.to_csv('rawYFinance.csv', index=False)
# gl = pd.read_csv('rawYFinance.csv')

maxDate = gl['Date'].max()
minDate = gl['Date'].min()

todayGL = gl[gl['Date'] == maxDate].sort_values(by='Ticker')
yesterdayGL = gl[gl['Date'] == minDate].sort_values(by='Ticker') 

todayGL.rename(columns={'Date': 'Today Date', 
                        'Adj Close': 'Today Adj Close', 
                        'Close': 'Today Close', 
                        'High': 'Today High',
                        'Low': 'Today Low',
                        'Open': 'Today Open',
                        'Volume': 'Today Volume'}, inplace=True)

yesterdayGL.rename(columns={'Date': 'Yesterday Date', 
                        'Adj Close': 'Yesterday Adj Close', 
                        'Close': 'Yesterday Close', 
                        'High': 'Yesterday High',
                        'Low': 'Yesterday Low',
                        'Open': 'Yesterday Open',
                        'Volume': 'Yesterday Volume'}, inplace=True)

comboGL = pd.merge(todayGL, yesterdayGL, on='Ticker', how='inner')

def getPctChg(New, Old):
    pctChg = (New - Old) / (Old)
    return pctChg

glPctChg = list(map(lambda x,y: getPctChg(x, y), comboGL['Today Close'],comboGL['Yesterday Close']))

comboGL.rename(columns={'Today Date': 'Date', 'Today Close': 'Close'}, inplace=True)

fullGL = comboGL[['Date', 'Ticker', 'Close']]
fullGL['1 Day Returns'] = pd.Series(glPctChg).values
fullGL['Close'] = [round(x, 2) for x in fullGL['Close']]

fullGL = fullGL.merge(sAndPRef, how='left', on='Ticker')


biggestLosers = fullGL.sort_values(by=['1 Day Returns']).iloc[0:10,:].reset_index()
biggestGainers = fullGL.sort_values(by=['1 Day Returns'], ascending=False).iloc[0:10,:].reset_index()

biggestLosers.rename(columns={'1 Day Returns':'1 Day Losses'}, inplace=True)
biggestGainers.rename(columns={'1 Day Returns':'1 Day Gains'}, inplace=True)

biggestLosers.rename(columns={'Close':'Latest Losers Price'}, inplace=True)
biggestGainers.rename(columns={'Close':'Latest Gainers Price'}, inplace=True)

biggestLosers['1 Day Losses'] = ['{0:.2f}'.format(x * 100) + '%' for x in biggestLosers['1 Day Losses']]
biggestGainers['1 Day Gains'] = ['{0:.2f}'.format(x * 100) + '%' for x in biggestGainers['1 Day Gains']]


biggestLosers['Biggest Losses'] = biggestLosers[['Company Name', 'Ticker']].agg(' '.join, axis=1)
biggestGainers['Biggest Gains'] = biggestGainers[['Company Name', 'Ticker']].agg(' '.join, axis=1)

blFinal = biggestLosers[['Biggest Losses', 'Latest Losers Price', '1 Day Losses']]
bgFinal = biggestGainers[['Biggest Gains', 'Latest Gainers Price', '1 Day Gains']]


biggestLG = pd.concat([bgFinal, blFinal], axis=1)

biggestLG.to_csv('gl.csv', index=False)

fileDate = str(datetime.today().strftime('%B %d, %Y'))

updateChart('k53KU', biggestLG, fileDate, ACCESS_TOKEN)