import numpy as np
import pandas as pd
from pandas.core.indexes import period
# import yfinance as yf
from datetime import date, datetime, timedelta
import os
import time
import requests
from datawrapper import Datawrapper
from yahooquery import Ticker

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

sAndPYesterday = pd.read_csv('sAndPYesterday.csv',
                             header=0, dtype={'symbol':str, 'yesterday_price':np.float64},
                             parse_dates=[2])

sAndPYesterday.yesterday_price = sAndPYesterday.yesterday_price.astype(float)

sAndP = pd.read_csv('S&P500.csv', names=['Symbol', 'Security'], sep=",",
                    dtype={'Symbol':str, 'Security':str}, skiprows=1)
sAndPRef = pd.read_csv('S&P500.csv', names=['Ticker', 'Company Name'], sep=",",
                        dtype={'Ticker':str, 'Company Name':str}, skiprows=1)

sAndP['Symbol'] = sAndP['Symbol'].str.replace('.', '-', regex=False)

spTickers = sAndP['Symbol'].to_list()

tickers = Ticker(spTickers, asynchronous=True, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
yqSP500_data = tickers.history(period='1d', interval='1d')

if isinstance(yqSP500_data, pd.DataFrame):
    yqSP500_data.reset_index(level=['symbol', 'date'], inplace=True)
    glToday = yqSP500_data[['symbol', 'date', 'close']]
    glToday.rename(columns={
        'date': 'today_date',
        'close': 'today_price'
    }, inplace=True)
    

elif isinstance(yqSP500_data, dict):
    sAndPTickers = []
    sAndPLatest = []
    # sAndPDates = []

    for i in yqSP500_data.keys():
        if isinstance(yqSP500_data[i], pd.DataFrame):
                tick = i
                sAndPTickers.append(i)
                val = yqSP500_data[i]['close'].item()
                sAndPLatest.append(val)
                # stockDate = yqSP500_data[i]['close'].index.item()
                # sAndPDates.append(stockDate)
        elif isinstance(yqSP500_data[i], dict):
                tick = i
                sAndPTickers.append(i)
                val = yqSP500_data[i]['meta']['regularMarketPrice']
                sAndPLatest.append(val)
        else:
            raise ValueError('Stock data is missing')

    # sAndPDate = list(set(sAndPDates))
    # sAndPDateFinal = sAndPDate[0]
    # sAndPDates = [sAndPDateFinal for i in range(505)]
    sAndPDate = date.today().strftime('%Y-%m-%d')
    sAndPDates = [sAndPDate] * len(sAndPTickers)

    glToday = pd.DataFrame(
        {
            'symbol': sAndPTickers,
            'today_price': sAndPLatest,
            'today_date': sAndPDates
        }
    )

    
else:
    print(f"Data returned from yahooquery is of type {type(yqSP500_data)}")





# if isinstance(yqSP500_data, pd.DataFrame):
#     print(f"The type of this object is {type(yqSP500_data)}. Writing out dataframe.")
#     yqSP500_data.to_csv(index=False)
# elif isinstance(yqSP500_data, dict):
#     print(f"The type of this object is {type(yqSP500_data)}. Writing out JSON.")
#     with open('yq_results.json', 'w') as gl:
#         json.dump(yqSP500_data, gl)
# else:
#     print(f"The type of this object is {type(yqSP500_data)}. Do something else with it.")




# gl = yqSP500_data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
# gl.reset_index(level=0,inplace=True)

# brokenSymbolsList = [sAndP['Symbol'][0:101], 
#                      sAndP['Symbol'][101:202], 
#                      sAndP['Symbol'][202:303], 
#                      sAndP['Symbol'][303:404],
#                      sAndP['Symbol'][404:505]]

# glRawList = []

# for i in range(len(brokenSymbolsList)):
#     spTickers = ' '.join(brokenSymbolsList[i])
#     glSlice = yf.download(
#         tickers = spTickers,
#         period = '1d',
#         interval = '1d',
#         group_by = 'ticker',
#         auto_adjust = False,
#         prepost = False,
#         threads = True,
#         proxy = None
#     )
#     time.sleep(61)
#     gl = glSlice.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
#     gl.reset_index(level=0,inplace=True)

#     glRawList.append(gl)

# glRaw = pd.concat(glRawList)

# if len(gl) != 505:
#     raise ValueError('Not all tickers downloaded successfully.')

# spTickers = ' '.join(sAndP['Symbol'])

# glRaw = yf.download(
#         tickers = spTickers,
#         period = '1d',
#         interval = '1d',
#         group_by = 'ticker',
#         auto_adjust = False,
#         prepost = False,
#         threads = True,
#         proxy = True
#     )

# gl = glRaw.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

# gl.reset_index(level=0,inplace=True)

# gl.to_csv('sAndPYesterday.csv', index=False)
# gl = pd.read_csv('rawYFinance.csv')

maxDate = glToday['today_date'].max()
minDate = sAndPYesterday['yesterday_date'].min()

todayGL = glToday[glToday['today_date'] == maxDate].sort_values(by='symbol')
yesterdayGL = sAndPYesterday[sAndPYesterday['yesterday_date'] == minDate].sort_values(by='symbol') 

# todayGL.rename(columns={'Date': 'Today Date', 
#                         'Adj Close': 'Today Adj Close', 
#                         'Close': 'Today Close', 
#                         'High': 'Today High',
#                         'Low': 'Today Low',
#                         'Open': 'Today Open',
#                         'Volume': 'Today Volume'}, inplace=True)

# yesterdayGL.rename(columns={'Date': 'Yesterday Date', 
#                         'Adj Close': 'Yesterday Adj Close', 
#                         'Close': 'Yesterday Close', 
#                         'High': 'Yesterday High',
#                         'Low': 'Yesterday Low',
#                         'Open': 'Yesterday Open',
#                         'Volume': 'Yesterday Volume'}, inplace=True)

comboGL = pd.merge(todayGL, yesterdayGL, on='symbol', how='inner')

def getPctChg(New, Old):
    pctChg = (New - Old) / (Old)
    return pctChg

glPctChg = list(map(lambda x,y: getPctChg(x, y), comboGL['today_price'],comboGL['yesterday_price']))

comboGLSliced = comboGL[['symbol', 'today_date', 'today_price']]
comboGLSliced.rename(columns={'today_date': 'Today Date', 'today_price':'Close', 'symbol':'Ticker'}, inplace=True)

fullGL = comboGLSliced[['Ticker', 'Close']]
fullGL['1 Day Returns'] = pd.Series(glPctChg).values
fullGL['Close'] = [round(x, 2) for x in fullGL['Close']]

fullGL = fullGL.merge(sAndPRef, how='left', on='Ticker')


biggestLosers = fullGL.sort_values(by=['1 Day Returns']).iloc[0:8,:].reset_index()
biggestGainers = fullGL.sort_values(by=['1 Day Returns'], ascending=False).iloc[0:8,:].reset_index()

biggestLosers.rename(columns={'1 Day Returns':'% Loss'}, inplace=True)
biggestGainers.rename(columns={'1 Day Returns':'% Gain'}, inplace=True)

biggestLosers.rename(columns={'Close':'Price\u061C'}, inplace=True)
biggestGainers.rename(columns={'Close':'Price'}, inplace=True)

biggestLosers['% Loss'] = ['{0:.2f}'.format(x * 100) + '%' for x in biggestLosers['% Loss']]
biggestGainers['% Gain'] = ['{0:.2f}'.format(x * 100) + '%' for x in biggestGainers['% Gain']]


biggestLosers['Biggest Losses'] = biggestLosers[['Ticker', 'Company Name']].agg(', '.join, axis=1)
biggestGainers['Biggest Gains'] = biggestGainers[['Ticker', 'Company Name']].agg(', '.join, axis=1)

blFinal = biggestLosers[['Biggest Losses', 'Price\u061C', '% Loss']]
bgFinal = biggestGainers[['Biggest Gains', 'Price', '% Gain']]


biggestLG = pd.concat([bgFinal, blFinal], axis=1)

biggestLG.to_csv('gl.csv', index=False)

fileDate = str(datetime.today().strftime('%B %d, %Y'))

updateChart('k53KU', biggestLG, fileDate, ACCESS_TOKEN)

## Rename columms then write out!!
glToday.rename(columns={'today_date': 'yesterday_date', 'today_price':'yesterday_price'}, inplace=True)
glToday.to_csv("sAndPYesterday.csv", index=False)