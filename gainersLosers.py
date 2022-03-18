import numpy as np
import pandas as pd
from pandas.core.indexes import period
import json
from datetime import date, datetime, timedelta
import os
import time
import requests
from urllib.parse import quote
import yfinance as yf
from datawrapper import Datawrapper

# Getting Datawrapper API key from Github Repository Secret
ACCESS_TOKEN = os.getenv('DW_API_KEY')
SIM_PW = os.getenv('INV_SIM_PW')

# Activating Datawrapper class used to send new data to chart
dw = Datawrapper(access_token=ACCESS_TOKEN)

# Function used to add new data to datawrapper chart via a pandas dataframe and 
# places the latest update date in the chart notes
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
                                            "notes": f"Data from Investopedia & Yahoo Finance. Updated {updateDate}"
                                    }
                                }},
                                headers=headers)

    response.raise_for_status()

    time.sleep(2)

    dw.publish_chart(chart_id=dw_chart_id)

# Function to get simulator OAuth Key
def getSimOAuth(pw):
    try:
        simHeaders = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {'client_id':'inv-simulator', 'username':quote('anesta95'), 'grant_type':'password', 'password':pw}

        res = requests.post(url='https://www.investopedia.com/auth/realms/investopedia/protocol/openid-connect/token',
                    headers=simHeaders,
                    data=payload
        )
        res.raise_for_status()
        simOAuthJSON = res.json()
        simOAuthAccess = simOAuthJSON['access_token']
        return(simOAuthAccess)
        
    except requests.exceptions.HTTPError as errh:
        print(f"OAuth Http Error:{errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"OAuth Connecting:{errc}")
    except requests.exceptions.Timeout as errt:
        print(f"OAuth Error:{errt}")
    except requests.exceptions.RequestException as err:
        print(f"OAuth Oops: Something Else:{err}")
    
    

# Function to use yfinance to get ticker data
def getYFinance(ticker):
    yfTicker = ticker.replace('.', '-')
    yfRes = yf.Ticker(yfTicker)
    tickerDF = yfRes.history(period='5d', interval='1d', prepost=False, auto_adjust=False, actions=False)
    time.sleep(0.25)
    tickerDF.reset_index(level=0, inplace=True)
    tickerDFSorted = tickerDF.sort_values(by=['Date'], ascending=False).reset_index(drop=True)
    todayPrice = float(tickerDFSorted['Close'][0])
    yesterdayPrice = float(tickerDFSorted['Close'][1])
    dayChangePrice = todayPrice - yesterdayPrice
    dodChgYF = round(((todayPrice - yesterdayPrice) / yesterdayPrice) * 100, 2)
    stockData = {"closePrice": yesterdayPrice, "dayChangePercent": dodChgYF, "dayChangePrice": dayChangePrice}

    return(stockData)

# Function to get individual stock data
def getSAndP500Data(ticker, OAuth):
    try:
        query = f"""query {{
    readStock(symbol:"{ticker}") {{
        ...on Stock {{
        technical {{
            closePrice
            dayChangePercent
            dayChangePrice
        }}
        }}
    }}
    }}"""
        simAuth = {'Authorization': f"Bearer {OAuth}", 'Origin': 'https://www.investopedia.com/'}
        res = requests.post(url="https://api.investopedia.com/simulator/graphql", 
                    headers=simAuth,
                    json={'query': query})

        stockJSON = res.json()
        # Check to see if simulator has ticker data, if not use yfinance
        if stockJSON is None:
            stockData = getYFinance(ticker=ticker)
        elif stockJSON['data'] is None:
            stockData = getYFinance(ticker=ticker)
        elif not stockJSON['data']['readStock']:
            stockData = getYFinance(ticker=ticker)
        else:
            stockData = stockJSON['data']['readStock']['technical']
            
    except requests.exceptions.HTTPError as errh:
        print(f"GraphQL Http Error:{errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"GraphQL Error Connecting:{errc}")
    except requests.exceptions.Timeout as errt:
        print(f"GraphQL Timeout Error:{errt}")
    except requests.exceptions.RequestException as err:
        print(f"GraphQL Oops: Something Else:{err}")

    return(stockData)
    

# Function to scrape the S&P 500 tickers from the S&P 500 Wikipedia page
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
def getSAndPTickers(url):
    table = pd.read_html(io=url)
    sAndP = table[0]
    assert isinstance(sAndP, pd.DataFrame),"Wikipedia table not scraped"
    # assert sAndP['Symbol'].count() == 505,"Not all 505 S&P Tickers Present"

    sAndPRef = sAndP[['Symbol', 'Security']]
    sAndPRef.rename(columns={"Symbol":"Ticker", "Security":"Company Name"}, inplace=True)

    # On the wikipedia pages the "." in the ticker needs to actually be replaced with a '-' for yfinance
    # I don't think this is the case for the investo simulator
    # sAndPRef['Ticker'] = sAndPRef['Ticker'].str.replace('.', '-', regex=False)

    return(sAndPRef)

# Function to create the final pandas dataframe sent to Datawrapper S&P 500 gainers/losers chart
def createGainersLosers(dict, df):
    fullGL = pd.DataFrame(dict)

    sAndPRef = df[['Ticker', 'Company Name']]

    fullGLMerged = fullGL.merge(sAndPRef, how='left', on='Ticker')

    biggestLosers = fullGLMerged.sort_values(by=['1 Day Returns']).iloc[0:8,:].reset_index()
    biggestGainers = fullGLMerged.sort_values(by=['1 Day Returns'], ascending=False).iloc[0:8,:].reset_index()

    biggestLosers.rename(columns={'1 Day Returns':'% Loss'}, inplace=True)
    biggestGainers.rename(columns={'1 Day Returns':'% Gain'}, inplace=True)

    # Adding in dummy character because datawrapper columns need to be unique and editorial
    # wants both columns to read "price"
    biggestLosers.rename(columns={'Close':'Price\u061C'}, inplace=True)
    biggestGainers.rename(columns={'Close':'Price'}, inplace=True)

    biggestLosers['% Loss'] = ['{0:.2f}'.format(x) + '%' for x in biggestLosers['% Loss']]
    biggestGainers['% Gain'] = ['{0:.2f}'.format(x) + '%' for x in biggestGainers['% Gain']]


    biggestLosers['Biggest Losses'] = biggestLosers[['Ticker', 'Company Name']].agg(', '.join, axis=1)
    biggestGainers['Biggest Gains'] = biggestGainers[['Ticker', 'Company Name']].agg(', '.join, axis=1)

    blFinal = biggestLosers[['Biggest Losses', 'Price\u061C', '% Loss']]
    bgFinal = biggestGainers[['Biggest Gains', 'Price', '% Gain']]

    gainers_losers = pd.concat([bgFinal, blFinal], axis=1)

    return(gainers_losers)


sAndPDF = getSAndPTickers(url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

spTickers = sAndPDF['Ticker'].to_list()

# See how many tickers there are
numTickers = sAndPDF['Ticker'].count()

simOAuth = getSimOAuth(pw=SIM_PW)

tickers = []
dodChg = []
close = []

# Grabbing all 505 stock data in a loop
for i in range(len(spTickers)):
    resDict = getSAndP500Data(ticker=spTickers[i], OAuth=simOAuth)
    if isinstance(resDict, dict):
        pctChg = float(resDict['dayChangePercent'])
        yesterdayClose = float(resDict['closePrice'])
        priceChange = float(resDict['dayChangePrice'])
        todayClose = yesterdayClose + priceChange
        ticker = spTickers[i]
        tickers.append(ticker)
        dodChg.append(pctChg)
        close.append(todayClose)
        time.sleep(0.5)
    else:
        break

# Making sure data for every ticker was put in the lists
if len(tickers) == numTickers & len(dodChg) == numTickers & len(close) == numTickers:
    dataDict = {'Ticker': tickers,
             '1 Day Returns': dodChg,
             'Close': close
    }

    biggestLG = createGainersLosers(dict=dataDict, df=sAndPDF)

    biggestLG.to_csv('./visualizations/gl.csv', index=False)

    fileDate = str(datetime.today().strftime('%B %d, %Y'))

    updateChart('k53KU', biggestLG, fileDate, ACCESS_TOKEN)

else:
    raise ValueError('Lists don\'t have all 505 S&P 500 tickers in them')