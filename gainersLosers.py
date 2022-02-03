import numpy as np
import pandas as pd
from pandas.core.indexes import period
import json
from datetime import date, datetime, timedelta
import os
import time
import requests
from datawrapper import Datawrapper

# Getting Datawrapper API key from Github Repository Secret
ACCESS_TOKEN = os.getenv('DW_API_KEY')

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
                                            "notes": "Data from Yahoo Finance. Updated " + fileDate
                                    }
                                }},
                                headers=headers)

    response.raise_for_status()

    time.sleep(2)

    dw.publish_chart(chart_id=dw_chart_id)

# Function to get Xignite stock price data from Hermes
def getSAndPJSON(url):
    try:
        r = requests.get(url,timeout=3)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print(f"Http Error:{errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting:{errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error:{errt}")
    except requests.exceptions.RequestException as err:
        print(f"Oops: Something Else:{err}")
    
    rJSON = r.json()
    return(rJSON)

# Function to scrape the S&P 500 tickers from the S&P 500 Wikipedia page
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
def getSAndPTickers(url):
    table = pd.read_html(io=url)
    sAndP = table[0]
    assert isinstance(sAndP, pd.DataFrame),"Wikipedia table not scraped"
    assert sAndP['Symbol'].count() == 505,"Not all 505 S&P Tickers Present"

    sAndPRef = sAndP[['Symbol', 'Security']]
    sAndPRef.rename(columns={"Symbol":"Ticker", "Security":"Company Name"}, inplace=True)

    # On the wikipedia pages the "." in the ticker needs to actually be replaced with a '-' 
    sAndPRef['Ticker'] = sAndPRef['Ticker'].str.replace('.', '-', regex=False)

    tickers_list = sAndPRef['Ticker'].to_list()

    return(tickers_list)

# Function to create the final pandas dataframe sent to Datawrapper S&P 500 gainers/losers chart
def createGainersLosers(dict):
    fullGL = pd.DataFrame(dict)

    biggestLosers = fullGL.sort_values(by=['1 Day Returns']).iloc[0:8,:].reset_index()
    biggestGainers = fullGL.sort_values(by=['1 Day Returns'], ascending=False).iloc[0:8,:].reset_index()

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


spTickers = getSAndPTickers(url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

tickers = []
dodChg = []
companyName = []
close = []

# Grabbing all 505 stock data in a loop
for i in range(len(spTickers)):
    resDict = getSAndPJSON(url=f'https://hermes-stable.a-ue1.dotdash.com/Simulator/stock?symbol={spTickers[i]}')
    if isinstance(resDict, dict):
        tickerData = resDict['data'][0]
        todayClose = float(tickerData['Last'])
        yestClose = float(tickerData['PreClose'])
        ticker = tickerData['Symbol']
        fullName = tickerData['Description']
        pctChg = round(((todayClose - yestClose) / yestClose) * 100, 2)
        tickers.append(ticker)
        dodChg.append(pctChg)
        companyName.append(fullName)
        close.append(todayClose)
        # I set this to 1 second but can adjust as needed.
        # I don't know the capabilities/rate-limiting of hermes/xignite so
        # please advise if I should change
        time.sleep(1)
    else:
        break

# Making sure data for every ticker was put in the lists
if len(tickers) == 505 & len(companyName) == 505 & len(dodChg) == 505 & len(close) == 505:
    dataDict = {'Ticker': tickers,
             'Company Name': companyName,
             '1 Day Returns': dodChg,
             'Close': close
    }

    biggestLG = createGainersLosers(dict=dataDict)

    biggestLG.to_csv('gl.csv', index=False)

    fileDate = str(datetime.today().strftime('%B %d, %Y'))

    updateChart('k53KU', biggestLG, fileDate, ACCESS_TOKEN)

else:
    raise ValueError('Lists don\'t have all 505 S&P 500 tickers in them')
