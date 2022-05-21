# !pip install yfinance

import yfinance as yf
import pandas as pd

report = pd.read_csv('kek.csv', parse_dates=[3, 4]) 

report_1 = report[['Quater of report','Signing date','Name of Issuer','Ticker of Issuer']]

Ticker = report_1['Ticker of Issuer'].unique()
First_Date = report_1.groupby('Ticker of Issuer')['Quater of report'].first().reset_index()
Last_Date = report_1.groupby('Ticker of Issuer')['Quater of report'].last().reset_index().rename(columns={'Quater of report':'Last date'})

report_2 = pd.merge(First_Date, Last_Date)

report_2['Quater of report'] = pd.PeriodIndex(report_2['Quater of report'], freq='Q').to_timestamp()

def Ticket(ticket, first_date, last_date):
    hist = yf.download(ticket).reset_index()  
    return(hist)

d = {}

for index in range(len(report_2)):
    d[index] = pd.DataFrame(Ticket(report_2['Ticker of Issuer'].loc[index], report_2['Quater of report'].loc[index], report_2['Last date'].loc[index]))