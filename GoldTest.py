
# coding: utf-8

# In[47]:


import pandas as pd
import numpy as np
import requests
import json
import datetime
from flask import Flask, json, jsonify, make_response, abort

app = Flask(__name__)

# GOLD PRICES
url = "https://www.quandl.com/api/v3/datasets/LBMA/GOLD.json?api_key=1qgxtyhysHxVA3XamT_-"

response = requests.get(url)
json_data = response.json()

dataset = json_data['dataset']

# Create DateTimeIndex
df = pd.DataFrame.from_records(dataset['data'], columns=dataset['column_names'])
dfwithdates = pd.DataFrame.from_records(dataset['data'], columns=dataset['column_names'])
df.set_index(pd.to_datetime(df['Date']), inplace=True)
df.drop(['Date'], axis=1, inplace=True)

# Add extra indexes that extends all the way to the life of mine
LOM = '2024-01'
forecast = pd.DataFrame(index=np.arange(df.index.max(), LOM, dtype='datetime64[D]'))
df2 = pd.concat([df, forecast], sort=True).sort_index(ascending=True)
# Eliminate duplicated start date created from step above ^
df2 = df2[~df2.index.duplicated(keep='last')]

# Get Recent Date
recent_date = str(df.index.max())

# Get Recent Month
monthly_mean = df.resample('M').mean()
recent_month_mean = str(monthly_mean.index.max())[:-12]

# Get Current Year
yearly_mean = df.resample('Y').mean()
current_year = str(yearly_mean.index.max())[:-15]

# Get Prices
Price_AU = df[recent_date]['USD (PM)']

# Extract Dates and Values

actual_month = str(monthly_mean.index.max())[5:-12]


# Define Naming fucntion
def month_name(x):
    """Funtion to get Name of Month as in Riivos Sheet"""
    dictionary = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul',
                  '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    for key in dictionary.keys():
        if key == x:
            name = dictionary[key]
    return name


@app.route('/')
def hello():
    return 'Hello, Riivos Team!'


@app.route("/GoldPrices")
def gold():
    return dfwithdates.to_json(orient='records')


@app.route("/GoldPrices/<string:currency>")
def string_curr(currency):
    if currency == 'USD':
        d1 = dfwithdates[['Date', 'USD (PM)', 'USD (AM)']]
        return d1.to_json(orient='records')
    elif currency == 'EURO':
        d2 = dfwithdates[['Date', 'EURO (PM)', 'EURO (AM)']]
        return d2.to_json(orient='records')
    elif currency == 'GBP':
        d3 = dfwithdates[['Date', 'GBP (PM)', 'GBP (AM)']]
        return d3.to_json(orient='records')


@app.route("/GoldPrices/date/<int:date>", methods=['GET'])
def date(date):
    string = str(date)
    if len(string) == 4:
        formating = string[:4]
    elif len(string) == 6:
        formating = string[:4] + '-' + string[4:]
    elif len(string) == 8:
        formating = string[:4] + '-' + string[4:-2] + '-' + string[6:]
    else:
        abort(404)
    Price = df[formating]
    return Price.to_json(orient='records')


@app.route("/GoldPrices/date/mean/<int:meandate>", methods=['GET'])
def mdate(meandate):
    string = str(meandate)
    if len(string) == 4:
        formating = string[:4]
        mean = yearly_mean[formating]
        return mean.to_json(orient='records')
    elif len(string) == 6:
        formating = string[:4] + '-' + string[4:]
        mean = monthly_mean[formating]
        return mean.to_json(orient='records')
    elif len(string) == 8:
        formating = string[:4] + '-' + string[4:-2] + '-' + string[6:]
        day = (df[formating]['USD (PM)'] + df[formating]['USD (AM)']) / 2
        return day.to_json(orient='records')
    else:
        abort(404)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Invalid Date or Entry'}), 404)


# Rolling Average 63 Trading Days (3 Month)
@app.route("/GoldPrices/RollingAve(<int:days>)")
def rollingave(days):
    df['RA'] = df[current_year]['USD (PM)'].sort_index().rolling(days).mean().shift()
    RollingAve = df[recent_date]['RA']
    return RollingAve.to_json(orient='records')


@app.route("/GoldPrices/RollingAve(<int:day>)/Riivos")
def combine(day):
    df['RA'] = df[current_year]['USD (PM)'].sort_index().rolling(day).mean().shift()
    RollingAve1 = df[recent_date]['RA']

    def riivos():
        column_dates = []
        column_values = []

        # This for loop gets the values for current year
        monthly_mean_Frc = df2.resample('M').mean()
        for index, row in monthly_mean_Frc.iterrows():
            if str(index)[5:-12] < actual_month and str(index)[:-15] == current_year:
                column_dates.append(month_name(str(index)[5:-12]) + " " + current_year + ' Act')
                column_values.append(row['USD (PM)'])

            elif str(index)[:-12] == recent_month_mean:
                column_dates.append(month_name(str(index)[5:-12]) + " " + str(index)[:-15])
                column_values.append(Price_AU)

            elif str(index)[:-12] > recent_month_mean:
                column_dates.append(month_name(str(index)[5:-12]) + " " + str(index)[:-15])
                column_values.append(RollingAve1)

        # Create Data Frame to create xlsx
        dic = dict(zip(column_dates, column_values))
        dtf = pd.DataFrame.from_dict(dic).reset_index(drop=True)
        dtf2 = dtf.assign(Sheet="Globals", Level1="Globals", Level2="Assumptions", Activities="Commodities",
                          LineItem="Gold Price (USD)", URA="A", Site="Neptune")
        dtf2.rename(columns={'Level2': "Level 2", 'Level1': "Level 1",
                             'LineItem': "Line Item"}, inplace=True)

        # #Reorganize Data Frame
        columns = dtf2.columns.tolist()
        columns = columns[-7:] + columns[:-7]
        dtfinal = dtf2[columns]
        return dtfinal.to_json(orient='records')

    return riivos()


@app.route("/GoldPrices/Riivos/<string:type>/otherdata")
def string(type):
    if type == 'Opening':
        variable = 'USD (PM)'
    elif type == 'Closing':
        variable = 'USD (AM)'

    def mmm():
        # Create Mean, Min , Max Data Import
        dfcurrent = df2[current_year][variable].resample("M").apply(['mean', np.min, np.max])

        new_index = []
        values = []
        for index, row in dfcurrent.iterrows():
            if str(index)[5:-12] < actual_month:
                new_index.append(month_name(str(index)[5:-12]) + " " + current_year + ' Act')
                values.append(row)
            else:
                new_index.append(month_name(str(index)[5:-12]) + " " + current_year)
                values.append(row)

        # Create Data Frame to create xlsx
        dic2 = dict(zip(new_index, values))
        dtf3 = pd.DataFrame(dic2).reset_index()
        dtf4 = dtf3.assign(Sheet="Globals", Level1="Globals", Level2="Assumptions", Activities="Commodities",
                           LineItem="", URA="A", Site="Neptune")
        dtf4.rename(columns={'Level2': "Level 2", 'Level1': "Level 1",
                             'LineItem': "Line Item"}, inplace=True)
        dtf4["Line Item"] = dtf4['index']
        dtf4.drop(['index'], axis=1, inplace=True)

        # Reorganize Data Frame
        columns = dtf4.columns.tolist()
        columns = columns[-7:] + columns[:-7]
        dtfinal2 = dtf4[columns]

        return dtfinal2.to_json(orient='records')

    return mmm()


if __name__ == '__main__':
    app.run(debug=True)

for num in [1, 2, 3]:
    print(num)
