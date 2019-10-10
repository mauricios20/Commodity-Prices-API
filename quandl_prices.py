""" python Flask code to run a microservice to fetch and return security prices from Quandl

Nov 06 2018

change notes:
 11/06/18  version 0
 call_quandl() has been added; will fetch data on a symbol, column, and time range from the quandl API
 currency field in input json has been converted to a "column" field because quandl has no consistency of how
   to request a specific currency in their API, unfortunately
 added map_to_new_cal(); will return a DataFrame with a new datetime index by finding rows in original DataFrame
   at or earlier than each date in the new calendar.
 added code to convert request dates to the last date of each month if there are not 2 dates in the same month
 fixed bug that prevented request dates in the future from working properly.  Also, extended the requested quandl
   date range to be 31 days earlier than the first request date, to cover case of monthly request time series.
 added support for request fields collapse and collapse_func (see API description below)
 added code to support the "RollingAve" request parameter
 added request json input validity checking



describe API here:

 Request JSON:
   "symbol" : "LBMA/GOLD" for example
   "column": "USD (PM)" for example.  Quandl has no consistency on how they name columns, so valid choices
                         here will depend on the symbol that is being used.  This is a PITA, but I don't see a way
                         to get around this limitation in Quandl's API.
   "dates" : array of dates, in this format: ["%Y-%m-%d", "%Y-%m-%d", ...]  Returned prices will be on this calendar
                         NOTE: "%Y-%m" and "%Y-%b" are also accepted, and will be converted to the *last* day of each
                         month!
   "collapse" : "monthly", "quarterly", or "annual" (OPTIONAL)   Will resample the quandl data to a lower
                          frequency *before* mapping data to the requested dates.
   "collapse_func" : "mean", "max", or "min" (OPTIONAL)   Function to apply to prices in the "collapse" step.
                          Only used if a valid value of "collapse" is supplied.  Defaults to "mean".
    "RollingAve" : an integer.  Number of periods to do a moving average over (OPTIONAL).  This is done *after*
                          the optional collapse to lower frequency and *before* mapping to the requested dates. A
                          value of 1 will have no effect.


 Return JSON:
    for each column (e.g., data item, see above), a list in the format:
        {"YYYY-mm-ddT00:00:00Z":value1, "YYYY-mm-ddT00:00:00Z":value2, etc.}
    where the number of date:value pairs equals the number of "dates" in the request JSON.  Note that the returned dates
    may not exactly match the requested dates, due to monthly averaging, for example.  The returned dates are
    *time-safe*, in other words, they represent dates on which the values were known.  So, a monthly average of daily
    prices is not known until the last day of the month.  In cases of missing values in the underlying time series
    of requested prices, mapping onto the requested dates is done via "fill-forward" (causal) calendar mapping.
    Note also that in the current version, all returned dates are at midnight (no time component).
"""

import pandas as pd
import datetime as dt
import quandl
import numpy as np
from flask import Flask, request

date_format = '%Y-%m-%d'  # will be used below to convert back/from from strings to datetimes
date_format2 = '%Y-%m'  # this will be tried on input json dates if the above fails
date_format3 = '%Y-%b'  # this will be tried on input json dates if the above fails


# function to make the Quandl API call and return data
# note that start_date and end_date are strings in %Y-%m-%d format
def call_quandl(symbol='LBMA/GOLD', column='ALL', start_date='2017-01-01', end_date=None, key='1qgxtyhysHxVA3XamT_-'):
    if end_date is None:
        end_date = dt.datetime.now().strftime(date_format)  # use today, if no end_date passed in
    quandl.ApiConfig.api_key = key
    # note: although not relevant in current example, 'collapse':'daily' below will prevent data more frequent than
    # once/day from being returned
    data_params = {'start_date': start_date, 'end_date': end_date, 'collapse': 'daily'}
    data = quandl.Dataset(symbol).data(params=data_params).to_pandas()  # import the data
    # The step above converts the Date column into a DateTimeIndex.
    # if desired, choose a single column:
    if column != 'ALL':
        data = data[[column]]  # select a single column
    data = data.dropna()  # drop rows with NA (missing) values.  Yes, Quandl does have some.
    assert isinstance(data, pd.DataFrame)  # sanity check
    return data


# function to take DataFrame on one calendar (DateTimeIndex) and return it on a different calendar
# 2FIX: this function *only* works if new_cal is a date-only (no times) object.  Need to fix this in order
# to use this function in the future with data at greater than daily freq. See comment below
# new_cal must be a list of datetimes.
def map_to_new_cal(df, new_cal):
    assert isinstance(new_cal, list)
    assert isinstance(df, pd.DataFrame)  # sanity check
    # check to see if new_cal extends beyond the time range of df.  If so, need to extend df to one day after the last
    # date in new_cal, so that the result (after the resample below) will have the full time range of new_cal
    if max(new_cal) > max(df.index):
        df = df.reindex(df.index.union([max(new_cal) + dt.timedelta(days=1)]))
    # ok, so this is a kludge.  we first upsample to a full daily time series, and then subselect just the
    # dates that are in new_cal.  The right way to do this would be to just map straight from the existing
    # datetime index to new_cal by finding the value of the existing index less than or equal to each value
    # of new_cal.  Fix this later.
    df = df.resample(rule='D').pad()  # every calendar day between first and last dates in df
    df = df[df.index.isin(new_cal)]  # select just the ones in new_cal
    df = df.sort_index()             # just in case ...
    return df


# function to get the last calendar day of a month, starting from a datetime object.  Returns a datetime, too
# why does this not already exist in Python?  No idea, but I can't find it ...
def last_day_of_month(date):
    next_month = date.replace(day=28) + dt.timedelta(days=4)
    return next_month - dt.timedelta(days=next_month.day)


app = Flask(__name__)


@app.route('/quandl-prices', methods=['POST'])
def post():
    ### check for validity of request:
    if not request.is_json:
        return "invalid request"
    content = request.get_json()
    # look at the components of content individually, and check each one for validity:
    if 'collapse' in content:   # this is an optional field, but must have one of these values:
        if content['collapse'] not in ['annual', 'quarterly', 'monthly']:
            return "invalid request"
    if 'dates' not in content:        # this is a mandatory field
        return "invalid request"
    if not isinstance(content['dates'], list):  # code below will break if this is not true
        return "invalid request"

    # translate input dates into a standard format
    request_date_str = content['dates']  # this a list of strings
    request_dates = [None]   # request_dates will be a list of datetimes
    # convert to datetimes.  Need to try multiple formats to see which one works
    for try_format in [date_format, date_format2, date_format3]:
        try:
            request_dates = [dt.datetime.strptime(x, try_format) for x in request_date_str]
            break
        except ValueError:
            pass
    # make sure the conversion to datetimes worked; if not, quit now:
    if not all(isinstance(x, dt.datetime) for x in request_dates):
        return "invalid request"

    ### create an alternate calendar where every date is *last* day of month:
    request_dates_end_of_month = [last_day_of_month(x) for x in request_dates]
    # 2DO: think about whether the rule below is really what we want ???
    # if dates passed in are monthly, or less frequent, make each date the last, not first, day of the month
    if  len(request_dates_end_of_month) > 1 and \
            (len(request_dates_end_of_month) == len(set(request_dates_end_of_month))):  # if no dup's
            request_dates = request_dates_end_of_month

    ### using the requested parameters, call the quandl API:
    data = call_quandl(
        symbol=content['symbol'],
        column=content['column'],
        start_date=(min(request_dates) - dt.timedelta(days=31)).strftime(date_format),
        # push back start_date one month to handle case of monthly request dates
        end_date=max(request_dates).strftime(date_format)
    )
    # data is a DataFrame

    ### do (optional) month/quarter/year averaging:
    # note: this section assumes that content was already checked above for valid values of collapse and collapse_func
    if 'collapse' in content:
        if content['collapse'] == 'annual':
            collapse_period = 'Y'
        elif content['collapse'] == 'quarterly':
            collapse_period = 'Q'
        elif content['collapse'] == 'monthly':
            collapse_period = 'M'
        if 'collapse_func' in content:
            if content['collapse_func'] == 'min':
                collapse_func = np.min
            elif content['collapse_func'] == 'max':
                collapse_func = np.max
            elif content['collapse_func'] == 'mean':
                collapse_func = np.mean
        else:
            collapse_func = np.mean  # this is the default value if nothing passed in

        data = data.groupby(pd.Grouper(freq=collapse_period)).apply(collapse_func)

    ### do (optional) moving averages:
    if 'RollingAve' in content:
        rollingave = int(content['RollingAve'])
        if rollingave > 1:
            data = data.rolling(rollingave, min_periods=1).mean()

    ### map quandl's dates (or optionally, averaged dates) onto the requested dates:
    data = map_to_new_cal(data, request_dates)

    ### return the result
    return data.to_json(date_format='iso', date_unit='s')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
