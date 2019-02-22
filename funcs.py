import pandas as pd
import numpy as np
import sqlalchemy
from datetime import datetime, timedelta
from collections.abc import Iterable


def get_postgress_data(sql, user, password, col_index=None):
    """get data from payability posgress. In develop"""
    txt_ = 'postgresql://{}:{}@database.payability.com:5432/payability_v2'.format(
        user, password)
    engine = sqlalchemy.create_engine(txt_)
    data = pd.read_sql_query(
        sql, engine, index_col=col_index, parse_dates=True)

    return(data)


# def extract_loan_amount(data):
#    d = json.loads(data)
#    result = d[list(d)[0]]['Loan Information']['Original Loan Amount']
#    return(result)


def get_info_from_notifications(data, notification_colum, date_column, period=90):
    """Short summary.
    Function return new dataframe with information about notifications type
    seller received in selected period of time
    Parameters
    ----------
    data : pandas dataframe
        Description of parameter `data`.
    notification_colum : string
        Description of parameter `notification_colum`.
    date_column : 'string'
        Description of parameter `date_column`.
    period : int
        Description of parameter `period`.

    Returns
    df
    type
        pandas dataframe.

    """
    df = data.copy()
    df[date_column] = pd.to_datetime(df[date_column])

    columns = ['products_removal',
               'policy_warning',
               'invoice_requested',
               'intellectual_property',
               'infringement',
               'pricing_error',
               'negative_customer_experiences',
               'reserve']

    columns_dict = {column: column.replace('_', ' ') for column in columns}

    for col in columns:
        df[col] = 0

    for index in df.index:
        row = df.loc[index, notification_colum]
        date_filter = df.loc[index, date_column] - timedelta(days=period)
        if isinstance(row, Iterable):
            for notification in row:
                if notification is not None:
                    splited = notification.split(':', 1)
                    date = datetime.strptime(splited[0], '%B %d, %Y')
                    message = splited[1].lower()
                    if date < date_filter:
                        break
                    for column_name, value in columns_dict.items():
                        if value in message:
                            df.loc[index, column_name] += 1

    return(df)


def extract_loan_amount(data, loan_info_column):
    """Short summary.
    Extracts Amazon loan amount info from json file
    Parameters
    ----------
    data : pandas dataframe from marketplace_ext_data
        Description of parameter `data`.
    loan_info_column : string
        name of the column with json info

    Returns
    pandas data frame with new column
    type
        Description of returned object.

    """
    df = data.copy()
    df['loan_amount'] = 0

    for index in df.index:
        loan = df.loc[index, loan_info_column]
        if loan is not None:
            keys = list(loan)
            if len(keys) == 1:
                key = keys[0]
                df.loc[index, 'loan_amount'] = loan[key]['Loan Information']['Original Loan Amount']
            elif len(keys) > 1:
                print('Something unexepected happended')
                df.loc[index, 'loan_amount'] = np.NaN

    return df


def has_active_loan(row, loan_column, date_column):
    """Short summary.
    Check whether seller has active Amazon loan that period
    Parameters
    ----------
    row : type
        Description of parameter `row`.
    loan_column : type
        Name of the column where is the info about loans
    date_column : type
        Name of the column where is the info about date of record

    Returns
    -------
    type
        Description of returned object.

    """
    record = row[loan_column]
    if record is not None:
        try:
            keys = list(record)
            date = row[date_column]
            has_a_loan = []

            if len(keys) > 0:
                keys.sort(reverse=True)
                for key in keys:
                    info = row[loan_column][key]['Loan Information']
                    origination = datetime.strptime(
                        info['Loan Origination Date'], '%B %d, %Y')
                    maturity = datetime.strptime(
                        info['Loan Maturity Date'], '%B %d, %Y')
                    if origination <= datetime(date.year, date.month, date.day) < maturity:
                        has_a_loan.append(1)
                    else:
                        has_a_loan.append(0)
                return np.max(has_a_loan)
            else:
                return 0
        except:
            return np.NaN
    else:
        return 0


def number_of_loans(row, loan_column):
    """Short summary.
    Return number of loans seller got from Amazon
    Parameters
    ----------
    row : type
        Description of parameter `row`.
    loan_column : string
        Name of the column where is the info about loans

    Returns
    integer type colum
    type
        Description of returned object.

    """
    record = row[loan_column]
    if record is not None:
        try:
            keys = list(record)
            return len(keys)
        except:
            return np.NaN
    else:
        return 0


def create_empty_columns(data, list_of_columns):
    df = data.copy()
    for col in list_of_columns:
        df[col] = 0
    return(df)
