import pandas as pd
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


def extract_loan_amount(data):
    d = json.loads(data)
    result = d[list(d)[0]]['Loan Information']['Original Loan Amount']
    return(result)


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
