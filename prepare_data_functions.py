import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections.abc import Iterable


def get_postgress_data(sql, user, password, col_index=None):
    """get data from payability posgress. In develop"""
    connection_text = 'postgresql://{}:{}@database.payability.com:5432/payability_v2'.format(
        user, password)
    engine = sqlalchemy.create_engine(connection_text)
    data = pd.read_sql_query(
        sql, engine, index_col=col_index, parse_dates=True)
    # close connection
    engine.dispose()

    return(data)


def get_info_from_notifications(data, notification_colum, columns, date_column,
                                period=90, return_errors=False):
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
    error_list = []
    df = data.copy()
    df[date_column] = pd.to_datetime(df[date_column])

    columns_dict = {column: column.replace('_', ' ') for column in columns}

    for col in columns:
        df[col] = 0

    df['other'] = 0

    for index in df.index:
        try:
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
                        other_message = []
                        for column_name, value in columns_dict.items():
                            if value in message:
                                df.loc[index, column_name] += 1
                                other_message.append(1)
                            else:
                                other_message.append(0)
                        if np.max(other_message) == 0:
                            df.loc[index, 'other'] += 1
        except Exception as e:
            message = 'Something unexpected happened at index' + index + '. Error message: ' + e
            error_list.append(message)
            print(message)

    if return_errors:
        return df, error_list

    return(df)


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
        except Exception as e:
            print(e)
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
        except Exception as e:
            print(e)
            return np.NaN
    else:
        return 0


def filter_out_small_sample_size(data, tresh=30):
    """Short summary.
    Filter out sellers with less than desired sample size
    Parameters
    ----------
    data : pandas dataframe
        Description of parameter `data`.
    tresh : int
        The size of treshold

    Returns
    Filtered dataframe
    type
        Description of returned object.

    """
    df = data.copy()
    number_of_observations = df.mp_sup_key.value_counts()
    sellers_to_stay = number_of_observations[number_of_observations > tresh].index

    return df.loc[df.mp_sup_key.isin(sellers_to_stay)]


def fill_missing_dates_by_supplier(data, by_column='mp_sup_key'):
    """Short summary.
    Fills missing days by supplier
    Parameters
    ----------
    data : pandas dataframe with datetime index
        Description of parameter `data`.
    by_column : string
        Description of parameter `by_column`.

    Returns
    -------
    type
        Description of returned object.

    """
    df = data.copy()
    suppliers = df[by_column].unique()
    print('Number of suppliers is', len(suppliers))
    supplier_dataframes_list = []

    for index, supplier in enumerate(suppliers):
        if index % 200 == 0:
            print(index)
        df_supplier = df.loc[df[by_column] == supplier]
        date_index = pd.date_range(
            df_supplier.index.min(), df_supplier.index.max())
        supplier_dataframes_list.append(df_supplier.reindex(date_index))

    return pd.concat(supplier_dataframes_list)


def interpolate_missing_values(data, cls_to_interpolate, by_column='mp_sup_key',
                               limit=60):
    """[summary]
    Function interpolates missing values lineary. For example when there
    is sequence 2, NaN, 4 function fills NaN value with 3.
    Arguments:
        data pandas dataframe -- data to fill missing values
        cls_to_interpolate list of strings -- list of column to interpolate

    Keyword Arguments:
        by_column {str} -- [column to group by] (default: {'mp_sup_key'})
        limit {int} -- [limit days of filling] (default: {60})

    Returns:
        [type] -- [description]
    """

    df = data.copy()
    cls_to_stay = list(set(df.columns) - set(cls_to_interpolate))
    cls_to_stay.append('date')

    suppliers = data[by_column].unique()
    print('Number of suppliers is', len(suppliers))

    df[cls_to_interpolate] = df.groupby(by_column)[cls_to_interpolate].transform(
        lambda x: x.interpolate(limit=limit, limit_area='inside'))

    return df


def fill_missing_values_from_notification(data, columns_from_notifications,
                                          limit=30, by_column='mp_sup_key'):
    """ Function fill missing values in columns from notifications. It fill forwards
    and backwards by given (limit) day

    Arguments:
        data {[pandas dataframe]} -- [pandas dataframe with notification columns]

    Keyword Arguments:
        limit {int} -- [limit day length for fill] (default: {30})
        by_column {str} -- [group by column] (default: {'mp_sup_key'})

    Returns:
        [pandas dataframe] -- [description]
    """

    df = data.copy()
    suppliers = df[by_column].unique()
    print('Number of suppliers is', len(suppliers))

    df[columns_from_notifications] = df.groupby(by_column)[columns_from_notifications].transform(
        lambda x: x.fillna(method='ffill', limit=limit))
    df[columns_from_notifications] = df.groupby(by_column)[columns_from_notifications].transform(
        lambda x: x.fillna(method='bfill', limit=limit))

    return df
