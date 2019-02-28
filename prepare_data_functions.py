import sqlalchemy
import pandas as pd
import numpy as np
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


def get_info_from_notifications(data, notification_colum, columns, date_column, period=90):
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
        except:
            print('Something unexpected happened at index', index)

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
                               limit=14, limit_area='inside'):
    """[Function fills missing values lineary.]

    Arguments:
        data {[pandas dataframe]} -- [pandas dataframe with datetimeindex]
        cls_to_interpolate {[list]} -- [columns to fill]

    Keyword Arguments:
        by_column {str} -- [column to group by] (default: {'mp_sup_key'})
        limit {int} -- [description] (default: {14})
        limit_area {str} -- [description] (default: {'inside'})
    """

    df = data.copy()
    df.index.rename('date', inplace=True)
    cls_to_stay = list(set(df.columns) - set(cls_to_interpolate))
    cls_to_stay.append('date')

    suppliers = data[by_column].unique()
    print('Number of suppliers is', len(suppliers))
    supplier_dataframes_list = []

    for index, supplier in enumerate(suppliers):
        if index % 200 == 0:
            print(index)
        df_supplier = df.loc[df[by_column] == supplier]
        df_supplier = df_supplier[cls_to_interpolate].interpolate(
            limit=limit, limit_area=limit_area)
        df_supplier['mp_sup_key'] = supplier
        supplier_dataframes_list.append(df_supplier)

    new_df = pd.concat(supplier_dataframes_list)

    new_df['mp_sup_key'] = new_df.mp_sup_key.astype(str)
    df['mp_sup_key'] = df.mp_sup_key.astype(str)

    df.reset_index(inplace=True)
    new_df.reset_index(inplace=True)

    final = pd.merge(df[cls_to_stay], new_df, on=['mp_sup_key', 'date'])

    return(final)


def fill_missing_values_from_notification(data, columns_from_notifications, limit=7, by_column='mp_sup_key'):
    """Function fill missing values in columns from notifications. It fill forwards
    and backwards by given (limit) day

    Arguments:
        data {[pandas dataframe]} -- [pandas dataframe with notification columns]

    Keyword Arguments:
        limit {int} -- [limit day length for fill] (default: {7})
        by_column {str} -- [group by column] (default: {'mp_sup_key'})

    Returns:
        [pandas dataframe] -- [description]
    """

    df = data.copy()
    suppliers = df[by_column].unique()
    print('Number of suppliers is', len(suppliers))
    supplier_dataframes_list = []

    for index, supplier in enumerate(suppliers):
        if index % 200 == 0:
            print(index)
        df_supplier = df.loc[df[by_column] == supplier]
        df_supplier[columns_from_notifications].fillna(
            'ffill', limit=limit, inplace=True)
        df_supplier[columns_from_notifications].fillna(
            'bfill', limit=limit, inplace=True)
        supplier_dataframes_list.append(df_supplier)
    return pd.concat(supplier_dataframes_list)


def some_func(a, b):
    """[summary]

    Arguments:
        a {[str]} -- [name of your mom]
        b {[int]} -- [age of your mom]
    """

    print('Hello world', a)
