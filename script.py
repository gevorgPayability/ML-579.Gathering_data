from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
import os

import prepare_data_functions as fnc

df = pd.read_pickle('data_pickle')

columns_from_notifications = ['products_removal',
                              'policy_warning',
                              'invoice_requested',
                              'intellectual_property',
                              'infringement',
                              'pricing_error',
                              'negative_customer_experiences',
                              'reserve']


columns_dict = fnc.create_dict_with_notification(
    columns=columns_from_notifications)


notifications_dataframe = df.apply(
    lambda row: fnc.get_info_from_notifications(row, notification_colum='last_not',
                                                columns_dictionary=columns_dict, date_column='date'), axis=1)

df = pd.concat([df, notifications_dataframe], axis=1)

df['has_active_loan'] = df.apply(
    lambda row: fnc.has_active_loan(row, 'loans', 'date'), axis=1)

df['number_of_loans'] = df.apply(
    lambda row: fnc.number_of_loans(row, 'loans'), axis=1)

# To check if conversion works
df[columns_from_notifications] = df[columns_from_notifications].astype(
    'int64')
df['other'] = df['other'].astype('int64')
df['has_active_loan'] = df['has_active_loan'].astype('int64')
df['number_of_loans'] = df['number_of_loans'].astype('int64')


df.to_pickle('data_to_check_resample')

df = pd.read_pickle('data_to_check_resample')
df['date'] = pd.to_datetime(df.date)

df.set_index('date', inplace=True)

# Fill missing dates by supplier
df = df.groupby('mp_sup_key').resample('D').last()
df.drop('mp_sup_key', axis=1, inplace=True)

cls_to_interpolate = ['order_defect_rate', 'late_shipment_rate', 'cancellation_rate', 'valid_tracking_rate_all_cat',
                      'late_responses', 'return_dissatisfaction_rate', 'customer_service_dissatisfaction_rate_beta',
                      'delivered_on_time', 'sales_7_days', 'sales_30_days', 'fba']

df.reset_index(inplace=True)
df = fnc.interpolate_missing_values(
    data=df, cls_to_interpolate=cls_to_interpolate, limit=60)

df = fnc.fill_missing_values(
    data=df, columns=columns_from_notifications)

df = fnc.fill_missing_values(
    data=df, columns='account_status', limit=4)

df.to_pickle('data_with_notification_and_loan_pickle')

df.fba.fillna(0, inplace=True)
df.has_active_loan.fillna(0, inplace=True)
df.number_of_loans.fillna(0, inplace=True)

df[cls_to_interpolate] = df[cls_to_interpolate].round(4)

# Drop rows with at least 50% NULL values of cls_to_interlpolate
# These are most important columns
# Also drops rows with missing account status
df.dropna(subset=cls_to_interpolate, thresh=len(
    cls_to_interpolate)/2, inplace=True)
df.dropna(subset=['account_status'], inplace=True)


final_cls = [
    'date', 'mp_sup_key', 'account_status', 'supplier_key', 'order_defect_rate', 'late_shipment_rate',
    'cancellation_rate', 'valid_tracking_rate_all_cat', 'late_responses',
    'return_dissatisfaction_rate', 'customer_service_dissatisfaction_rate_beta',
    'delivered_on_time', 'sales_7_days', 'sales_30_days', 'fba',
    'products_removal', 'policy_warning', 'invoice_requested', 'intellectual_property',
    'infringement', 'pricing_error', 'negative_customer_experiences', 'reserve', 'other',
    'has_active_loan', 'number_of_loans'
]

final = df.loc[:, final_cls]
final[columns_from_notifications] = final[columns_from_notifications].astype(
    'int64')
final['other'] = final['other'].astype('int64')
final['has_active_loan'] = final['has_active_loan'].astype('int64')
final['number_of_loans'] = final['number_of_loans'].astype('int64')


# Uploading dataset
user = os.environ.get('ml_user')
password = os.environ.get('ml_psw')

txt = 'postgresql://{}:{}@machine-learning-db.payability.com:5432/ml'.format(
    user, password)

engine = create_engine(txt)

connection = engine.raw_connection()
cursor = connection.cursor()

cursor.execute("""DROP TABLE IF EXISTS marketplace_extended_raw;
    CREATE TABLE marketplace_extended_raw (
    date DATE, mp_sup_key UUID, supplier_key UUID, order_defect_rate numeric(11,8) , late_shipment_rate numeric(11,8),
    cancellation_rate numeric(11,8), valid_tracking_rate_all_cat numeric(11,8), late_responses numeric(11,8),
    return_dissatisfaction_rate numeric(11,8), customer_service_dissatisfaction_rate_beta numeric(11,8),
    delivered_on_time numeric(11,8), sales_7_days numeric(12,2), sales_30_days numeric(12,2), fba numeric(3,2),
    products_removal SMALLINT, policy_warning SMALLINT, invoice_requested SMALLINT, intellectual_property SMALLINT,
    infringement SMALLINT, pricing_error SMALLINT, negative_customer_experiences SMALLINT, reserve SMALLINT, other SMALLINT,
    has_active_loan SMALLINT, number_of_loans SMALLINT
);""")

connection.commit()

output = StringIO()

columns = final.columns
final.sort_values('date', inplace=True)
final.to_csv(output, sep='\t', header=False, index=False)
output.getvalue()

# jump to start of stream
output.seek(0)

# null values become ''
cursor.copy_from(output, 'marketplace_extended_raw',
                 null="", columns=(columns))
connection.commit()
cursor.close()
