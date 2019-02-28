from io import StringIO
import psycopg2 as pg
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


df = fnc.get_info_from_notifications(
    df, 'last_not', columns_from_notifications, 'date')

df['has_active_loan'] = df.apply(
    lambda row: fnc.has_active_loan(row, 'loans', 'date'), axis=1)

df['number_of_loans'] = df.apply(
    lambda row: fnc.number_of_loans(row, 'loans'), axis=1)

df.set_index('date', inplace=True)
df = fnc.fill_missing_dates_by_supplier(data=df)

cls_to_interpolate = ['order_defect_rate', 'late_shipment_rate', 'cancellation_rate', 'valid_tracking_rate_all_cat',
                      'late_responses', 'return_dissatisfaction_rate', 'customer_service_dissatisfaction_rate_beta',
                      'delivered_on_time', 'sales_7_days', 'sales_30_days', 'fba']


df = fnc.interpolate_missing_values(
    data=df, cls_to_interpolate=cls_to_interpolate, limit=60)

df = fnc.fill_missing_values_from_notification(
    data=df, columns_from_notifications=columns_from_notifications)

df.to_pickle('data_with_notification_and_loan_pickle')

df.fba.fillna(0, inplace=True)
df.has_active_loan.fillna(0, inplace=True)
df.number_of_loans.fillna(0, inplace=True)

df[cls_to_interpolate] = df[cls_to_interpolate].round(4)

df.dropna(subset=cls_to_interpolate, thresh=len(
    cls_to_interpolate)/2, inplace=True)

final_cls = [
    'date', 'mp_sup_key', 'supplier_key', 'order_defect_rate', 'late_shipment_rate',
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


final.to_sql('marketplace_extended_raw', engine)


print(2+2)
