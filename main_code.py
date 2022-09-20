import  pandas as pd
import  numpy as np
import openpyxl
import warnings
import datetime
from datetime import datetime
from urllib.parse import quote_plus
import sqlalchemy
# ignoring warnings
warnings.filterwarnings("ignore")

db_cred = dict(
    DATABASE_USERNAME = 'test',
    DATABASE_PASSWORD = 'akshay1994',
    DATABASE_HOSTNAME = 'DESKTOP-O12GUMB',
    DATABASE_NAME = 'salary_cal'
)

db_cred['DATABASE_PASSWORD'] = quote_plus(db_cred['DATABASE_PASSWORD'])
DATABASE_URI = 'mssql+pyodbc://%(DATABASE_USERNAME)s:%(DATABASE_PASSWORD)s@%(DATABASE_HOSTNAME)s/%(DATABASE_NAME)s?charset=utf8&driver=ODBC+Driver+17+for+SQL+Servre' % db_cred
# LOADING_DATETIME = datetime.utnoww().isoformat()

def create_engine():
    return sqlalchemy.create_engine(DATABASE_URI)
db = create_engine()
# reading the raw file
raw_file = pd.read_excel(r"C:\Users\Acer\Desktop\Khushi'S_ETL_project\July_Salary_cal.xlsx")
raw_file.to_sql('raw_file', db, index=False, if_exists='replace')
# print(list(raw_file.columns))

# processing emp attendance
Emp_attendance = raw_file[['Emp_id', 'First_name', 'Last_name','Joining_date', 'Department' ]]
# print(Emp_attendance.dtypes)
Emp_attendance['Joining_date'] = pd.to_datetime(Emp_attendance['Joining_date'])
date_format = "%Y/%m/%d"
Emp_attendance['Present_days'] = Emp_attendance.apply(lambda x: 31 if x['Joining_date'] <datetime.strptime('2022/07/01', date_format) else (datetime.strptime('2022/07/31', date_format) - x['Joining_date']).days, axis = 1)
# print(list(Emp_attendance.columns))
Emp_attendance.to_excel(r"C:\Users\Acer\Desktop\Khushi'S_ETL_project\emp_validation.xlsx")
Emp_attendance.to_sql('Emp_attendance', db, index= False, if_exists='replace')

# processing epm details
emp_details = raw_file[['Emp_id', 'First_name', 'Last_name', 'Address', 'Email', 'Contact', 'Department']]
emp_details.to_sql('emp_details', db, index= False, if_exists='replace')

# print(list(emp_details.columns))
# processing investment tax calculation
investment_tax_cal = raw_file[['Salary_month', 'Emp_id', 'Inhand_salary', 'Pf', 'Professional_tex','Incentives', 'Investment']]
# cols_to_check = ['Inhand_salary', 'Pf', 'Professional_tex','Incentives', 'Investment']
# investment_tax_cal[cols_to_check] = investment_tax_cal[cols_to_check].replace({'$':''}, regex=True)
investment_tax_cal['Inhand_salary'] = investment_tax_cal['Inhand_salary'].str.replace(r'\D', '').astype(int)
investment_tax_cal['Pf'] = investment_tax_cal['Pf'].str.replace(r'\D', '').astype(int)
investment_tax_cal['Professional_tex'] = investment_tax_cal['Professional_tex'].str.replace(r'\D', '').astype(int)
investment_tax_cal['Incentives'] = investment_tax_cal['Incentives'].str.replace(r'\D', '').astype(int)
investment_tax_cal['Investment'] = investment_tax_cal['Investment'].str.replace(r'\D', '').astype(int)
# calculating gross salary
investment_tax_cal['gross_sal'] = investment_tax_cal.apply(lambda x: x['Inhand_salary']+ x['Pf']+ x['Professional_tex'], axis=1)
# processing tax calculation
investment_tax_cal['income_tax'] = investment_tax_cal.apply(lambda x: x['Investment']*0.20 if x['Investment']>=1000 and x['Investment']<=3000
                                                                 else x['Investment']*0.15 if x['Investment']>3000 and x['Investment']<=10000
                                                                 else x['Investment']*0.10 if x['Investment']>10000 and x['Investment']<=15000
                                                                 else x['Investment']*0.05 if x['Investment']>15000
                                                                 else None, axis =1)
investment_tax_cal['new_gross_salary'] = investment_tax_cal.apply(lambda x: x['gross_sal']- x['income_tax'], axis =1)
investment_tax_cal.to_sql('investmnet_tax_cal', db, index= False, if_exists='replace')

# print(list(investment_tax_cal.columns))
# investment_tax_cal.to_excel(r"C:\Users\Acer\Desktop\Khushi'S_ETL_project\invetment_tax.xlsx")

# creating final salary
final_salary = raw_file[['Salary_month', 'Emp_id', 'First_name', 'Last_name', 'Email', 'Contact', 'Department', 'Pf', 'Professional_tex', 'Incentives' ]]
final_salary['Pf'] = final_salary['Pf'].str.replace(r'\D', '').astype(int)
final_salary['Professional_tex'] = final_salary['Professional_tex'].str.replace(r'\D', '').astype(int)
final_salary['Incentives'] = final_salary['Incentives'].str.replace(r'\D', '').astype(int)
final_salary = pd.merge(final_salary,investment_tax_cal[['Emp_id','new_gross_salary']], how='left', on='Emp_id')
# print(list(final_salary.columns))
final_salary['final_inhand_sal'] = final_salary.apply(lambda x: x['new_gross_salary']- x['Pf']- x['Professional_tex']+ x['Incentives'], axis=1)
final_salary.to_sql('final_salary', db, index= False, if_exists='replace')

# print(list(final_salary.columns))
# print(final_salary)
