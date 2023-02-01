"""
*** Add tooltips
*** Auto-fill the observational threshold (50% of window)
    and 6 month threshold based on window (5 months if window >= 18 months, 4 months if window < 18 months)
- Date checking (read in the range of dates in the dataset):
- Make sure testing date is in the dataset 
- Make sure there is enough testing data in the code to satisfy the window **before** the testing date
- Save config file

"""


import PySimpleGUI as sg
import pandas as pd
import numpy as np
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import pathlib
import pickle
import os
import datetime as dt


from pkg_resources import to_filename

from consumption_anomalies_analysis import ConsumptionAnomalies

def consumption_analysis_gui():
    window = file_select_window()
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED or event == 'Cancel': # if user closes window or clicks cancel
            break
        elif event == "Submit":
            assert(values['-IN-'])
            path = pathlib.Path(values['-IN-'])
            if path.suffix in ('.csv', '.xlsx'):
                print('proper file')
                column_select_window(path)
                break
            else:
                print('wrong file')
    window.close()



def file_select_window():
    sg.theme('DarkAmber')   

    layout = [  [sg.Text('File Selection:')],
                [sg.Text('Choose a file: '), sg.Input(), sg.FileBrowse(key = '-IN-')],
                [sg.Button('Submit'), sg.Button('Cancel')] ]
    return sg.Window('Anomaly Detection Tool', layout, size = (600,240))

def analysis_complete_window():
    sg.theme('DarkAmber')
    
    layout = [  [sg.Button('Open Report')]  ]
    window = sg.Window('Consumption Anomalies Analysis Complete',layout, size = (600,240))

    while True:
        event, values = window.read()
        if event == "Exit" or event == sg.WIN_CLOSED:
            break
        elif event == 'Open Report':
            print('open report now')
            os.system("start excel.exe Consumption_Anomaly_Report.xlsx")
            break
    window.close()

def column_select_window(path):
    sg.theme('DarkAmber')   

    if path.suffix == '.csv':
        df = pd.read_csv(path)
    elif path.suffix == '.xlsx':
        df = pd.read_excel(path,engine = 'openpyxl')

    prod_opts = {'prod_id_col': '', 'prod_cat_col': '', 'prod_psize_col': ''}
    fac_opts = {'fac_id_col': '', 'fac_dist_col':'', 'fac_reg_col':''}

    '''def is_dt(col_name):
        pd.to_datetime(df[[col_name]])
    period_cols_list = [column for column in df.columns if is_dt(df[column])]
    '''

    options = df.columns
    layout = [  
                [sg.Text('Parameter Selection: '), sg.Button('Save Config'), sg.Button('Load Config')],

                [sg.Text('Select date column: ', tooltip = 'period on which to detect anomalies - typically the most recent period available in this dataset'),
                 sg.Text('Monthly or Quarterly Data?', p = ((444,0),(0,0)))],
                [sg.Combo(values=list(options), size = (60,9), enable_events = True, key='date_col',
                tooltip = 'period on which to detect anomalies - typically the most recent period available in this dataset'),
                sg.Button('Monthly', p = ((144,0),(0,0)), button_color = 'grey', key = 'monthly', disabled = True), sg.Button('Quarterly', disabled = False, button_color = 'orange', p = ((20,0),(0,0)), key = 'quarterly')],

                [sg.Text('Select consumption column: ', tooltip = 'amount of a product stock consumed by or issued from a given facility for a given period'), sg.Text('MONTHLY selected', p = ((420,0),(0,0)), text_color = 'blue', key = 'freq_text')],
                [sg.Combo(values=list(options), size = (60,9), key='cons_col', 
                tooltip = 'amount of a product stock consumed by or issued from a given facility for a given period'), sg.Text('Advanced Settings', p = ((156,0),(0,0)))],

                [sg.Text('Choose date period: '), sg.Text('Required months of data: (advanced setting)', text_color = 'grey', key = 'adv1', p = ((316,0),(0,0)) )],
                [sg.Combo(values=list(range(1,25)), default_value = 12, size = (60,6), enable_events=True, key='window', 
                tooltip = 'number of months of data included in the dataset preceding the testing month'),
                sg.Combo(values=list(range(1,24)), size = (60,6), disabled = True, key='obs_thresh', 
                tooltip = 'number of months of data that must be present in the dataset for a given facility/product combination in order to calculate an anomaly')],

                #[sg.Text('Required months of data: (advanced setting)', text_color = 'grey', key = 'adv1')],
                #[sg.Combo(values=list(range(1,16)), size = (60,6), disabled = True, key='obs_thresh', 
                #tooltip = 'number of months of data that must be present in the dataset for a given facility/product combination in order to calculate an anomaly')],

                #[sg.Text('Required months of data in last 6 months: (advanced setting)', text_color = 'grey', key = 'adv2')],
                #[sg.Combo(values=list(range(1,10)), size = (60,6), disabled = True, key='obs_thresh_6M', 
                #tooltip = 'number of months of data in the last 6 months that must be present in the dataset for a given facility/product combination in order to calculate an anomaly')],

                [sg.Text('Select test year: '),sg.Text('Required months of data in last 6 months: (advanced setting)', text_color = 'grey', key = 'adv2', p = ((336,0),(0,0)) )],
                [sg.Combo(values=list(range(2018,2023)), size = (60,6), disabled = False, key='test_year', 
                tooltip = 'designates the year of the period to be tested for anomalies'),
                sg.Combo(values=list(range(1,10)), size = (60,6), disabled = True, key='obs_thresh_6M', 
                tooltip = 'number of months of data in the last 6 months that must be present in the dataset for a given facility/product combination in order to calculate an anomaly')],

                [sg.Text('Select test month: ')],
                [sg.Combo(values=list(range(1,13)), size = (60,6), disabled = False, key='test_month', 
                tooltip = 'designates the month of the period to be tested for anomalies')],

                [sg.Text('Select product name column: ')],
                [sg.Combo(values=list(options), size = (60,9), key='prod_col', 
                tooltip = 'column containing the name of the product')],

                [sg.Text('Select facility name column: ')],
                [sg.Combo(values=list(options), size = (60,9), key='fac_col', 
                tooltip = 'column containing the name of the facility')],

                [sg.Button('Advanced Controls'), sg.Button('Additional Product Info'), sg.Button('Additional Facility Info')],
                [sg.Button('Submit'), sg.Button('Cancel')],
                [sg.Text('Select test year: ', visible = False, text_color = 'red', key = 'error')] 
            ]

        
    window = sg.Window('Data Configurations', layout, size = (860,556), finalize=True)
    quarterly = False
    while True:
        event, values = window.read()
        v = int(values['window'])
        
        window['obs_thresh'].update(value = round(int(values['window'])*.75))
        window['obs_thresh_6M'].update(value = 5 if v >= 18 else 4)
        if event == "Cancel" or event == sg.WIN_CLOSED:
            break
        elif event == 'date_col':
            col_name = values['date_col']
            #period_column = pd.to_datetime(df[col_name])
        elif event == 'Save Config':
            save_config(values, prod_opts, fac_opts, quarterly)
        elif event == 'Load Config':
            configs, prod_opts, fac_opts, quarterly = load_config()
            vars_list = ['date_col', 'cons_col', 'obs_thresh', 'obs_thresh_6M', 'test_year', 'test_month', 'prod_col', 'fac_col']
            for var in vars_list:
                if (configs[var]): window[var].update(configs[var])

            if (quarterly == True):
                window['quarterly'].update(disabled = True, button_color = 'grey')
                window['monthly'].update(disabled = False, button_color = 'orange')
                window['adv1'].update(value = "Required quarters of data: (advanced setting)")
                window['adv2'].update(value = "Required quarters of data in last 6 months: (advanced setting)")
                window['freq_text'].update(value = "QUARTERLY selected")
            else:
                window['quarterly'].update(disabled = False, button_color = 'orange')
                window['monthly'].update(disabled = True, button_color = 'grey')
                window['adv1'].update(value = "Required months of data: (advanced setting)")
                window['adv2'].update(value = "Required months of data in last 6 months: (advanced setting)")
                window['freq_text'].update(value = "MONTHLY selected")
                # toggle button!!!
            #if (configs['date_col']): window['date_col'].update(configs['date_col'])
            #if (configs['cons_col']): window['cons_col'].update(configs['cons_col'])
        elif event == 'monthly':
            quarterly = False
            window['quarterly'].update(disabled = False, button_color = 'orange')
            window['monthly'].update(disabled = True, button_color = 'grey')
            window['adv1'].update(value = "Required months of data: (advanced setting)")
            window['adv2'].update(value = "Required months of data in last 6 months: (advanced setting)")
            window['freq_text'].update(value = "MONTHLY selected")
        elif event == 'quarterly':
            quarterly = True
            window['quarterly'].update(disabled = True, button_color = 'grey')
            window['monthly'].update(disabled = False, button_color = 'orange')
            window['adv1'].update(value = "Required quarters of data: (advanced setting)")
            window['adv2'].update(value = "Required quarters of data in last 6 months: (advanced setting)")
            window['freq_text'].update(value = "QUARTERLY selected")
        elif event == 'window':
            window['obs_thresh'].update(value = round(int(v*.75)))
            window['obs_thresh_6M'].update(value = 5 if v >= 18 else 4)
        elif event == 'Advanced Controls':
            window['obs_thresh'].update(disabled = False)
            window['adv1'].update(text_color = 'orange')
            window['obs_thresh_6M'].update(disabled = False)
            window['adv2'].update(text_color = 'orange')
        elif event == 'Additional Product Info':
            prod_opts = product_window(prod_opts, options)
        elif event == 'Additional Facility Info':
            fac_opts = facility_window(fac_opts, options)
        elif event == "Submit":
            vars_list = ['date_col', 'cons_col', 'obs_thresh', 'obs_thresh_6M', 'test_year', 'test_month', 'prod_col', 'fac_col']

            if (not all([values[var] for var in vars_list])):
                window['error'].update(value = "Please ensure all boxes are filled before continuing", visible = True)
            elif ((pd.to_datetime(str(values['test_year'])+'-'+str(values['test_month'])+'-1').date()) not in (pd.to_datetime(df[values['date_col']]).apply(lambda x: x.replace(day=1).date()).unique())):
                print (pd.to_datetime(str(values['test_year'])+'-'+str(values['test_month'])+'-1').date())
                print (pd.to_datetime(df[values['date_col']]).apply(lambda x: x.replace(day=1).date()).unique())
                window['error'].update(value = "Please select a test month and year that are present in the dataset", visible = True)
            else:

                date_col = values['date_col']
                cons_col = values['cons_col']
                window_var = int(values['window'])
                
                if (values['obs_thresh']):
                    obs_thresh = int(values['obs_thresh'])
                else:
                    obs_thresh = round(int(values['window']*.75))
                    print(obs_thresh)
                if (values['obs_thresh_6M']):
                    obs_thresh_6M = int(values['obs_thresh_6M'])
                else:
                    obs_thresh_6M = 5 if values['window'] >= 18 else 4
                    print(obs_thresh_6M)
                test_year = int(values['test_year'])
                test_month = int(values['test_month'])
                prod_col = values['prod_col']
                fac_col = values['fac_col']

                fac_id_col = fac_opts['fac_id_col']
                fac_dist_col = fac_opts['fac_dist_col']
                fac_reg_col = fac_opts['fac_reg_col']
                prod_id_col = prod_opts['prod_id_col']
                prod_cat_col = prod_opts['prod_cat_col']
                prod_psize_col = prod_opts['prod_psize_col']


                print(values.values())
                historical_months = num_historical_months(window_var, df[date_col], test_year, test_month)
                vars_list = ['date_col', 'cons_col', 'obs_thresh', 'obs_thresh_6M', 'test_year', 'test_month', 'prod_col', 'fac_col']


                
                if (historical_months < window_var):
                    window['error'].update(value = f"Data set does not include enough data preceding the test date to meet the requirements of the given window. \n This dataset includes {historical_months} months prior to the test date. Try reducing the window or using more historical data",visible = True)
                else:
                    c = ConsumptionAnomalies(data_path = path,date_col = date_col, cons_col = cons_col, window = window_var,
                                     obs_thresh = obs_thresh, obs_thresh_6M = obs_thresh_6M, test_year = test_year,
                                     test_month = test_month,prod_col = prod_col,prod_id_col=prod_id_col,prod_cat_col=prod_cat_col,
                                     prod_psize_col=prod_psize_col,fac_col = fac_col,fac_id_col=fac_id_col,fac_dist_col=fac_dist_col,fac_reg_col=fac_reg_col, quarterly = quarterly)
                    c.run_all_analysis_methods()
                    analysis_complete_window()
                    break
    window.close()


def product_window(prod_opts, options):
    sg.theme('DarkAmber')
    layout = [  
                [sg.Text('Parameter Selection: ')],

                [sg.Text('Select Product ID column: ', tooltip = 'Select the column providing the product ID (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='prod_id_col',
                tooltip = 'Select the column providing the product ID (optional)')],

                [sg.Text('Select Product Category column: ', tooltip = 'Select the column providing the product category (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='prod_cat_col', 
                tooltip = 'Select the column providing the product category (optional)')],

                [sg.Text('Select Product Pack Size column: ', tooltip = 'Select the column providing the product pack size (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='prod_psize_col', 
                tooltip = 'Select the column providing the product pack size (optional)')],


                [sg.Button('Submit'), sg.Button('Cancel')],
                [sg.Text('Select test year: ', visible = False, text_color = 'red')] ]

        
    window = sg.Window('Additional Product Settings', layout, size = (480,240), modal = True, finalize=True)


    window['prod_id_col'].update(value = prod_opts['prod_id_col'])
    window['prod_cat_col'].update(value = prod_opts['prod_cat_col'])
    window['prod_psize_col'].update(value = prod_opts['prod_psize_col'])
    while True:
        event, values = window.read()

        if event == "Cancel" or event == sg.WIN_CLOSED:
            window.close()
            return prod_opts
            break
        elif event == 'Submit':
            window.close()
            return {'prod_id_col': values['prod_id_col'], 'prod_cat_col': values['prod_cat_col'], 'prod_psize_col': values['prod_psize_col']}
            break
            

    window.close()


def facility_window(prod_opts, options):
    sg.theme('DarkAmber')
    layout = [  
                [sg.Text('Parameter Selection: ')],

                [sg.Text('Select Facility ID column: ', tooltip = 'Select the column providing the facility ID (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='fac_id_col',
                tooltip = 'Select the column providing the facility ID (optional)')],

                [sg.Text('Select Facility District column: ', tooltip = 'Select the column providing the facility district (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='fac_dist_col', 
                tooltip = 'Select the column providing the facility district (optional')],

                [sg.Text('Select Facility Region column: ', tooltip = 'Select the column providing the facility region (optional)')],
                [sg.Combo(values=list(options), size = (60,9), key='fac_reg_col', 
                tooltip = 'Select the column providing the facility region (optional)')],


                [sg.Button('Submit'), sg.Button('Cancel')],
                [sg.Text('Select test year: ', visible = False, text_color = 'red')] ]

        
    window = sg.Window('Additional Facility Settings', layout, size = (480,240), modal = True, finalize=True)


    window['fac_id_col'].update(value = prod_opts['fac_id_col'])
    window['fac_dist_col'].update(value = prod_opts['fac_dist_col'])
    window['fac_reg_col'].update(value = prod_opts['fac_reg_col'])
    while True:
        event, values = window.read()

        if event == "Cancel" or event == sg.WIN_CLOSED:
            window.close()
            return prod_opts
            break
        elif event == 'Submit':
            window.close()
            return {'fac_id_col': values['fac_id_col'], 'fac_dist_col': values['fac_dist_col'], 'fac_reg_col': values['fac_reg_col']}
            break
            

    window.close()


def num_historical_months(window, period_col, test_year, test_month):
    adj_date = pd.to_datetime(period_col).apply(lambda x: x.replace(day=1).date())
    all_dates = sorted(list(adj_date.unique()))
    test_date = pd.to_datetime(str(test_year)+'-'+str(test_month)+'-1').date()
    historical_dates = all_dates[:all_dates.index(test_date)]
    return len(historical_dates)


def save_config(configs, prod_opts, fac_opts, quarterly: bool):
    obj = (configs, prod_opts, fac_opts, quarterly)
    with open('preset.pickle', 'wb') as file:
        pickle.dump(obj, file, protocol = pickle.HIGHEST_PROTOCOL)

def load_config():
    with open('preset.pickle', 'rb') as handle:
        return pickle.load(handle)

def main():
    consumption_analysis_gui()

if __name__ == '__main__':
    main()

'''
self.input_data['Adjusted Date'] = self.input_data[self.date_col].apply(lambda x: x.replace(day=1))
all_dates = sorted(list(self.input_data['Adjusted Date'].unique()))
test_date = np.datetime64(str(self.test_year)+'-'+str(self.test_month)+'-01T00:00:00.000000000')
historical_dates = all_dates[:all_dates.index(test_date)]
if len(historical_dates) < self.window:
    raise ValueError(f"Data set does not include enough data preceding the test date to meet the requirements of the given window. This dataset includes {len(historical_dates)} months prior to the test date. Try reducing the window or using more historical data")
train_dates = historical_dates[-(self.window):]
keep_dates = train_dates + [test_date]
self.input_data = self.input_data[self.input
'''