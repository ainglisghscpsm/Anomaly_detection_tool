import pandas as pd
import numpy as np
import pathlib

class ConsumptionAnomalies():
    def __init__(self,data_path=None,data_sheet=0,date_col=None,cons_col=None,window=24,obs_thresh=12,obs_thresh_6M=5,test_year=None,test_month=None,fac_col=None,fac_id_col=None,fac_dist_col=None,fac_reg_col=None,prod_col=None,prod_id_col=None,prod_cat_col=None,prod_psize_col=None,prod_list=[],loc_level='Facility',samples_seasonality = 3,remove_zeros = True,exemptions=None,quarterly=None):
        self.data_path = data_path
        self.data_sheet = data_sheet

        # read in data frame and clean columns
        self.input_data = pd.read_excel(self.data_path,sheet_name = self.data_sheet) if self.data_path.suffix == '.xlsx' else pd.read_csv(self.data_path)
        #self.input_data = pd.read_excel(self.data_path,sheet_name = self.data_sheet) if ('.xlsx' in self.data_path) else pd.read_csv(self.data_path)
        self.input_data = self.input_data.rename(columns=lambda x: x.strip())

        self.date_col = date_col
        
        self.cons_col = cons_col
        
        self.window = window
        self.obs_thresh = obs_thresh
        self.obs_thresh_6M = obs_thresh_6M
        
        self.test_year = test_year
        self.test_month = test_month
        
        self.remove_zeros = remove_zeros
        self.quarterly = quarterly
        
        self.fac_col = fac_col
        self.fac_id_col = fac_id_col if fac_id_col else 'Facility ID'
        self.fac_dist_col = fac_dist_col if fac_dist_col else 'District'
        self.fac_reg_col = fac_reg_col if fac_reg_col else 'Region'
        
        self.prod_col = prod_col
        self.prod_id_col = prod_id_col if prod_id_col else 'Product ID'
        self.prod_cat_col = prod_cat_col if prod_cat_col else 'Product Category'
        self.prod_psize_col = prod_psize_col if prod_psize_col else 'Pack Size'
        
        self.prod_list = prod_list
        self.loc_level = loc_level
        
        self.samples_seasonality = samples_seasonality
        
        if exemptions:
            self.exempt = pd.read_excel(exemptions)
        else:
            self.exempt = pd.DataFrame(columns=[self.fac_col,self.prod_col,'Exemption_Period_Start','Exemption_Period_End','Exemption_Code'])

        self.group_by_list = [self.fac_col,self.prod_col]
        if self.quarterly:
            self.month_count_constant = self.test_year*4 + ((self.test_month-1)//3 + 1) - (window+1)
        else:
            self.month_count_constant = self.test_year*12 + self.test_month - (window+1)
        
        self.column_rename = {
            self.cons_col:'Consumption',
            self.date_col:'Period',
            self.fac_col:'Facility',
            self.fac_id_col:'Facility ID',
            self.fac_dist_col:'District',
            self.fac_reg_col:'Region',
            self.prod_col:'Product',
            self.prod_id_col:'Product ID',
            self.prod_cat_col:'Product Category',
            self.prod_psize_col:'Pack Size'}

    # set properties and perform initial data checks    
    @property
    def date_col(self):
        return self._date_col

    @date_col.setter
    def date_col(self, d):
        if not d:
            raise ValueError("Date column not specified")
        if not (d in self.input_data):
            raise ValueError(f"Column '{d}' is not in data")
        try:
            self.input_data[d] = pd.to_datetime(self.input_data[d])
        except:
            raise ValueError(f"Column '{d}' cannot be converted to date type")
        self._date_col = d

    @property
    def cons_col(self):
        return self._cons_col
        
    @cons_col.setter
    def cons_col(self, c):
        if not c:
            raise ValueError("Issue column not specified")
        if not (c in self.input_data):
            raise ValueError(f"Column '{c}' is not in data")
        self._cons_col = c
        
    @property
    def fac_col(self):
        return self._fac_col
        
    @fac_col.setter
    def fac_col(self, f):
        if not f:
            raise ValueError("Issue column not specified")
        if not (f in self.input_data):
            raise ValueError(f"Column '{f}' is not in data")
        self._fac_col = f

    @property
    def window(self):
        return self._window
    
    @window.setter
    def window(self, w):
        if not (type(w) == int): #and type(w[1] == int)):
            raise ValueError("Invalid window types - must be integer")
        self._window = w

    @property
    def input_data(self):
        return self._input_data

    @input_data.setter
    def input_data(self, df):
        self._input_data = df

    @property
    def loc_level(self):
        return self._loc_level

    @loc_level.setter
    def loc_level(self, l):
        if not (l in ['Facility', 'District', 'Region']): 
            raise ValueError("Invalid location type")
        self._loc_level = l

    def prep_data(self):
        
        # convert facility and date columns
        self.input_data[self.fac_col] = self.input_data[self.fac_col].str.strip().str.upper()
        self.input_data[self.prod_col] = self.input_data[self.prod_col].str.strip().str.upper()

        
        # filter for only months in sample
        self.input_data['Adjusted Date'] = self.input_data[self.date_col].apply(lambda x: x.replace(day=1))
        all_dates = sorted(list(self.input_data['Adjusted Date'].unique()))
        test_date = np.datetime64(str(self.test_year)+'-'+(str(self.test_month) if self.test_month >= 10 else '0'+str(self.test_month))+'-01T00:00:00.000000000')
        historical_dates = all_dates[:all_dates.index(test_date)]
        if len(historical_dates) < self.window:
            raise ValueError(f"Data set does not include enough data preceding the test date to meet the requirements of the given window. This dataset includes {len(historical_dates)} months prior to the test date. Try reducing the window or using more historical data")
        train_dates = historical_dates[-(self.window):]
        keep_dates = train_dates + [test_date]
        self.input_data = self.input_data[self.input_data['Adjusted Date'].isin(keep_dates)]
        
        # separate out month and year from date columns
        self.input_data['Month'] = self.input_data[self.date_col].apply(lambda x: x.month)
        self.input_data['Year'] = self.input_data[self.date_col].apply(lambda x: x.year)
        
        # create temporary extra product and facility info columns
        for extra_col in [self.fac_id_col,self.fac_reg_col,self.fac_dist_col,self.prod_id_col,self.prod_cat_col,self.prod_psize_col]:
            if not (extra_col in self.input_data):
                print(f'> Creating standin for {extra_col}')
                self.input_data[extra_col] = np.nan
        
        # grab all product related fields - create product master
        self.prod_master = self.input_data[[self.prod_col,self.prod_id_col,self.prod_cat_col,self.prod_psize_col]].drop_duplicates(subset=[self.prod_col])
        # grab all fac related fields - create fac master
        self.fac_master = self.input_data[[self.fac_col,self.fac_id_col,self.fac_reg_col,self.fac_dist_col]].drop_duplicates(subset=[self.fac_col])
        
        # filter for only necessary columns
        self.input_data = self.input_data[[self.fac_col,self.prod_col,'Month','Year',self.cons_col]]
        
        # remove consumption values of zero, if specified
        if self.remove_zeros:
            self.input_data = self.input_data[self.input_data[self.cons_col] > 0]


    def split_data(self):

        df = self.input_data
        
        df['Consumption_Quantity_Lag1'] = df.groupby(self.group_by_list)[self.cons_col].shift(1)        
        # Add a month count variable
        if self.quarterly:
            df['Month_Count'] = df['Year']*4 + ((df['Month']-1)//3 + 1) - self.month_count_constant
        else:
            df['Month_Count'] = df['Year']*12 + df['Month'] - self.month_count_constant 
        # Get the lag of month count for computations
        df['Month_Count_Lag1'] = df.groupby(self.group_by_list)['Month_Count'].shift(1)
        print('> New Variables Created')

        # Get the training dataset
        consumption_train = df.loc[(df['Month_Count'] > 0) & (df['Month_Count'] < (self.window+1))]
        # Get the test dataset
        consumption_test = df.loc[(df['Month_Count'] == (self.window+1))]
        print('> New Datasets Created')
        
        self.train = consumption_train
        print('train data shape: ',self.train.shape)
        self.test = consumption_test
        print('test data shape: ',self.test.shape)

    def compute_outliers(self):

        # Keep only the records in the specified product list; otherwise, use entire datase
        if not self.prod_list:
            df_subset = self.train.copy()
        else:
            df_subset = self.train[self.train[self.prod_col].isin(self.prod_list)]
        
        # Calculate 1st quantile
        df_subset_q1 = df_subset.groupby(self.group_by_list)[self.cons_col].quantile(0.25).reset_index().rename(columns={self.cons_col: 'Q1'})
        print('> Q1 Complete')
        # Calculate 3rd quantile
        df_subset_q3 = df_subset.groupby(self.group_by_list)[self.cons_col].quantile(0.75).reset_index().rename(columns={self.cons_col: 'Q3'})
        print('> Q3 Complete')
        # Merge 1st and 3rd quantiles
        df_quantile = pd.merge(df_subset_q1, df_subset_q3, how='inner', left_on=self.group_by_list, right_on=self.group_by_list)
        print('> Quantile Merge Complete')  
        # Calculate outlier upper and lower limits
        df_quantile['Outlier_Upper'] = df_quantile['Q3'] + 1.5*(df_quantile['Q3'] - df_quantile['Q1'])
        df_quantile['Outlier_Lower'] = np.maximum(0, df_quantile['Q1'] - 1.5*(df_quantile['Q3'] - df_quantile['Q1']))
        print('> Limits Complete')
        # Merge outlier information with original dataset
        df_subset_outlier = pd.merge(df_subset, df_quantile, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
        print('> Outliers Merge Complete')
        # Add outlier flags
        df_subset_outlier['Outlier'] = np.where((df_subset_outlier[self.cons_col] > df_subset_outlier['Outlier_Upper']) | (df_subset_outlier[self.cons_col] < df_subset_outlier['Outlier_Lower']), 1, 0)
        df_subset_outlier['Outlier_Lag1'] = df_subset_outlier.groupby(self.group_by_list)['Outlier'].shift(1)
        print('> Subset Complete')
        
        self.outliers = df_subset_outlier

    def compute_parameters(self):
    
        # I-MR CHART PARAMETERS
        # ====================
        # If records are not in consecutive months or if the current/previous month consumption is an outlier, a range cannot be calculated
        self.outliers['Range'] = np.where((self.outliers['Outlier'] == 1) | (self.outliers['Outlier_Lag1'] == 1), \
            np.NaN, abs(self.outliers[self.cons_col] - self.outliers['Consumption_Quantity_Lag1']))
        #####################
        # Calculate x-bar parameters for non-outliers only
        df_subset_outlier_xbar = self.outliers.loc[(self.outliers['Outlier'] == 0)].groupby(self.group_by_list)[self.cons_col].mean().reset_index().rename(columns={self.cons_col: 'X_Bar'})
        # Calculate r-bar parameters for non-outliers only    
        df_subset_outlier_rbar = self.outliers.loc[(self.outliers['Outlier'] == 0)].groupby(self.group_by_list)['Range'].mean().reset_index().rename(columns={'Range': 'R_Bar'})
        # Merge x-bar and r-bar parameters
        df_subset_outlier_bars = pd.merge(df_subset_outlier_xbar, df_subset_outlier_rbar, how='inner', left_on=self.group_by_list, right_on=self.group_by_list)
        # Calculate x-bar and r-bar upper and lower limits
        df_subset_outlier_bars['X_UCL'] = df_subset_outlier_bars['X_Bar'] + (3*df_subset_outlier_bars['R_Bar'])/1.128
        df_subset_outlier_bars['X_LCL'] = np.maximum(0, df_subset_outlier_bars['X_Bar'] - (3*df_subset_outlier_bars['R_Bar'])/1.128)
        df_subset_outlier_bars['R_UCL'] = 3.267*df_subset_outlier_bars['R_Bar']
        # Merge x-bar and r-bar information with original dataset
        df_subset_temp = pd.merge(self.outliers, df_subset_outlier_bars, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
        # Get non-zero dataset
        df_subset_temp_non_zero = df_subset_temp[df_subset_temp[self.cons_col] > 0]
        print('> Parameters Complete')
        
        # QC PARAMETERS
        # ====================
        # Each product combination should only have one unique x-bar and r-bar
        df_subset_temp_qc = df_subset_temp.groupby(self.group_by_list)[['X_Bar','R_Bar']].nunique().reset_index()
        # Output any combination with more than 1 x-bar or 1 r-bar
        df_subset_temp_qc_counts = df_subset_temp_qc.loc[(df_subset_temp_qc['X_Bar'] > 1) | (df_subset_temp_qc['R_Bar'] > 1)]
        # Check if there are any duplicate product combinations
        df_subset_temp_qc['Key'] = df_subset_temp_qc.drop(['X_Bar','R_Bar'], axis=1).sum(axis=1)
        # Find duplicate keys
        df_subset_temp_qc_combos_temp = df_subset_temp_qc['Key']
        df_subset_temp_qc_combos = df_subset_temp_qc_combos_temp[df_subset_temp_qc_combos_temp.duplicated(keep=False)]
        print('> QC Complete')

        # COUNTS
        # ====================
        # Calculate number of valid samples (including zeroes)
        zero_obs = df_subset_temp.loc[(df_subset_temp['Outlier'] == 0)].groupby(self.group_by_list)['Month_Count'].nunique().reset_index().rename(columns={'Month_Count': 'Total_Month_Obs'})
        # Calculate number of valid samples (excluding zeroes)
        non_zero_obs = df_subset_temp_non_zero.loc[(df_subset_temp['Outlier'] == 0)].groupby(self.group_by_list)['Month_Count'].nunique().reset_index().rename(columns={'Month_Count': 'Total_Non_Zero_Month_Obs'})
        # Number of valid samples (including zeroes) in last 6 months
        critical_obs = df_subset_temp.loc[((df_subset_temp['Month_Count'] >= self.obs_thresh) & (df_subset_temp['Month_Count'] <= (self.obs_thresh+2))) & (df_subset_temp['Outlier'] == 0)] \
            .groupby(self.group_by_list)['Month_Count'].nunique().reset_index().rename(columns={'Month_Count': 'Total_Valid_Obs_Year_Prior'})

        last_6_months_obs = df_subset_temp.loc[(df_subset_temp['Month_Count'] > (self.window - 6)) & (df_subset_temp['Outlier'] == 0)] \
            .groupby(self.group_by_list)['Month_Count'].nunique().reset_index().rename(columns={'Month_Count': 'Total_Valid_Obs_Last_6M'})
        # Merge obs datasets
        obs_1 = pd.merge(zero_obs, non_zero_obs, how='outer', left_on=self.group_by_list, right_on=self.group_by_list)
        obs_2 = pd.merge(obs_1, last_6_months_obs, how='outer', left_on=self.group_by_list, right_on=self.group_by_list)
        obs_3 = pd.merge(obs_2, critical_obs, how='outer', left_on=self.group_by_list, right_on=self.group_by_list)

        # Create final dataset
        df_subset_window = pd.merge(df_subset_temp, obs_3, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
        print('> Counts Complete')
        
        # UNIQUE COMBINATIONS
        # ====================    
        # List of variables to drop in train data
        drop_list_subset_window = ['Year','Month',self.cons_col,'Consumption_Quantity_Lag1','Month_Count','Month_Count_Lag1','Outlier','Outlier_Lag1','Range']
        # Get list of product combinations and their associated parameters
        df_subset_unique = df_subset_window.drop(drop_list_subset_window, axis=1).drop_duplicates()
        print('> Train Data Complete')
        
        self.df_subset_window = df_subset_window
        self.df_subset_unique = df_subset_unique
        self.df_qc_counts = df_subset_temp_qc_counts
        self.df_qc_combos = df_subset_temp_qc_combos
        

    def score_test_data(self):
    
        # List of variables to keep in test data
        keep_list = self.group_by_list + [self.cons_col,'Consumption_Quantity_Lag1','Month_Count','Month_Count_Lag1']
        # Keep only specified variables and products in test data
        if not self.prod_list:
            df_test_reduced = self.test.copy()[keep_list]
        else:
            df_test_reduced = self.test[self.test[self.prod_col].isin(self.prod_list)][keep_list]
        # Merge test data with unique combination parameters for scoring
        df_test = pd.merge(df_test_reduced, self.df_subset_unique, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
        # Add investigation month    
        df_test['Investigation_Month'] = str(self.test_year) + '-' + str(self.test_month) + '-1'
        df_test['Investigation_Month'] = pd.to_datetime(df_test['Investigation_Month'])
        # Calculate outlier information and range for test data
        df_test['Outlier'] = np.where((df_test[self.cons_col] > df_test['Outlier_Upper']) | (df_test[self.cons_col] < df_test['Outlier_Lower']), 1, 0)
        df_test['Outlier_Lag1'] = np.where((df_test['Consumption_Quantity_Lag1'] > df_test['Outlier_Upper']) | (df_test['Consumption_Quantity_Lag1'] < df_test['Outlier_Lower']), 1, 0)
     
        df_test['Range'] = np.where((df_test['Outlier_Lag1'] == 1), \
            np.NaN, abs(df_test[self.cons_col] - df_test['Consumption_Quantity_Lag1']))
        ####################
        print('> Test Data Complete')
       
        # ADD EXEMPTIONS
        # ====================    
        # Because exemptions are only by facility-product combinations, a new variable needs to be created
        # if the group-by value is not facility code
        if self.loc_level != 'Facility':
            self.exempt[self.loc_level] = np.NaN
            # Keep only the group-by variables and exemption variables (5 total)
            df_keep_vars = self.group_by_list + ['Exemption_Period_Start','Exemption_Period_End','Exemption_Code']
            self.exempt = self.exempt[df_keep_vars]
            
        # Add exemptions to test dataset
        df_test_exempt = pd.merge(df_test, self.exempt, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
        print('> Exemptions Complete')
       
        # SCORE DATA
        # ====================
        # Add exemption variable
        df_test_exempt['Exemption_Status'] = np.where((df_test_exempt['Exemption_Period_Start'] <= df_test_exempt['Investigation_Month']) & (df_test_exempt['Investigation_Month'] <= df_test_exempt['Exemption_Period_End']), 'E', 'N')
        # Calculate if test data breaches I-MR charts
        df_test_exempt['X_Breach'] = np.where((df_test_exempt[self.cons_col] > df_test_exempt['X_UCL']) | (df_test_exempt[self.cons_col] < df_test_exempt['X_LCL']), 1, 0)
        df_test_exempt['R_Breach'] = np.where((df_test_exempt['Range'] > df_test_exempt['R_UCL']), 1, 0)
        # Create anomaly code
        def anomaly_code(df):
            if ((df['X_Breach'] == 1) and (df['R_Breach'] == 1)):
                return 'XR'
            elif ((df['X_Breach'] == 1) and (df['R_Breach'] == 0)):
                return 'X'
            elif ((df['X_Breach'] == 0) and (df['R_Breach'] == 1)):
                return 'R'
            elif ((df['X_Breach'] == 0) and (df['R_Breach'] == 0)):
                return np.NaN
        df_test_exempt['Anomaly_Code'] = df_test_exempt.apply(anomaly_code, axis=1)

        def high_low_breach(df):
            if (df['X_Breach'] == 1) and (df[self.cons_col] > df['X_UCL']):
                return 'High'
            elif (df['X_Breach'] == 1) and (df[self.cons_col] < df['X_LCL']):
                return 'Low'
            elif (df['R_Breach'] == 1):
                return 'High'
            else:
                return np.NaN
        df_test_exempt['High_Low_Breach'] = df_test_exempt.apply(high_low_breach, axis=1)
        print('> Score Complete')
        print('Before filter:')
        print(df_test_exempt)
        # FILTER DATA
        # ====================
        # Flag valid combinations
        print("Validity Criteria:")
        print(df_test_exempt[['Total_Month_Obs','Total_Valid_Obs_Last_6M','Total_Valid_Obs_Year_Prior']])
        print(df_test_exempt['Total_Month_Obs'].unique())
        print(df_test_exempt['Total_Valid_Obs_Last_6M'].unique())
        print(df_test_exempt['Total_Valid_Obs_Year_Prior'].unique())
        df_test_exempt['Valid_Combo'] = np.where((df_test_exempt['Total_Month_Obs'] >= self.obs_thresh) & (df_test_exempt['Total_Valid_Obs_Last_6M'] >= self.obs_thresh_6M) & (df_test_exempt['Total_Valid_Obs_Year_Prior'] == self.samples_seasonality), 1, 0)
        # Get valid combinations only
        df_valid_obs = df_test_exempt.loc[(df_test_exempt['Valid_Combo'] == 1)]
        print('valid combos:')
        print(df_valid_obs)
        # Keep only breaching test data
        df_valid_breach = df_valid_obs.loc[df_valid_obs['Anomaly_Code'].isin(['X','R','XR'])]
        print('breaching')
        print(df_valid_breach)
        print('> Filter Complete')
        
        # RANK DATA
        # ====================
        # Calculate anomaly deviations;
        def calc_x_delta(df):   
            if (df[self.cons_col] > df['X_UCL']):
                if (df['X_Bar'] == 0):
                    return 1 
                else:
                    return (df[self.cons_col] - df['X_UCL'])/df['X_Bar']
            elif (df[self.cons_col] < df['X_LCL']):
                if (df['X_Bar'] == 0):
                    return 1
                else:
                    return (df['X_LCL'] - df[self.cons_col])/df['X_Bar']
            else:
                return np.NaN
        def calc_r_delta(df): 
            if (df['Range'] > df['R_UCL']):
                if (df['R_Bar'] == 0):
                    return 1
                else:
                    return (df['Range'] - df['R_UCL'])/df['R_Bar']
            else:
                return np.NaN
        print(df_valid_breach)
        df_valid_breach['X_Delta'] = df_valid_breach.apply(calc_x_delta, axis=1)
        df_valid_breach['R_Delta'] = df_valid_breach.apply(calc_r_delta, axis=1)
        # Calculate anomaly rankings
        df_valid_breach['X_Rank'] = df_valid_breach['X_Delta'].rank(ascending=False)
        df_valid_breach['R_Rank'] = df_valid_breach['R_Delta'].rank(ascending=False)
        
        # MERGE DETAILS
        # ====================
        # Main output variables
        output_variables_main = [self.cons_col,'Range','Anomaly_Code','High_Low_Breach','X_Rank','R_Rank', \
                                'Exemption_Status','Exemption_Code','Exemption_Period_Start','Exemption_Period_End', \
                                'Outlier','Q1','Q3','Outlier_Upper','Outlier_Lower','X_Bar','R_Bar','X_UCL','X_LCL','R_UCL', \
                                'Total_Month_Obs','Total_Non_Zero_Month_Obs','Total_Valid_Obs_Last_6M','Total_Valid_Obs_Year_Prior']
        
        # Merge details onto valid dataset
        if self.loc_level != 'Facility':
            #df_valid_breach_merge = pd.merge(df_valid_breach, df_test_details_other, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
            output_variables_all = ['Investigation_Month',self.fac_col,self.prod_col] + output_variables_main
        else:
            #df_valid_breach_merge = pd.merge(df_valid_breach, df_test_details_facility, how='left', left_on=self.group_by_list, right_on=self.group_by_list)
            output_variables_all = ['Investigation_Month',self.fac_col,self.prod_col] + output_variables_main
       
        # Drop variables
        df_valid_final = df_valid_breach[output_variables_all]
        
        # Check for duplicate combinations
        df_qc_combos_final = df_valid_final[df_valid_final.duplicated(self.group_by_list, keep=False)]
        
        print('> Rank Complete')
                
        # FREQUENCIES
        # ====================
        # Find number of valid and invalid product combinations by the group-by list
        df_valid_product_counts = pd.crosstab(index=df_test_exempt[self.prod_col], columns=df_test_exempt['Valid_Combo']).rename(columns={0: 'Invalid', 1:'Valid'}).reset_index()
        df_valid_group_by_counts = pd.crosstab(index=df_test_exempt[self.fac_col], columns=df_test_exempt['Valid_Combo']).rename(columns={0: 'Invalid', 1:'Valid'}).reset_index()   
        # Get total combinations
        df_valid_product_counts['Total'] = df_valid_product_counts['Invalid'] + df_valid_product_counts['Valid']
        df_valid_group_by_counts['Total'] = df_valid_group_by_counts['Invalid'] + df_valid_group_by_counts['Valid']
        # Get percent valid combinations
        df_valid_product_counts['% Valid'] = df_valid_product_counts['Valid']/df_valid_product_counts['Total']
        df_valid_group_by_counts['% Valid'] = df_valid_group_by_counts['Valid']/df_valid_group_by_counts['Total']
        # Find number of anomalies by product code and type of anomaly
        df_anomaly_product_counts = pd.crosstab(index=df_valid_final[self.prod_col], columns=df_valid_final['Anomaly_Code']).reset_index()
        df_anomaly_group_by_counts = pd.crosstab(index=df_valid_final[self.fac_col], columns=df_valid_final['Anomaly_Code']).reset_index()
        # Merge all counts
        df_product_counts_final = pd.merge(df_valid_product_counts, df_anomaly_product_counts, how='left', left_on=[self.prod_col], right_on=[self.prod_col])
        df_group_by_counts_final = pd.merge(df_valid_group_by_counts, df_anomaly_group_by_counts, how='left', left_on=[self.fac_col], right_on=[self.fac_col])    
        # Get total anomalies
        # Guarantee all anomaly codes have a column
        for anomaly_code1 in [i for i in ['X','XR','R'] if i not in df_product_counts_final.columns]:
            df_product_counts_final[anomaly_code1]=np.nan
        for anomaly_code2 in [i for i in ['X','XR','R'] if i not in df_group_by_counts_final.columns]:
            df_group_by_counts_final[anomaly_code2]=np.nan
        df_product_counts_final['Total Anomalies'] = df_product_counts_final['X'].fillna(0) + df_product_counts_final['R'].fillna(0) + df_product_counts_final['XR'].fillna(0)
        df_group_by_counts_final['Total Anomalies'] = df_group_by_counts_final['X'].fillna(0) + df_group_by_counts_final['R'].fillna(0) + df_group_by_counts_final['XR'].fillna(0)
        print('> Frequencies Complete')
        
        self.df_investigation = df_valid_final
        self.df_qc_combos_final = df_qc_combos_final
        self.df_freq_by_product = df_product_counts_final
        self.df_freq_by_group_by = df_group_by_counts_final
    
    def output_tables(self):
        # map back on master product and facility info
        self.df_investigation = self.df_investigation.merge(self.prod_master,how='left',left_on=self.prod_col,right_on=self.prod_col)
        self.df_investigation = self.df_investigation.merge(self.fac_master,how='left',left_on=self.fac_col,right_on=self.fac_col)
        
        self.df_freq_by_product = self.df_freq_by_product.merge(self.prod_master,how='left',left_on=self.prod_col,right_on=self.prod_col)
        self.df_freq_by_group_by = self.df_freq_by_group_by.merge(self.fac_master,how='left',left_on=self.fac_col,right_on=self.fac_col)
        
        # rename column names to standardize
        self.df_investigation = self.df_investigation.rename(columns=self.column_rename)
        self.df_freq_by_group_by = self.df_freq_by_group_by.rename(columns=self.column_rename)
        self.df_freq_by_product = self.df_freq_by_product.rename(columns=self.column_rename)
        self.df_qc_combos_final = self.df_qc_combos_final.rename(columns=self.column_rename)
        
        # write tables to files
        self.df_investigation.to_csv('investigation.csv',index=None,mode='w')
        self.df_qc_combos_final.to_csv('quality_control.csv',index=None,mode='w')
        self.df_freq_by_product.to_csv('freq_by_product.csv',index=None,mode='w')
        self.df_freq_by_group_by.to_csv('freq_by_facility.csv',index=None,mode='w')
        print('> Tables written')
        
    def run_all_analysis_methods(self):
        self.prep_data()
        self.split_data()
        self.compute_outliers()
        self.compute_parameters()
        self.score_test_data()
        self.output_tables()
        
        