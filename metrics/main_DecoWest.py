# By Sahil Nagpal

from os import read
import pandas as pd
import numpy as np

import pyodbc

import pycomm3
import time

import matplotlib.pyplot as plt

import re

from datetime import datetime
from datetime import timedelta
from datetime import date

import math

from scipy.optimize import curve_fit



# Variables

ADRESS = '172.19.11.38'
TABLE = 'DW-FcDbExporter'
SCHEDULE_TABLE = 'machine_performances'

class Graph:
  def __init__(self, df):
    self.df =  df

    fig = plt.figure()
    axes = fig.add_axes([0.1,0.1,0.8,0.8])

    axes.plot(self.df['production_day'],self.df['oee'],'b')
    plt.show()




class Line_Metrics:
  '''
  Class used to summarize manufacturing Performance Metrics from dataframe, such as
    - OEE
    - Downtime Top Hitters
  '''
  
  def OEE(self, df, time_frame, line_name, shift):
    '''
    Method used to summarize OEE from a certain time frame.

    df: Dataframe containing OEE values
    time_frame: an array of start and end dates in the format ['2020-12-18', '2020-12-25'] 
    line_name: string, for the line name or station for which OEE is required.

    returns average OEE, highest OEE and lowest OEE observed in the timeframe and what date they occured.
    '''

    timeFrame = pd.date_range(time_frame[0],time_frame[1])

    df = df[(df['machine_name'] == line_name) & (df['shift_number'] == shift)]

    df['production_day'] = df['production_day'].apply(lambda x: pd.to_datetime(x))

    df_2 = df[(df['production_day'].isin(timeFrame)) & (df['time_running'] != 0) ][['oee','production_day']]
    
    
    return Graph(df_2)


class Plc_AB:
  '''
    Class initializes Allen Bradelly PLC.
    Use read_tag and write_tag methods, to read a sql table
  '''
  def __init__(self, ip_adress):
    '''
    ip_adress: Ip adress of the Allen Bradley PLC
    '''
    self.ip_adress = ip_adress
  
  def read_tag(self, tag_name):
    '''
      Read data from a tag.

      tag_name: name of tag

      returns: data type in tag.
    '''
    with pycomm3.LogixDriver(self.ip_adress) as plc:
      return plc.read('{}'.format(tag_name))

  def write_tag(self, tag_name, value):
    '''
      Write data to a tag.

      tag_name: name of tag
      value: value to write to tag

      returns: No return
    '''
    with pycomm3.LogixDriver(self.ip_adress) as plc:
      plc.write(('{}'.format(tag_name),value))
  
  def continuos_read(self, tag_name, interval = 1,index = 1):
    '''
      Continously read the data associated with the tag.

      tag_name: name of the tag.
      interval: time interval to read data in seconds
      index: Pick which index from the tag needs to be printed.

      returns: Continous print of the tag at the specified index.

    '''
    while True:
      print(self.read_tag(tag_name)[index])
      time.sleep(interval)
  
  def get_ip(self):
    '''
    Get current ip associated with the PLC instance.
    '''
    return 'This instance is connected to {}'.format(self.ip_adress)

  
class Sql (Line_Metrics):
  '''
    Class initilizes entire database.
    Use table_to_df(table_name) method to read a sql table to a dataframe.

    Example:
      sql = Sql('172.19.11.34','DE-FcDbExporter')
      df = sql.table_to_df('annotations')
      print(df)
  '''

  def __init__(self, server_name, database_name):
    '''
    server_name: Ip adress of the sql server
    database_name: Name of the sql database
    '''
    self.server_name = server_name
    self.database_name = database_name
  

  def create_connection(self):
    '''
      Sets-up connection with the SQL database. Do not manually call

      returns: connection object
    '''
    conn = pyodbc.connect('Driver={{SQL Server}};'
                          'Server={};'
                          'Database={};'
                          'Trusted_Connection=yes;'.format(self.server_name, self.database_name))
    return conn


  def table_to_df(self,table= None, sql_q = None):
    '''
      Method to read the specific table from SQL database to a dataframe.

      table: Name of specific table from database

      returns: pandas dataframe
    '''
    if (sql_q == None) and (table != None):
      sql_query = pd.read_sql_query('SELECT * FROM [{}].dbo.{}'.format(self.database_name,table),self.create_connection())
    
    elif sql_q != None:
      sql_query = pd.read_sql_query(sql_q,self.create_connection())

    return sql_query


class Failure_analysis:

  
  def __init__(self, equipment_name=None, component_regex=None):
    self.equipment_name = equipment_name
    self.component_regex = component_regex

    self.component_df()
    self.scd_df()



  def date_to_datetime(self, sentence, date_format = '%Y-%m-%d %H:%M:%S.%f'):

    if type(sentence) == str:
      return datetime.strptime(sentence, date_format)
    else:
      return sentence


  def component_df(self):
    #Comment DF
    print('Getting Component Df')
    s = Sql(ADRESS,TABLE)
    sql_query = '''SELECT dbo.annotations.comment, dbo.machine_activities.machine_id, dbo.machine_activities.start_time, dbo.machine_activities.end_time, dbo.machine_activities.duration, dbo.machines.name, dbo.machine_activities.downtime_code_id, dbo.downtime_codes.name as Name_2
                    FROM dbo.annotations
                    RIGHT JOIN dbo.machine_activities ON dbo.annotations.annotateable_id = dbo.machine_activities.id
                    INNER JOIN dbo.machines ON dbo.machine_activities.machine_id = dbo.machines.id
                    FULL JOIN dbo.downtime_codes ON dbo.machine_activities.downtime_code_id = dbo.downtime_codes.id
                    '''
      
    self.df = s.table_to_df(sql_q=sql_query)
    self.df.dropna(subset=['start_time', 'end_time'], inplace = True)
    self.df.dropna(subset=['machine_id', 'Name_2', 'comment'], how='all', inplace = True)


    #Make comments lower case
    self.df['comment'] = self.df['comment'].apply(lambda x: x.lower() if type(x) == str else x)
    self.df['Name_2'] = self.df['Name_2'].apply(lambda x: x.lower() if type(x) == str else x)

    if self.component_regex == None:
      return self.df

    else: 
      #Filter based on equipment and component
        #If equipment_name is none, the failure data of a component across all equipment will be considered.
      if self.equipment_name == None:
        self.df_2 = self.df[ 
                              (self.df['Name_2'].str.contains(r'{}'.format(self.component_regex), regex=True)) | 
                              (self.df['comment'].str.contains(r'{}'.format(self.component_regex), regex=True))
                            
                            ]
      
      elif self.equipment_name != None:
        self.df_2 = self.df[  (self.df['name'] == self.equipment_name) &
                              ( (self.df['Name_2'].str.contains(r'{}'.format(self.component_regex))) | 
                              (self.df['comment'].str.contains(r'{}'.format(self.component_regex))) )
                            
                            ]
      
      if self.df_2.shape[0] == 0:
        return self.df_2
    

      #Make Dates into Datetime Obejects
      self.df_2['start_time'] = self.df_2['start_time'].apply(self.date_to_datetime)
      self.df_2['end_time'] = self.df_2['end_time'].apply(self.date_to_datetime)

      print('Returning Component Df')

      return self.df_2

  
  def scd_df(self):

    if self.component_regex !=  None:
      print('Getting Schedule Df')
      s = Sql(ADRESS,TABLE)
      self.df_scd = s.table_to_df(SCHEDULE_TABLE)
      self.df_scd.dropna(subset = ['start_time'], inplace = True)

      #Dates to Datetime
      self.df_scd['start_time'] = self.df_scd['start_time'].apply(self.date_to_datetime) 
      self.df_scd['end_time'] = self.df_scd['end_time'].apply(self.date_to_datetime) 
      self.df_scd['production_day'] = self.df_scd['production_day'].apply(self.date_to_datetime, date_format = '%Y-%m-%d')

      print('Returning Schedule Df')
      return self.df_scd
    return self.df

  
  def time_between(self, date_1, date_2):
    dates = [date_1.date() + timedelta(days=i) for i in range((date_2.date() - date_1.date()).days)] 
    
    time_count = 0
    
    for date in dates:
        for i in self.df_scd.loc[(self.df_scd['production_day'] == date) & (self.df_scd['machine_name'] == self.equipment_name), 'time_running']:
            if i > 0.01:
                time_count += i
                
    return time_count/3600
  

  def bernards_approximation(self, df):
    df.reset_index(inplace=True)
    df['i'] = df.apply(lambda x: x.name +1, axis = 1)
    df["Bernard's Approximation"] = df['i'].apply(lambda x: (x-0.3)/(df.shape[0]+0.4))

    return df

  
  def weibull(self, df):
    df['x'] = df['Failure_times(hours)'].apply(lambda x: math.log(x))
    df['y'] = df["Bernard's Approximation"].apply(lambda x: math.log(math.log(1/(1-x))))

    return df

  def get_failure_data(self):
    #Flag to check if the df is empty or not.

    if self.df_2.shape[0]<5:
      return self.df_2
    
    times_diff = []

    for i in range(self.df_2.shape[0]):
        try:
            times_diff.append(self.time_between(self.df_2.iloc[i]['end_time'], self.df_2.iloc[i+1]['start_time']))
        except IndexError:
            break
    
    times_diff.append(np.nan)
    self.df_2['Failure_times(hours)'] = times_diff
    
    self.df_2.sort_values(by = 'Failure_times(hours)', inplace = True)

    self.df_2 = self.df_2[self.df_2['Failure_times(hours)'] != 0]

    self.bernards_approximation(self.df_2)

    self.weibull(self.df_2)

    #Get current Operational Time of the component. This references the schedule df.
    self.df_2.loc[:, 'Operational Time'] = self.time_between(self.df_2['end_time'].max(), pd.to_datetime(date.today()))


    # To get the line of best fit for the linear transformed weibull
    x = self.df_2['x'][0:-1]
    y = self.df_2['y'][0:-1]

    def objective(x, a, b):
        return a * x + b
    
    try:
      # curve fit
      popt, _ = curve_fit(objective, x, y)
      # summarize the parameter values
      a, b = popt
      print('y = %.5f * x + %.5f' % (a, b))


      #Weibull Parameters (alpha and beta) 
      beta = a
      l = math.exp(-b/beta)


      # Values to graph the Weibull curve. This is outputted to a csv to graph in Power Bi.
      #x_vals = np.arange(min(self.df_2['Failure_times(hours)']), max(self.df_2['Failure_times(hours)']),1)
      #failure = [1 - math.exp(-(v/l)**beta) for v in x_vals]  

      #failure_curve = pd.DataFrame(data = {'x_vals':x_vals, 'y_vals': failure})

      #failure_curve.to_csv('{}_{}.csv'.format(self.equipment_name, re.sub(r'[^\w\s]','',self.component_regex)))

      # Conditional Probability calculation.
      # rel_plus_24_hours = math.exp(-((self.df_2['Operational Time'][0]+240)/l)**beta)
      rel_at_current_time = math.exp(-((self.df_2['Operational Time'][0]+0)/l)**beta)

      self.df_2.loc[:,'Probability of Failure'] = round(1-rel_at_current_time, 2)
    
    # self.df_2.loc[:,'Probability of Failure'] = round((((1-rel_plus_24_hours)-(1-rel_at_current_time))/(rel_at_current_time) ), 2)

    except ValueError:
      pass

    return self.df_2

  
  def get_comment_df(self):
    return self.df

