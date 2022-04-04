import re
import string
import pandas as pd
import numpy as np


regex_test = 'weld\sid(\s|\d)?\d+(\s?\s?\s?\d+)?(\s?\s?\s?\d+)?(\s?\s?\s?\d+)?(\s?\s?\s?\d+)?(\s?\s?\s?\d+)?(\s?\s?\s?\d+)?'
print('here')

# Global Variables
assembly_lines = ["ST130 Interlock WLM01",
                  "ST130 Interlock WLM02",
                  "ST130 Interlock WLM03"]

# File Save Location
location = r"\\magna.global\dco\Open_Share\DECO WEST\PowerBI Data\Raw Data\weld_id_DW.csv"

class RunAnalysis:
  def __init__(self, filtered_df_name) -> None:  

    # Main Dataframe
    self.filtered_df = pd.read_csv(r"\\magna.global\dco\Open_Share\DECO WEST\PowerBI Data\Raw Data\{}.csv".format(filtered_df_name)) 
    print('here_2')
    # Save Location
    self.file_location = location

    # Run
    self.runAll() 


  def runAll(self):
    self.remove_nan_comments()
    self.remove_punctuation()
    self.save_df(self.extract_weld_id())

  def remove_nan_comments(self):
    self.filtered_df = self.filtered_df.dropna(subset = ['comment'])
    
  def remove_punctuation(self):
    
    self.filtered_df['comment'] = self.filtered_df['comment'].apply(lambda x: re.compile('[%s]' % re.escape('!"#$%&()*+,-/:;<=>?@[\\]^_`{|}~')).sub(' ', x))

  def extract_weld_id(self):
    weld_id_df = pd.DataFrame(columns=['weld_id', 'start_time', 'station', 'line', 'comment'])

    # Iterate through the rows and get the weld ids.
    for row in self.filtered_df.iterrows():
      # Comment => row [1][1]
      #start_date => row[1][3]
      # station => row[1][10]
      # line Name => row[1][6]
      weld_ids = RunAnalysis.getPattern(row[1][1])

      if len(weld_ids) != 0:
        for weld_id in weld_ids:
          if float(weld_id) < 400:
            df = pd.DataFrame(data = [[float(weld_id), row[1][3], row[1][10], row[1][6], row[1][1]]], columns = ['weld_id', 'start_time', 'station', 'line','comment'])
            weld_id_df = pd.concat([weld_id_df,df])
    
    return weld_id_df
  
  def save_df(self,df):
    df.to_csv(location)
      

  @staticmethod
  def getPattern(comment):
    weld_ids = []
    pattern = re.compile(r'weld\sid\s?(\d+(\.\d+)?)\s?(and)?(\d+(\.\d+)?)?\s?(and)?(\d+(\.\d+)?)?\s?(and)?(\d+(\.\d+)?)?\s?(and)?(\d+(\.\d+)?)?\s?(and)?(\d+(\.\d+)?)?\s?(and)?(\d+(\.\d+)?)?')
    try:
      matches = pattern.finditer(comment)
    except:
      print(comment)

    for match in matches:
      pattern_2 = re.compile(r'\d+(\.\d+)?')
      matches_2 = pattern_2.finditer(match[0])

      for match_2 in matches_2:
        weld_ids.append(match_2[0])
    return weld_ids

# Trial

RunAnalysis('data_DW')

# print(RunAnalysis.getPattern('weld id 2'))
