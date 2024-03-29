import manufacturing as manu
import pandas as pd
import re

# Global Variables
assembly_lines = ["ST130 Interlock WLM01",
                  "ST130 Interlock WLM02",
                  "ST130 Interlock WLM03",
                  "Flex 1 - Final",
                  "Flex 2 - Final",
                  "Flex 3 - Final",
                  "Flex 4 - Final",
                  "LX AWD - Final",
                  "Mazak 1",
                  "Mazak 2",
                  "Mazak 3",
                  "Mazak 6",
                  "Mazak 7",
                  "FM1",
                  "FM2",
                  "FM3",
                  "6084/85 Bushing",
                  "6066",
                  "6063",
                  "6062/86",
                  "6084/85",
                  "6077/78",
                  "6058/62/63/82",
                  "6086 Bushing",
                  "RU XMBR - Final",
                  "DSSF2",
                  "DSSF1",
                  "DSSF3",
                  "DSSF4",
                  ]

# File Save Location
location = r"/media/Data/Deco West/PowerBI Data/Raw Data/data_DW.csv"



class RunAnalysis:
  def __init__(self) -> None:  

    # Main Dataframe
    self.df = manu.Failure_analysis().df

    print(self.df)

    # Save Location
    self.file_location = location

    # Run
    self.runAll() 


  def runAll(self):
    filtered_data = self.filterData()
    filtered_data = self.changeComments(filtered_data)
    filtered_data['Station'] = filtered_data[['comment','Name_2']].apply( lambda x: self.getStation(*x), axis = 1)
    
    # Save the dataframe in specified file location
    filtered_data.to_csv(self.file_location)


  def filterData(self):
  
    
    df_filtered = self.df[ ( self.df['name'].str.contains('|'.join(assembly_lines)) ) & 
                          (self.df['Name_2'] != "not scheduled") ]

    df_filtered['year'] = df_filtered['start_time'].apply( lambda x: x.split('-')[0])

    df_filtered = df_filtered.loc[ ((df_filtered['year'] == '2021') | 
                                  ((df_filtered['year'] == '2022'))) , :]
    
    
    return df_filtered


  def handleComment(self, comment, reason_code):
    if ( (comment is None) and (reason_code is None) ):
      return "No Comment and No Reason Code"
    
    elif ( (comment is None) and (reason_code == 'down (unclassified)') ):
      return reason_code

    elif (comment is None):
      if (reason_code in ['lunch', 'breaks', 'break', 'shift start', 'shift end']):
        return "DELETED"
      else:
        return reason_code
    
    else:
      return reason_code

  
  def changeComments(self, df):
    print(df.head(5))

    df['Name_2'] = df[['comment', 'Name_2']].apply(lambda x: self.handleComment(x[0], x[1]), axis = 1)

    df = df[df['Name_2'] != 'DELETED']
    
    return df

  
  def getPattern(self,col):
    pattern = re.compile(r'(\bgrob|\bop|\bstn|\bstation)(\s|-|#|.|\d|op)(\s|-|#|op|stn)?\s?\d+[a-zA-Z]?')
    try:
      matches = pattern.finditer(col)
    except:
      print(col)

    for match in matches:
      pattern = re.compile(r'(\d+(a|b)?)')
      matches = pattern.finditer(match[0])

      for match in matches:
        return(match[0])
  
  # Extract station from comment
  def getStation(self, comment, reason_code):
    if comment== None and reason_code == None:
      return None

    # Check Reason Code
    if reason_code != None:
      station = self.getPattern(reason_code)

      if station == None and comment != None:
        return self.getPattern(comment)
    
      else:
        return station
  
