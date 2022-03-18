import manufacturing as manu
import pandas as pd


# Global Variables
IP_ADDRESS = '172.19.11.38'
DATABASE_NAME = 'DW-FcDbExporter'
TABLE_NAME = 'machine_performances'

LOCATION = r"\\magna.global\dco\Open_Share\DECO WEST\PowerBI Data\Raw Data\dw_metrics.csv"

class RunMetrics:
  def __init__(self):

    # Initialize Connection
    conn = manu.Sql(IP_ADDRESS, DATABASE_NAME)
    

    # Fetch Dataframe
    self.df = conn.table_to_df(TABLE_NAME)

    self.saveFile(self.getMetrics())

  
  def getMetrics(self):
    metrics_df = self.df.loc[(self.df['utilization']> 0.2) & (self.df['oee'] <= 1),:]

    return metrics_df

  def saveFile(self, df):
    df.to_csv(LOCATION)

    
    