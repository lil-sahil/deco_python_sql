import pandas as pd
import numpy as np

df = pd.read_csv("data.csv")

print(df['Name_2'].unique())
print(df[df['Name_2'] == ' shift start: overage '])