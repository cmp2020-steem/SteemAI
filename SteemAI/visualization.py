import pandas as pd

from Downloading import SteemSQL
from SteemAI import DeepPreprocess as dp

db = SteemSQL.SSQL()
data = dp.get_dataframe()
data = data.iloc[:182000]
corr = data.corr()['total_value']

pd.set_option('display.max_rows', None)
print(corr)