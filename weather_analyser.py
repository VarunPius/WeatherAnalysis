from src import data_parser
from src import data_analyser


data_parser.start()
df1, df2 = data_analyser.process_dataframe()
