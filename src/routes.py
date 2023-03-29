from src import app
from src import data_parser, data_analyser

@app.get("/")
async def home():
    return {"Hello":"World"}

@app.get("/getdata")
async def get_data():
    print("Inside get data")
    data_parser.start()
    print("done get data")
    return {"Status":"Data Obtained"}

@app.get("/getresults")
async def get_results():
    print("Inside get results")
    result_df1, result_df2 = data_analyser.process_dataframe()
    print("done get Results")

    return result_df1, result_df2

