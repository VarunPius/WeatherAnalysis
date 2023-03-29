######################################################################################################################################################
# Code Info                                                                                                                                          #
#                                                                                                                                                    #
# routes.py                                                                                                                                          #
# Author(s): Varun Pius Rodrigues                                                                                                                    #
# About: Routes for FastAPI are written here                                                                                                         #
######################################################################################################################################################


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports goes here
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# Internal modules
from src import app
from src import data_parser, data_analyser


######################################################################################################################################################
# Code starts here
######################################################################################################################################################

'''
Route for Home URL
Base URL: localhost:8001
'''
@app.get("/")
async def home():
    return {"Hello":"World"}


'''
Route to get data from OpenWeather
API Request: GET
URI: localhost:8001/getdata
'''
@app.get("/getdata")
async def get_data():
    print("Inside get data")
    data_parser.start()
    print("done get data")
    return {"Status":"Data Obtained"}


'''
Route to get results from storage
API Request: GET
URI: localhost:8001/getresults
'''
@app.get("/getresults")
async def get_results():
    print("Inside get results")
    result_df1, result_df2 = data_analyser.process_dataframe()
    print("done get Results")

    return result_df1, result_df2

