# WeatherAnalysis
Weather Analysis using OpenWeatherMap APIs

# Objective
This Workflow is in Python and fulfills the following requirements:
- Extract the last 5 days of data from the free API: https://openweathermap.org (Historical weather data) from 10 different locations to choose by me.
- Build a repository of data where we will keep the data extracted from the API.
    This repository should only have deduplicated data. Idempotency should also be guaranteed on the repository
    I store the data with the `data` directory as I am working with only 5 days of data.
- Build another repository of data that will contain the results of the following calculations from the data stored in step 2.
    - A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
    - A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.


# Running
To run the project, you need to have **Docker** installed in your system. Next do the following:
- Clone this project from Github
- Replace the value of `API_Key` in `resources/config.yml` with the one obtained from **OpenWeather**
- Run Docker compose file as follows:
    ```
    docker-compose up --build
    ```
- Go to the following location in your API client (or browser) to check if FastAPI/Uvicorn Gateway is up
    You should see `{"Hello":"World"}` as output
- Go to the following location to start pulling data from OpenWeather: http://localhost:8001/getdata
- Go to the following location to get the results: http://localhost:8001/getresults

# Considerations:
Since the volume of data is small (10 cities and 5 days of data), I have stored the data in JSON format locally. In production scenario, I would build a pipeline to store data in either Parquet or Avro format. Data Analysis would be done in Spark cluster with this data (rather than Pandas).

