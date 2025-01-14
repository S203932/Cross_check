# Cross_check
The idea behing this project was to combine a dataset of current and former NHL players with a movie dataset, which allowed us to look for their appearance in movies and TV shows. This created a database capable of answering the questions:
1. How many players has appeared in television?
```
SELECT * FROM PLAYERS WHERE AMOUNT_TV > 0
```
2. Which year contained the most appearances of NHL players in movies?
```
SELECT EXTRACT(YEAR FROM RELEASE) AS year, COUNT(*) AS count FROM CREDITS GROUP BY EXTRACT(YEAR FROM RELEASE) ORDER BY count DESC
```
3. Which players has made atleast two appearances in both tv shows and movies?
```
SELECT * FROM PLAYERS WHERE AMOUNT_MOVIES > 1 AND AMOUNT_TV > 1
```


## Ingestion
The data used in this project comes from two sources:

1. https://www.hockeydb.com
2. https://developer.themoviedb.org/reference/person-movie-credits

The DAG data_ingestion_dag is used to scrape data about all players who has played a minimum of one game in the NHL from https://www.hockeydb.com. The first step in this DAG is to check if the connection to the website works. If it works, the scraper will loop through all the letters in the alphabet, since the tables are split by the first letter of the last name and then concatenate the resulting data and save it to a csv. If the connection doesn't work, the DAG will instead use a small downloaded subset of the data from the website. When the online or offline scraper has succeeded, the staging DAG will be triggered to start.

## Staging
The DAG data_staging_dag then process the data from the ingestion. In the first step, the data is cleaned by removing the unnecessary columns and any player where the birthday is missing. The second step splits the first and last name to separate columns and reformats the birthday to yyyy-mm-dd. The resulting data is saved to a new csv file and the csv file containing the raw data from the ingestion zone is deleted.

After this, the connection to the API https://developer.themoviedb.org/reference/person-movie-credits is tested. If it works, the processed data is used to enrich the dataset with players appearance in entertaining media from the API. To make sure that the data comes from the player and not a namesake, the players birthday from the csv is checked against the birthday from the api. If the birthdays match, the data is saved in a JSON file. If the connection doesn't work, a downloaded subset will be used.

## Production
The DAG sql_dag creates and populates the tables based on the data completed in the staging. It starts by instantiating the necessary tables; Players (which provides essential information about the players in general), Movies (which contains information about all the movies that any players have partaken in), TV (which contains information about what tv shows/programs the players have partaken in) and lastly Credits (which contains information about what players have played which roles in which movies and tv shows). 

After having instantiated the tables, it then populates the players table using the data gathered by scraping the site along with the entertainment data fetched from the api. Having populated the players table, it then populates the remaining tables using the players table along with the api data. 

As the data has already been gathered in previous stages it doesn't check whether it is online or not. 

All the tables are created and stored in an sql database using the star schema. Access to the database is through PostgreSQL.


## Setting up
1. Clone the repository.
2. Create a `.env` in `/Cross_check/dags` and add your API key to it:
```
TMDB_API_KEY=your_api_key_here
```
3. Start the container:
```
docker-compose up -d
```
4. Start postgres
```
docker compose exec airflow-webserver airflow connections add 'postgres_default' --conn-uri 'postgres://airflow:airflow@postgres:5432/airflow'
```
5. Open http://localhost:8080.
6. Run the DAGS manually, starting with data_ingestion_dag.


## Results
If it all have run successfully then one should be able to answer the initial questions using the queries and get similar answers to the following: 
1. How many players has appeared in television?
![alt text](https://github.com/S203932/Cross_check/blob/3e57b4dbab21f9c6a9a060776dd77018087a8c59/Images/Amount_in_tv.png)
2. Which year contained the most appearances of NHL players in movies?
![alt text](https://github.com/S203932/Cross_check/blob/3e57b4dbab21f9c6a9a060776dd77018087a8c59/Images/Years_most_apperances_movies.png)
3. Which players has made atleast two appearances in both tv shows and movies?
![alt text](https://github.com/S203932/Cross_check/blob/3e57b4dbab21f9c6a9a060776dd77018087a8c59/Images/Players_movies_tv_2.png)


