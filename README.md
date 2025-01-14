# Cross_check
The idea is to use the NHL data set of players, but look for their appearance in Movies and Tv.
https://www.hockeydb.com/ihdb/players/player_ind_a.html is used to get information about current and former players. 
Then an api like https://www.omdbapi.com/, https://developer.themoviedb.org/reference/person-movie-credits or similar is used to gather information about each person to populate the database. 
This would create a database capable of answering questions such as "which players appear in television?", "what years contained the most appearances of NHL players in movies?" and "which players have appeared tv shows and movies"?

# Ingestion
The data used in this project comes from two sources: 
1. https://www.hockeydb.com
2. https://developer.themoviedb.org/reference/person-movie-credits

The first DAG scrape_hockey_data is used to scrape data about all players who has played a minimum of one game in the NHL from https://www.hockeydb.com. The tables are split by the first letter of the last name so the scraper need to loop through all the letters. The resulting table is saved to a csv file.

# Staging
The second DAG process_hockey_data then cleans the scraped data by only saving the name and birthday of the players, removing any tuple where the birthday is missing, splitting first name and last name to different columns and reformatting the birthday to yyyy-mm-dd. The data is saved to a new csv file and the csv file containing the raw data from the ingestion zone is deleted.

The third DAG fetching_api_data uses the cleaned data to enrich the dataset with players appearance in entertaining media from the api https://developer.themoviedb.org/reference/person-movie-credits. To make sure that the data comes from the player and not a namesake, the players birthdate from the csv is checked against the birthdate from the api. If the birthdays match, the data is saved in a JSON file.
