import requests
from bs4 import BeautifulSoup

# Import DBHandler class which contains the methods to interact with both MySQL and Mongo database
from boxoffice import DBHandler


class MovieScraper:
    """This class contains the variables and methods to scrape webpages from imdb.com to create below -
    - For each genre of a predefined list of genres, create a collection of top 50 movie records in Mongo DB
    - For each movie record, get the metadata name value pair from imdb.com. This is done such way that
    future addition of metadata will get automatically scraped. Since Mongo DB does horizontal scaling,
    it can store future metadata as well.
    - While retrieving the movie records, create a list of key roles and names e.g. Directors, Writers, Actors.
    This data will be used to support the UI application. If there are new names found in imdb.com for the
    three key roles, MySQL DB will scale vertically."""

    def __init__(self):
        """This is the constructor class. It sets the 'headers' string to be used for page retrieval.
        It also sets the list of genres to retrieve."""

        # Set the User-Agent in headers. Site will reject request without this.
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko)'
                ' Chrome/109.0.0.0 Mobile Safari/537.36'}

        # Set the list of genres
        self.genres = [
            'Action',
            'Adventure',
            'Animation',
            'Biography',
            'Comedy',
            'Crime',
            'Drama',
            'Family',
            'Fantasy',
            'Film-Noir',
            'History',
            'Horror',
            'Music',
            'Musical',
            'Mystery',
            'Romance',
            'Sci-Fi',
            'Short',
            'Sport',
            'Superhero',
            'Thriller',
            'War',
            'Western'
        ]

    def scrape_movies(self):
        """This method will perform all the activities for the scarping and storing of data."""
        # Create instance of DBHandler class. This will call the __init__ method of the DBHandler
        # which initializes the connections to the databases.
        db_handler = DBHandler()

        # This is the page url which will show the top 50 movies for the genre passed in attribute.
        # The genre will be appended with this url.
        specific_genre_url_prefix = \
            "https://www.imdb.com/search/title/?title_type=feature&explore=genres&view=simple&genres="
        # Iterate through the list of genres.
        for specific_genre in self.genres:
            print("################ Starting Genre ->" + specific_genre)
            specific_genre = specific_genre.lower()
            # Append the genre to the page url to form the complete url.
            specific_genre_url = specific_genre_url_prefix + specific_genre

            # Open the url to get page data
            r = requests.get(specific_genre_url, headers=self.headers)
            # Read the URL content and create a BeautifulSoup object. Using html.parser whereas
            # other parsers can be used as well
            page_soup = BeautifulSoup(r.content,
                                      'html.parser')

            # Get the parent div tag which contains the list of movie names for the genre
            page_div_parent = page_soup.find_all('div', attrs={'class': 'lister-list'})[0]

            # Get the list of divs containing the movie names
            page_div_movies = page_div_parent.find_all('div', attrs={'class': 'lister-item mode-simple'})

            # Create the collection name for the genre
            genre_collection_name = specific_genre + "boxoffice"
            # Create the Mongo DB collection with the genre-specific name.
            db_handler.create_movie_genre_collection(genre_collection_name)

            # Iterate through the list of divs for the movies
            for page_movie_tag in page_div_movies:
                # Create a dictionary for the movie details that will be stored into db
                movie_dict = {}
                # Get tag for one row in the list
                movie_tag = page_movie_tag.find_all('div', attrs={'class': 'col-title'})[0]
                # Get the movie href
                movie_url = "http://www.imdb.com" + movie_tag.find('a').get("href")
                # Get the movie rank in the selected genre and store in the dictionary
                movie_rank = movie_tag.find('span',
                                            attrs={'class': 'lister-item-index unbold text-primary'}).text[:-1]
                movie_dict["Rank"] = movie_rank
                # Get the movie title and store in the dictionary
                movie_title = movie_tag.find('a').text
                movie_dict["Title"] = movie_title
                # Store the movie url into dictionary
                movie_dict["URL"] = movie_url
                print(movie_rank + ". " + movie_title + "(" + movie_url + ")")

                # Open the page for the movie
                r = requests.get(movie_url, headers=self.headers)

                # Read the URL content and create a BeautifulSoup object. Using html.parser whereas
                # other parsers can be used as well
                movie_soup = BeautifulSoup(r.content,
                                           'html.parser')

                # Get the genres of the movie and store into dictionary
                movie_genres = []
                movie_genre_tags = movie_soup.find_all('span', attrs={'class': 'ipc-chip__text'})
                for movie_genre_tag in movie_genre_tags:
                    movie_genres.append(movie_genre_tag.text)
                movie_dict["Genre"] = movie_genres

                # Get the movie plot and store into dictionary
                movie_plot = movie_soup.find_all('span', attrs={'data-testid': 'plot-xl'})[0].text
                movie_dict["Plot"] = movie_plot
                # The details about the movie cast are stored as metadata.
                # Below part of code will get the metadata nameand the values against them.
                movie_metadata_tag = movie_soup.find('ul', attrs={
                    'class': 'ipc-metadata-list ipc-metadata-list--dividers-all title-pc-list ipc-metadata-list--baseAlt'})
                movie_metadata_list = movie_metadata_tag.find_all('li',
                                                                  attrs={
                                                                      'data-testid': 'title-pc-principal-credit'})
                # Iterate through the metadata items
                for movie_metadata in movie_metadata_list:
                    # Get the metadata name
                    metadata_name_tag = \
                        movie_metadata.select(
                            '[class*="ipc-metadata-list-item__label ipc-metadata-list-item__label"]')[
                            0]
                    metadata_name = metadata_name_tag.text
                    # Get the metadata values
                    metadata_value_tags = movie_metadata.select(
                        '[class*="ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item"]')
                    metadata_values = []
                    for metadata_value in metadata_value_tags:
                        metadata_values.append(metadata_value.text)
                        # Store each of the cast name and role into MySQL db
                        db_handler.insert_role_to_db(metadata_value.text, metadata_name)
                    # Store the metadata and the values into the dictionary
                    movie_dict[metadata_name] = metadata_values

                # Insert the dictionary into the Mogo db collection
                result = db_handler.insert_into_movie_collection(movie_dict)
                print(f"One movie: {result.inserted_id}")
                print("Movies added ->")
                print(movie_dict)

        # Close the connections
        db_handler.close_connections()


# Check whether the tool is executed from command
if __name__ == '__main__':
    # Create instance of the class
    movie_scraper = MovieScraper()
    # Call the method
    movie_scraper.scrape_movies()
