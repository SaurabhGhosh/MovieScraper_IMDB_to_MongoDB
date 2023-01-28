from .moviescraper import MovieScraper

if __name__ == '__main__':
    # Create instance of the class
    movie_scraper = MovieScraper()
    # Call the method
    movie_scraper.scrape_movies()