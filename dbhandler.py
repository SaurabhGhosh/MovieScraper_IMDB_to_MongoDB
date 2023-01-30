# Import MySQL related libraries 
import mysql
from mysql.connector import Error
# Import Mongo db client library
from pymongo import MongoClient


class DBHandler:
    """This class contains the attributes and methods to handle database interactions from the scraping program as
    well as the UI tool. """
    def __init__(self):
        """This is the constructor method. It initiates the connection instances with the database."""
        # SQL DB initiation
        try:
            # Create the connection instance as a class level variable by calling "connect" method
            # and supplying the connection details and credentials.
            self.sql_connection = mysql.connector.connect(host='localhost',
                                                          port='3306',
                                                          database='pythonapps',
                                                          user='pythonuser',
                                                          password='Welcome1')
            if self.sql_connection.is_connected():
                db_Info = self.sql_connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)

        # Handle exception if connectivity fails
        except Error as e:
            print("Error while connecting to MySQL", e)

        # NoSQL DB initiation
        self.mongo_client = MongoClient(host="localhost", port=27017)
        self.mongo_db = self.mongo_client["local"]
        # Create a class variable for the collection. This will get overwritten with each gnre specific collection 
        self.movie_collection = None

    def create_movie_roles_table(self):
        """This method creates the table in MySQL db to store the role and name pairs from the scraped movies. """
        # Get a cursor for the connection
        cursor = self.sql_connection.cursor()
        # Create the query as a string. Keep both values as primary key to reject duplicate entry by error
        create_query = "CREATE TABLE IF NOT EXISTS movieroles (\
                          name VARCHAR(100) NOT NULL,\
                          role VARCHAR(100) NOT NULL,\
                          PRIMARY KEY (name,role));"
        # Enclose database call within try and except block
        try:
            # Execute the create query
            cursor.execute(create_query)
            # Return true to indicate that creation was successful
            return True
        except Error as e:
            print("Error while creating table", e)
            # Close the cursor if error.
            cursor.close()
            # Return false on failure
            return False

    def insert_role_to_db(self, name, role):
        """ This method inserts one entry into the MySQL db."""
        # Some movies have the role mentioned as plural. Maintain uniform role keyword.
        # So, whenever th role has the director, writer or star word, change the role to insert as plural.
        if "Director" in role:
            role = "Directors"
        if "Writer" in role:
            role = "Writers"
        if "Star" in role:
            role = "Stars"
        # Create the sql query for insertion as a string with placeholders 
        insert_query = "INSERT INTO movieroles " \
                       "(name, role) " \
                       "VALUES (%s, %s)"

        # Call the create method always prior 
        if self.create_movie_roles_table():
            # Get cursor instance.
            cursor = self.sql_connection.cursor()
            # Enclose the database call within try and except
            try:
                # Execute the insert query
                # Pass the values as a parameter to library method avoid sql injection issues
                cursor.execute(insert_query, (name, role))
                # Remember to commit if successful
                self.sql_connection.commit()
            except Error as e:
                # Check for duplicate record.
                # Checking through database failure is cheaper as we can check with single database call.
                # The error number 1062 is specific to duplicate record failure if primary key (book_ISBN)
                # is already present.
                if e.errno == 1062:
                    print("Error while inserting record - duplicate record", e)
                    # Close the cursor
                    cursor.close()
                else:
                    # For other errors, close the cursor.
                    # If further specific failures need to be captured, the best way would be to check
                    # the error number and handle like duplicate record block.
                    print("Error while inserting record", e)
                    cursor.close()
        else:
            # Don't attempt anything if creation of table failed.
            pass

    def create_movie_genre_collection(self, genre_collection_name):
        """This method creates the Mongo db collection with the genre-specific collection name"""
        self.movie_collection = self.mongo_db[genre_collection_name]
        # Delete all existing records in the collection if the collection already exists, enforcing fresh collection
        # when the scraper program runs
        self.movie_collection.delete_many({})

    def insert_into_movie_collection(self, movie_dict):
        """This method inserts record into the Mongo db collection"""
        # Insert the dictionary in parameter into the current genre-specific collection
        result = self.movie_collection.insert_one(movie_dict)
        # Return the result in case calling program needs to handle the success/failure specifically.
        return result

    def close_connections(self):
        """This method closes both MySQL and Mongo db collections"""
        self.sql_connection.close()
        self.mongo_client.close()