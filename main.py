# main.py
"""
Main script to fetch and display records from a DynamoDB table.
"""
import json
from config import DYNAMODB_TABLE_NAME, AWS_REGION
from dynamodb_handler import DynamoDBHandler, DecimalEncoder

def display_records(records):
    """
    Displays records in a readable format.

    :param records: A list of records (items) from DynamoDB.
    """
    if not records:
        print("No records to display.")
        return


    print("\n--- Displaying DynamoDB Records ---")
    for i, record in enumerate(records, 1):
        print(f"\n--- Record {i} ---")
        # Use the custom DecimalEncoder to handle DynamoDB's Decimal type
        print(json.dumps(record, indent=4, cls=DecimalEncoder))
    print("\n-----------------------------------\n")


def main():
    """
    The main function to run the application.
    """
    print("Starting application...")
    try:
        # Initialize the handler
        db_handler = DynamoDBHandler(
            table_name=DYNAMODB_TABLE_NAME,
            region_name=AWS_REGION
        )

        while True:
            print("\n--- Main Menu ---")
            print("1. Fetch all movies")
            print("2. Update a movie rating by title")
            print("3. Find a movie by title")
            print("4. Delete a movie by title")
            print("5. Exit")
            choice = input("Enter your choice (1-5): ")

            if choice == '1':
                # Fetch and display all records
                print("\n--- Fetching all records ---")
                all_records = db_handler.get_all_records()
                if all_records is not None:
                    display_records(all_records)

            elif choice == '2':
                # Prompt user for movie title and new rating to update
                print("\n--- Interactive Movie Rating Update ---")
                movie_title_to_update = input("Enter the title of the movie to update: ")
                
                new_rating_str = ""
                while not new_rating_str:
                    new_rating_str = input("Enter the new rating (e.g., 8.5): ")
                    try:
                        new_rating = float(new_rating_str)
                    except ValueError:
                        print("Invalid rating. Please enter a number.")
                        new_rating_str = "" # Reset to loop again

                # Call the method to update the movie(s) by title
                db_handler.update_movie_rating_by_title(title=movie_title_to_update, new_rating=new_rating)
            
            elif choice == '3':
                # Prompt user for movie title to find
                print("\n--- Find Movie by Title ---")
                movie_title_to_find = input("Enter the title of the movie to find: ")
                found_movies = db_handler.find_movies_by_title(movie_title_to_find)
                if found_movies:
                    display_records(found_movies)

            elif choice == '4':
                # Prompt user for movie title to delete
                print("\n--- Interactive Movie Deletion ---")
                movie_title_to_delete = input("Enter the title of the movie to delete: ")
                found_movies = db_handler.find_movies_by_title(movie_title_to_delete)

                if not found_movies:
                    continue # No movies found, go back to menu
                
                year_to_delete = None
                if len(found_movies) == 1:
                    movie = found_movies[0]
                    print("Found one movie:")
                    display_records([movie])
                    confirm = input(f"Are you sure you want to delete '{movie['title']}' ({movie['year']})? (y/n): ").lower()
                    if confirm == 'y':
                        year_to_delete = movie['year']
                else: # Multiple movies found
                    print("Found multiple movies with that title. Please specify which one to delete.")
                    display_records(found_movies)
                    try:
                        year_to_delete_str = input("Enter the YEAR of the movie to delete: ")
                        year_to_delete = int(year_to_delete_str)
                    except ValueError:
                        print("Invalid year. Please enter a number.")
                
                if year_to_delete is not None:
                    # Check if the chosen year is valid among the found movies
                    if any(movie['year'] == year_to_delete for movie in found_movies):
                        db_handler.delete_movie(year=year_to_delete, title=movie_title_to_delete)
                    else:
                        print(f"No movie with title '{movie_title_to_delete}' and year '{year_to_delete}' was found.")

            elif choice == '5':
                break # Exit the loop
            else:
                print("Invalid choice. Please enter a number between 1 and 5.")


    except Exception as e:
        print(f"An error occurred in the main application: {e}")
    
    print("Application finished.")


if __name__ == "__main__":
    main()
