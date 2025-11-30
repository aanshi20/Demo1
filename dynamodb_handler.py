# dynamodb_handler.py
"""
Handles all interactions with the AWS DynamoDB table.
"""
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
import json

class DynamoDBHandler:
    """A class to interact with a DynamoDB table."""

    def __init__(self, table_name, region_name):
        """
        Initializes the DynamoDB resource and table.

        :param table_name: The name of the DynamoDB table.
        :param region_name: The AWS region of the table.
        """
        try:
            self.resource = boto3.resource('dynamodb', region_name=region_name)
            self.table = self.resource.Table(table_name)
        except Exception as e:
            print(f"Error connecting to DynamoDB: {e}")
            raise

    def get_all_records(self):
        """
        Retrieves all records from the DynamoDB table using a scan operation.

        Note: A scan operation can be inefficient and costly for large tables.
        It reads every item in the entire table. For production use cases with
        large datasets, consider using 'query' with a specific key.

        :return: A list of items from the table, or None if an error occurs.
        """
        try:
            print("Scanning the table for all records...")
            response = self.table.scan()
            
            # The scan result might be paginated, so we loop until all items are retrieved
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
            
            print(f"Found {len(items)} records.")
            return items
        except ClientError as e:
            print(f"An error occurred: {e.response['Error']['Message']}")
            return None

    def update_movie_rating(self, year, title, new_rating):
        """
        Updates the 'rating' attribute for a specific movie item.

        :param year: The partition key (year) of the movie.
        :param title: The sort key (title) of the movie.
        :param new_rating: The new rating value to set.
        :return: The response from the update_item call, or None if an error occurs.
        """
        try:
            print(f"Updating rating for '{title}' ({year}) to {new_rating}...")
            response = self.table.update_item(
                Key={'year': year, 'title': title},
                UpdateExpression="set info.rating = :r",
                ExpressionAttributeValues={':r': Decimal(str(new_rating))},
                ReturnValues="UPDATED_NEW"
            )
            print("Update successful.")
            return response
        except ClientError as e:
            # Handle the case where the item to update doesn't exist
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(f"Error: A movie with title '{title}' ({year}) was not found.")
            else:
                print(f"An error occurred while updating: {e.response['Error']['Message']}")
            return None

    def update_movie_rating_by_title(self, title, new_rating):
        """
        Finds movies by title and updates their 'rating' attribute.
        Note: This uses a scan operation with a filter, which can be inefficient
        for large tables. For better performance, consider adding a Global Secondary Index
        on the 'title' attribute if this operation is frequent.

        :param title: The title of the movie(s) to update.
        :param new_rating: The new rating value to set.
        :return: A list of responses from the update_item calls, or None if an error occurs.
        """
        print(f"Attempting to update rating for movie(s) with title '{title}' to {new_rating}...")
        updated_responses = []
        try:
            # Use scan with FilterExpression to find items by title
            response = self.table.scan(
                FilterExpression=Attr('title').eq(title)
            )
            
            items_to_update = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=Attr('title').eq(title),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items_to_update.extend(response.get('Items', []))

            if not items_to_update:
                print(f"No movie found with title '{title}'. No updates performed.")
                return None

            print(f"Found {len(items_to_update)} movie(s) with title '{title}'. Attempting to update...")
            for item in items_to_update:
                movie_year = item.get('year')
                if movie_year:
                    # Call the existing update_movie_rating method for each found item
                    update_response = self.update_movie_rating(movie_year, title, new_rating)
                    if update_response:
                        updated_responses.append(update_response)
                else:
                    print(f"Warning: Movie with title '{title}' found but 'year' attribute is missing. Skipping update for this item.")
            
            if updated_responses:
                print(f"Successfully updated {len(updated_responses)} movie(s) with title '{title}'.")
                return updated_responses
            else:
                print(f"No updates were successfully applied for title '{title}'.")
                return None

        except ClientError as e:
            print(f"An error occurred while searching for movies by title: {e.response['Error']['Message']}")
            return None

    def find_movies_by_title(self, title):
        """
        Finds movies by title using a scan operation.
        Note: This uses a scan operation with a filter, which can be inefficient
        for large tables.

        :param title: The title of the movie(s) to find.
        :return: A list of found items, or an empty list if none are found or an error occurs.
        """
        print(f"Searching for movies with title '{title}'...")
        try:
            response = self.table.scan(
                FilterExpression=Attr('title').eq(title)
            )
            
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=Attr('title').eq(title),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            if not items:
                print(f"No movie found with title '{title}'.")
            else:
                print(f"Found {len(items)} movie(s) with title '{title}'.")
            
            return items

        except ClientError as e:
            print(f"An error occurred while searching for movies by title: {e.response['Error']['Message']}")
            return []

    def delete_movie(self, year, title):
        """
        Deletes a specific movie item from the table.

        :param year: The partition key (year) of the movie.
        :param title: The sort key (title) of the movie.
        :return: The response from the delete_item call, or None if an error occurs.
        """
        try:
            print(f"Deleting movie '{title}' ({year})...")
            response = self.table.delete_item(
                Key={'year': year, 'title': title},
                ReturnValues='ALL_OLD'  # Returns the deleted item's content
            )
            if 'Attributes' in response:
                print("Deletion successful.")
            else:
                print(f"Movie '{title}' ({year}) not found. Nothing to delete.")
            return response
        except ClientError as e:
            print(f"An error occurred while deleting: {e.response['Error']['Message']}")
            return None

# Helper class to convert DynamoDB's Decimal types to Python's float for JSON serialization.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
