import json
import psycopg2
import os

def lambda_handler(event, context):
    try:
        # Connect to the RDS database
        connection = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        cursor = connection.cursor()

        # Updated SQL query to fetch restaurants with the new address fields
        query = '''
            SELECT r.restaurant_id, r.name, r.street_address, r.city, r.state, r.zip_code, 
                   r.latitude, r.longitude, r.description, r.features, 
                   d.day, d.start_time, d.end_time, d.specials
            FROM restaurants r
            LEFT JOIN deals d ON r.restaurant_id = d.restaurant_id
            WHERE d.day IS NOT NULL
        '''
        cursor.execute(query)
        rows = cursor.fetchall()

        # Process rows into the desired format
        restaurants = {}
        for row in rows:
            rest_id = row[0]
            if rest_id not in restaurants:
                # Add restaurant details
                restaurants[rest_id] = {
                    'restaurant_id': rest_id,
                    'name': row[1],
                    'street_address': row[2],
                    'city': row[3],
                    'state': row[4],
                    'zip_code': row[5],
                    'latitude': row[6],
                    'longitude': row[7],
                    'description': row[8],  # Correct mapping
                    'features': row[9],     # Correct mapping
                    'cobalt_apps': []       # Deals
                }

            # Check if deal data exists
            if row[10] and row[11] and row[12]:  # Check day, start_time, and end_time are not NULL
                deal = {
                    'day': row[10],
                    'start_time': row[11],
                    'end_time': row[12],
                    'specials': row[13] if row[13] else ""  # Handle NULL specials gracefully
                }
                if deal not in restaurants[rest_id]['cobalt_apps']:
                    restaurants[rest_id]['cobalt_apps'].append(deal)

        # Return the result as JSON
        return {
            'statusCode': 200,
            'body': json.dumps(list(restaurants.values()))
        }

    except Exception as e:
        # Return an error response in case of failure
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
