import json
import psycopg2
import os
from datetime import datetime

def convert_time_string_to_hours(time_str):
    """Convert a time string like '5:00 PM' to fractional hours."""
    try:
        dt = datetime.strptime(time_str, '%I:%M %p')
        return dt.hour + dt.minute / 60.0
    except Exception as e:
        return 0.0

def lambda_handler(event, context):
    try:
        # --- 1. Read filter parameters from the incoming event ---
        # (Assuming API Gateway passes these as queryStringParameters)
        params = event.get("queryStringParameters") or {}
        searchQuery = params.get("searchQuery", "").lower()
        locationQuery = params.get("locationQuery", "").lower()
        # Expecting selectedDays as a comma-separated string; e.g., "Mon,Tue"
        selectedDays_str = params.get("selectedDays", "")
        selectedDays = [day.strip() for day in selectedDays_str.split(",")] if selectedDays_str else []
        # Default times (adjust as needed)
        userStartTime = params.get("startTime", "12:00 AM")
        userEndTime = params.get("endTime", "11:30 PM")
        userStart = convert_time_string_to_hours(userStartTime)
        userEnd = convert_time_string_to_hours(userEndTime)

        # --- 2. Connect to the database and fetch restaurant/deal rows ---
        connection = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        cursor = connection.cursor()

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

        # --- 3. Group rows by restaurant and build a combined structure ---
        restaurants = {}
        for row in rows:
            rest_id = row[0]
            # Compute full address for location filtering (all lowercased for case-insensitive match)
            full_address = f"{row[2]}, {row[3]}, {row[4]} {row[5]}".lower()

            # Create the restaurant entry if it does not exist already
            if rest_id not in restaurants:
                restaurants[rest_id] = {
                    'restaurant_id': rest_id,
                    'name': row[1],
                    'street_address': row[2],
                    'city': row[3],
                    'state': row[4],
                    'zip_code': row[5],
                    'latitude': row[6],
                    'longitude': row[7],
                    'description': row[8],
                    'features': row[9],
                    'full_address': full_address,
                    'cobalt_apps': []  # to hold deal info
                }
            # Append deal information if available
            if row[10] is not None and row[11] is not None and row[12] is not None:
                deal = {
                    'day': row[10],
                    'start_time': row[11],
                    'end_time': row[12],
                    'specials': row[13] if row[13] is not None else ""
                }
                restaurants[rest_id]['cobalt_apps'].append(deal)

        # --- 4. Apply filtering on restaurants and their deals ---
        filtered_restaurants = []
        for rest in restaurants.values():
            # Filter by search query (restaurant name)
            if searchQuery and searchQuery not in rest['name'].lower():
                continue

            # Filter by location query (full address)
            if locationQuery and locationQuery not in rest['full_address']:
                continue

            # Filter deals based on selected days and time range
            valid_deals = []
            for deal in rest['cobalt_apps']:
                # If days are selected, skip deals that do not match
                if selectedDays and deal['day'] not in selectedDays:
                    continue

                # Convert deal times to fractional hours
                dealStart = convert_time_string_to_hours(deal['start_time'])
                dealEnd = convert_time_string_to_hours(deal['end_time'])

                # Check for time overlap (the user’s time range overlaps the deal’s time range)
                if not (userStart < dealEnd and userEnd > dealStart):
                    continue

                valid_deals.append(deal)

            # Only include the restaurant if it has at least one valid deal
            if valid_deals:
                rest['cobalt_apps'] = valid_deals  # update with filtered deals
                filtered_restaurants.append(rest)

        # --- 5. Return filtered results ---
        return {
            'statusCode': 200,
            'body': json.dumps(filtered_restaurants)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
