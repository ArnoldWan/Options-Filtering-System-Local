import pyodbc
import requests
from datetime import datetime
import pytz

# Correct database name in the connection string
cnxn_string = 'DRIVER={SQL SERVER};Server=localhost\\MSSQLSERVER02;Database=Options_Screener;Trusted_Connection=True;'

# Function to retrieve an available API key from the database
def get_available_api_key():
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Get today's date in US Eastern Time
    eastern = pytz.timezone('US/Eastern')
    current_date_us = datetime.now(eastern).date()
    current_date_us_str = current_date_us.strftime('%Y-%m-%d')

    query = """
    SELECT TOP 1 k.[API_Key]
    FROM [dbo].[Alpha_Vantage_API_Keys] k
    LEFT JOIN (
        SELECT [API_KEY], [Count]
        FROM [dbo].[Alpha_Vantage_API_Key_Usage_Count]
        WHERE [Used_On_US_Date] = ?
    ) uc ON k.[API_Key] = uc.[API_KEY]
    WHERE ISNULL(uc.[Count], 0) < 25
    ORDER BY ISNULL(uc.[Count], 0) ASC
    """

    cursor.execute(query, (current_date_us_str,))
    result = cursor.fetchone()

    if result:
        api_key = result[0]
    else:
        api_key = None

    cursor.close()
    cnxn.close()

    return api_key

# Function to fetch and store historical options data using Alpha Vantage API
def fetch_and_store_options_data(symbol, date):
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Check for duplicate entry before making the API call
    if check_duplicate_entry(cursor, symbol, date):
        print(f"Data for {symbol} on {date} has already been processed. Skipping API call.")
    else:
        # Fetch available API key
        api_key = get_available_api_key()
        if not api_key:
            print("No available API keys within usage limit.")
            return

        print(f"API Key '{api_key}' retrieved. Fetching data for {symbol} on {date}...")

        # Make API request to Alpha Vantage
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={date}&apikey={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            historical_options = data.get('data', [])

            if historical_options:
                print(f"Data fetched successfully. Inserting data into the database...")

                # Insert data into database
                insert_historical_data(cursor, historical_options)

                # Update API key usage
                update_api_key_usage(cursor, api_key)

                print(f"Data for {symbol} on {date} has been processed successfully.")
            else:
                print("No historical options data found.")
        else:
            print(f"Failed to fetch data for {symbol} on {date}. Status code: {response.status_code}")

    cnxn.commit()
    cursor.close()
    cnxn.close()

# Function to check for duplicate entry in the database
def check_duplicate_entry(cursor, symbol, date):
    query = """
    SELECT COUNT(*)
    FROM Options_Historical_Data_Master
    WHERE Symbol = ? AND Date = ?
    """
    cursor.execute(query, (symbol, date))
    count = cursor.fetchone()[0]
    return count > 0

# Function to insert historical options data into the database
def insert_historical_data(cursor, historical_options):
    insert_query = """
    INSERT INTO Options_Historical_Data_Master (
        ContractID, Symbol, Expiration, Strike, Type, Last, Mark, Bid, Bid_Size, Ask, Ask_Size,
        Volume, Open_Interest, Date, Implied_Volatility, Delta, Gamma, Theta, Vega, Rho, Created_On
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """
    for option in historical_options:
        cursor.execute(insert_query, (
            option.get('contractID'),  # Ensure this matches the key in your API response
            option.get('symbol'),
            option.get('expiration'),
            option.get('strike'),
            option.get('type'),
            option.get('last'),
            option.get('mark'),
            option.get('bid'),
            option.get('bid_size'),
            option.get('ask'),
            option.get('ask_size'),
            option.get('volume'),
            option.get('open_interest'),
            option.get('date'),
            option.get('implied_volatility'),
            option.get('delta'),
            option.get('gamma'),
            option.get('theta'),
            option.get('vega'),
            option.get('rho')
        ))


# Function to update API key usage in the database
def update_api_key_usage(cursor, api_key):
    hk_time = datetime.now()
    eastern = pytz.timezone('US/Eastern')
    us_time = datetime.now(eastern)
    current_date_us_str = us_time.date().strftime('%Y-%m-%d')

    insert_usage_query = """
    INSERT INTO Alpha_Vantage_API_Key_Usage (API_Key_ID, API_Key, Used_At_HK_Time, Used_At_US_Time)
    VALUES (
        (SELECT API_KEY_ID FROM Alpha_Vantage_API_Keys WHERE API_Key = ?),
        ?, ?, ?
    )
    """
    cursor.execute(insert_usage_query, (api_key, api_key, hk_time, us_time))

    select_count_query = """
    SELECT Usage_ID, Count
    FROM Alpha_Vantage_API_Key_Usage_Count
    WHERE API_KEY = ? AND Used_On_US_Date = ?
    """
    cursor.execute(select_count_query, (api_key, current_date_us_str))
    result = cursor.fetchone()

    if result:
        update_count_query = """
        UPDATE Alpha_Vantage_API_Key_Usage_Count
        SET Count = Count + 1, Updated_On = ?
        WHERE Usage_ID = ?
        """
        cursor.execute(update_count_query, (hk_time, result.Usage_ID))
    else:
        insert_count_query = """
        INSERT INTO Alpha_Vantage_API_Key_Usage_Count (API_Key_ID, API_KEY, Used_On_US_Date, Count, Created_On, Updated_On)
        VALUES (
            (SELECT API_KEY_ID FROM Alpha_Vantage_API_Keys WHERE API_Key = ?),
            ?, ?, 1, ?, ?
        )
        """
        cursor.execute(insert_count_query, (api_key, api_key, current_date_us_str, hk_time, hk_time))

# Example usage
fetch_and_store_options_data('DELL', '2024-06-25')
